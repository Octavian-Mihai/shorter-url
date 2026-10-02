package metrics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Octavian-Mihai/shorter-url/internal/ratelimit"
)

func TestHTTPInstrumentRecordsRouteMethodCodeAndLatency(t *testing.T) {
	reg := prometheus.NewRegistry()
	wrap := HTTPInstrument(reg)
	h := wrap("redirect", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusFound) })
	h(httptest.NewRecorder(), httptest.NewRequest("GET", "/abc", nil))
	h(httptest.NewRecorder(), httptest.NewRequest("GET", "/def", nil))

	// Raw paths must not become labels (cardinality).
	got, err := testutil.GatherAndCount(reg, "shortener_http_requests_total")
	if err != nil || got != 1 {
		t.Fatalf("series = %d, %v; want 1 (route label, not path)", got, err)
	}
	if n := testutil.CollectAndCount(reg, "shortener_http_request_duration_seconds"); n == 0 {
		t.Error("no latency histogram")
	}
	exp := `
# HELP shortener_http_requests_total HTTP requests by route, method and status code.
# TYPE shortener_http_requests_total counter
shortener_http_requests_total{code="302",method="GET",route="redirect"} 2
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(exp), "shortener_http_requests_total"); err != nil {
		t.Error(err)
	}
}

func TestHTTPInstrumentDefaultsTo200(t *testing.T) {
	reg := prometheus.NewRegistry()
	h := HTTPInstrument(reg)("x", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("hi")) })
	h(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	exp := `
# HELP shortener_http_requests_total HTTP requests by route, method and status code.
# TYPE shortener_http_requests_total counter
shortener_http_requests_total{code="200",method="GET",route="x"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(exp), "shortener_http_requests_total"); err != nil {
		t.Error(err)
	}
}

func TestLinkObserverCountsOutcomes(t *testing.T) {
	reg := prometheus.NewRegistry()
	o := LinkObserver(reg)
	o.Resolve("hit")
	o.Resolve("hit")
	o.Resolve("miss")
	exp := `
# HELP shortener_resolve_total Slug resolutions by cache outcome.
# TYPE shortener_resolve_total counter
shortener_resolve_total{result="cache_error"} 0
shortener_resolve_total{result="db_error"} 0
shortener_resolve_total{result="hit"} 2
shortener_resolve_total{result="miss"} 1
shortener_resolve_total{result="negative_hit"} 0
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(exp), "shortener_resolve_total"); err != nil {
		t.Error(err)
	}
}

type stubLimiter struct {
	r   ratelimit.Result
	err error
}

func (s stubLimiter) Allow(context.Context, string) (ratelimit.Result, error) { return s.r, s.err }

func TestLimiterDecisions(t *testing.T) {
	reg := prometheus.NewRegistry()
	ok := WrapLimiter(reg, stubLimiter{r: ratelimit.Result{Allowed: true}})
	_, _ = ok.Allow(context.Background(), "k")
	_, _ = ok.Allow(context.Background(), "k")
	reg2 := prometheus.NewRegistry()
	bad := WrapLimiter(reg2, stubLimiter{err: errors.New("redis down")})
	_, _ = bad.Allow(context.Background(), "k")

	exp := `
# HELP shortener_ratelimit_decisions_total Rate limiter decisions.
# TYPE shortener_ratelimit_decisions_total counter
shortener_ratelimit_decisions_total{decision="allowed"} 2
shortener_ratelimit_decisions_total{decision="error"} 0
shortener_ratelimit_decisions_total{decision="limited"} 0
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(exp), "shortener_ratelimit_decisions_total"); err != nil {
		t.Error(err)
	}
	if v := testutil.ToFloat64(counterFor(t, reg2, "error")); v != 1 {
		t.Errorf("error decisions = %v", v)
	}
}

func counterFor(t *testing.T, reg *prometheus.Registry, decision string) prometheus.Collector {
	t.Helper()
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			if m.GetLabel()[0].GetValue() == decision {
				c := prometheus.NewCounter(prometheus.CounterOpts{Name: "tmp"})
				c.Add(m.GetCounter().GetValue())
				return c
			}
		}
	}
	t.Fatalf("decision %s not found", decision)
	return nil
}

func TestConsumerObserver(t *testing.T) {
	reg := prometheus.NewRegistry()
	o := ConsumerObserver(reg)
	o.BatchStored(10, 7, 20*time.Millisecond)
	o.InsertFailed()
	o.Skipped(2)
	exp := `
# HELP shortener_consumer_events_duplicate_total Re-delivered events skipped by idempotency.
# TYPE shortener_consumer_events_duplicate_total counter
shortener_consumer_events_duplicate_total 3
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(exp), "shortener_consumer_events_duplicate_total"); err != nil {
		t.Error(err)
	}
}

func TestHandlerServesRegistry(t *testing.T) {
	reg := New()
	rec := httptest.NewRecorder()
	Handler(reg).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "go_goroutines") {
		t.Errorf("code %d body %.80s", rec.Code, rec.Body)
	}
}

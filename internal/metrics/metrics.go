// Package metrics wires Prometheus instrumentation into the services without
// making the domain packages depend on Prometheus: they expose small observer
// interfaces / counters, and this package adapts them.
package metrics

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Octavian-Mihai/shorter-url/internal/consumer"
	"github.com/Octavian-Mihai/shorter-url/internal/events"
	"github.com/Octavian-Mihai/shorter-url/internal/idgen"
	"github.com/Octavian-Mihai/shorter-url/internal/link"
	"github.com/Octavian-Mihai/shorter-url/internal/ratelimit"
)

const ns = "shortener"

// New returns a registry with Go runtime and process collectors installed.
func New() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// Handler serves /metrics for reg.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

// ---- HTTP ----

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) { w.code = c; w.ResponseWriter.WriteHeader(c) }

// HTTPInstrument returns a function suitable for httpapi.Deps.Instrument.
// Buckets are dense in the low milliseconds because the redirect path should
// be sub-5ms on a cache hit.
func HTTPInstrument(reg prometheus.Registerer) func(string, http.HandlerFunc) http.HandlerFunc {
	reqs := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "http_requests_total", Help: "HTTP requests by route, method and status code.",
	}, []string{"route", "method", "code"})
	dur := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "http_request_duration_seconds", Help: "HTTP request latency by route.",
		Buckets: []float64{.0005, .001, .002, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	}, []string{"route"})
	reg.MustRegister(reqs, dur)
	return func(route string, next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
			next(sw, r)
			dur.WithLabelValues(route).Observe(time.Since(start).Seconds())
			reqs.WithLabelValues(route, r.Method, strconv.Itoa(sw.code)).Inc()
		}
	}
}

// ---- redirect cache outcomes ----

type linkObserver struct{ c *prometheus.CounterVec }

// LinkObserver counts Resolve outcomes: hit / negative_hit / miss / cache_error / db_error.
func LinkObserver(reg prometheus.Registerer) link.Observer {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "resolve_total", Help: "Slug resolutions by cache outcome.",
	}, []string{"result"})
	reg.MustRegister(c)
	for _, r := range []string{"hit", "negative_hit", "miss", "cache_error", "db_error"} {
		c.WithLabelValues(r)
	}
	return &linkObserver{c: c}
}

func (o *linkObserver) Resolve(result string) { o.c.WithLabelValues(result).Inc() }

// ---- click pipeline (API side) ----

// RegisterAsync exposes the click queue's counters. Values are read lazily
// from the atomics on scrape, so there is no cost on the redirect path.
func RegisterAsync(reg prometheus.Registerer, a *events.Async) {
	reg.MustRegister(
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: ns, Name: "click_events_sent_total", Help: "Click events published to the bus."}, func() float64 { return float64(a.Sent()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: ns, Name: "click_events_dropped_total", Help: "Click events dropped because the queue was full."}, func() float64 { return float64(a.Dropped()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: ns, Name: "click_events_failed_total", Help: "Click events lost because publishing failed."}, func() float64 { return float64(a.Failed()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: ns, Name: "click_queue_length", Help: "Click events waiting to be published."}, func() float64 { return float64(a.QueueLen()) }),
	)
}

// ---- Postgres pool ----

func RegisterPool(reg prometheus.Registerer, pool *pgxpool.Pool) {
	g := func(name, help string, f func(*pgxpool.Stat) float64) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: ns, Name: name, Help: help}, func() float64 { return f(pool.Stat()) })
	}
	reg.MustRegister(
		g("db_pool_acquired_conns", "Connections currently in use.", func(s *pgxpool.Stat) float64 { return float64(s.AcquiredConns()) }),
		g("db_pool_idle_conns", "Idle connections.", func(s *pgxpool.Stat) float64 { return float64(s.IdleConns()) }),
		g("db_pool_max_conns", "Pool size limit.", func(s *pgxpool.Stat) float64 { return float64(s.MaxConns()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: ns, Name: "db_pool_empty_acquire_total", Help: "Acquires that had to wait for a free connection."},
			func() float64 { return float64(pool.Stat().EmptyAcquireCount()) }),
	)
}

// ---- ID block claims ----

type countingBlocks struct {
	idgen.BlockSource
	claims prometheus.Counter
	errs   prometheus.Counter
}

// WrapBlockSource counts ID block claims (expected: one per BLOCK_SIZE creates).
func WrapBlockSource(reg prometheus.Registerer, src idgen.BlockSource) idgen.BlockSource {
	c := prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "id_block_claims_total", Help: "ID blocks claimed from Postgres."})
	e := prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "id_block_claim_errors_total", Help: "Failed ID block claims."})
	reg.MustRegister(c, e)
	return &countingBlocks{BlockSource: src, claims: c, errs: e}
}

func (b *countingBlocks) ClaimBlock(ctx context.Context, size int64) (int64, int64, error) {
	s, e, err := b.BlockSource.ClaimBlock(ctx, size)
	if err != nil {
		b.errs.Inc()
	} else {
		b.claims.Inc()
	}
	return s, e, err
}

// ---- rate limiter ----

type countingLimiter struct {
	inner interface {
		Allow(context.Context, string) (ratelimit.Result, error)
	}
	c *prometheus.CounterVec
}

// WrapLimiter counts allowed / limited / error (fail-open) decisions.
func WrapLimiter(reg prometheus.Registerer, l interface {
	Allow(context.Context, string) (ratelimit.Result, error)
}) interface {
	Allow(context.Context, string) (ratelimit.Result, error)
} {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "ratelimit_decisions_total", Help: "Rate limiter decisions.",
	}, []string{"decision"})
	reg.MustRegister(c)
	for _, d := range []string{"allowed", "limited", "error"} {
		c.WithLabelValues(d)
	}
	return &countingLimiter{inner: l, c: c}
}

func (l *countingLimiter) Allow(ctx context.Context, key string) (ratelimit.Result, error) {
	r, err := l.inner.Allow(ctx, key)
	switch {
	case err != nil:
		l.c.WithLabelValues("error").Inc()
	case r.Allowed:
		l.c.WithLabelValues("allowed").Inc()
	default:
		l.c.WithLabelValues("limited").Inc()
	}
	return r, err
}

// ---- consumer ----

type consumerObserver struct {
	events, inserted, dupes, failures, skipped prometheus.Counter
	batchSize                                  prometheus.Histogram
	insertDur                                  prometheus.Histogram
}

// ConsumerObserver adapts consumer pipeline events to Prometheus.
func ConsumerObserver(reg prometheus.Registerer) consumer.Observer {
	o := &consumerObserver{
		events:   prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "consumer_events_total", Help: "Valid click events processed."}),
		inserted: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "consumer_events_inserted_total", Help: "Click rows newly inserted."}),
		dupes:    prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "consumer_events_duplicate_total", Help: "Re-delivered events skipped by idempotency."}),
		failures: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "consumer_insert_failures_total", Help: "Failed batch insert attempts (retried)."}),
		skipped:  prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "consumer_events_skipped_total", Help: "Invalid / undecodable events skipped."}),
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Name: "consumer_batch_size", Help: "Events per stored batch.",
			Buckets: prometheus.ExponentialBuckets(1, 4, 7)}),
		insertDur: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Name: "consumer_insert_duration_seconds", Help: "Batch insert latency.",
			Buckets: prometheus.DefBuckets}),
	}
	reg.MustRegister(o.events, o.inserted, o.dupes, o.failures, o.skipped, o.batchSize, o.insertDur)
	return o
}

func (o *consumerObserver) BatchStored(n int, inserted int64, took time.Duration) {
	o.events.Add(float64(n))
	o.inserted.Add(float64(inserted))
	o.dupes.Add(float64(int64(n) - inserted))
	o.batchSize.Observe(float64(n))
	o.insertDur.Observe(took.Seconds())
}
func (o *consumerObserver) InsertFailed() { o.failures.Inc() }
func (o *consumerObserver) Skipped(n int) { o.skipped.Add(float64(n)) }

// RegisterLag exposes how far behind the log end this consumer is.
func RegisterLag(reg prometheus.Registerer, lag func() int64) {
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: ns, Name: "consumer_lag_messages", Help: "Messages behind the end of the assigned partitions.",
	}, func() float64 { return float64(lag()) }))
}

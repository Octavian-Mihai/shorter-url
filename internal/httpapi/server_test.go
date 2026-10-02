package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Octavian-Mihai/shorter-url/internal/analytics"
	"github.com/Octavian-Mihai/shorter-url/internal/auth"
	"github.com/Octavian-Mihai/shorter-url/internal/events"
	"github.com/Octavian-Mihai/shorter-url/internal/link"
	"github.com/Octavian-Mihai/shorter-url/internal/ratelimit"
)

// ---- fakes ----

type fakeLinks struct {
	mu        sync.Mutex
	links     map[string]*link.Link
	resolves  int
	createErr error
}

func (f *fakeLinks) Create(_ context.Context, in link.CreateInput) (*link.Link, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	slug := in.Alias
	if slug == "" {
		slug = "gen1234"
	}
	l := &link.Link{Slug: slug, URL: in.URL, APIKeyID: in.APIKeyID, ExpiresAt: in.ExpiresAt, CreatedAt: time.Unix(1700000000, 0).UTC()}
	f.links[slug] = l
	return l, nil
}

func (f *fakeLinks) Resolve(_ context.Context, slug string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resolves++
	l, ok := f.links[slug]
	if !ok {
		return "", link.ErrNotFound
	}
	if l.Expired(time.Now()) {
		return "", link.ErrExpired
	}
	return l.URL, nil
}

func (f *fakeLinks) Get(_ context.Context, slug string) (*link.Link, error) {
	if l, ok := f.links[slug]; ok {
		return l, nil
	}
	return nil, link.ErrNotFound
}

type fakeClicks struct {
	mu  sync.Mutex
	got []events.Click
}

func (f *fakeClicks) Emit(c events.Click) { f.mu.Lock(); f.got = append(f.got, c); f.mu.Unlock() }

type fakeAuth map[string]int64

func (a fakeAuth) Authenticate(_ context.Context, k string) (int64, error) {
	if id, ok := a[k]; ok {
		return id, nil
	}
	return 0, auth.ErrInvalidKey
}

type fakeLimiter struct {
	res  ratelimit.Result
	err  error
	keys []string
}

func (l *fakeLimiter) Allow(_ context.Context, k string) (ratelimit.Result, error) {
	l.keys = append(l.keys, k)
	return l.res, l.err
}

type fakeStats struct{}

func (fakeStats) Stats(_ context.Context, slug string) (*analytics.Stats, error) {
	return &analytics.Stats{Slug: slug, TotalClicks: 42, ByDay: []analytics.DayCount{}, TopReferers: []analytics.RefererCount{}}, nil
}

type harness struct {
	srv    *Server
	links  *fakeLinks
	clicks *fakeClicks
	lim    *fakeLimiter
}

func newHarness() *harness {
	h := &harness{
		links:  &fakeLinks{links: map[string]*link.Link{}},
		clicks: &fakeClicks{},
		lim:    &fakeLimiter{res: ratelimit.Result{Allowed: true}},
	}
	h.srv = New(Deps{
		Links: h.links, Clicks: h.clicks, Auth: fakeAuth{"key-a": 1, "key-b": 2},
		Limiter: h.lim, Stats: fakeStats{}, BaseURL: "http://short.test/",
		Readiness: map[string]func(context.Context) error{"ok": func(context.Context) error { return nil }},
	})
	return h
}

func (h *harness) do(method, path, body string, hdr ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	h.srv.Handler().ServeHTTP(rec, req)
	return rec
}

// ---- redirect path ----

func TestRedirect302WithNoStoreAndClickEvent(t *testing.T) {
	h := newHarness()
	h.links.links["abc"] = &link.Link{Slug: "abc", URL: "https://example.com/dest?x=1"}

	rec := h.do("GET", "/abc", "", "Referer", "https://ref.example/p", "User-Agent", "UA/1")
	if rec.Code != http.StatusFound {
		t.Fatalf("status = %d", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "https://example.com/dest?x=1" {
		t.Errorf("Location = %q", loc)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q", cc)
	}
	if len(h.clicks.got) != 1 {
		t.Fatalf("clicks = %d", len(h.clicks.got))
	}
	c := h.clicks.got[0]
	if c.Slug != "abc" || c.EventID == "" || c.Referer != "https://ref.example/p" || c.UserAgent != "UA/1" || c.IP != "192.0.2.1" {
		t.Errorf("click = %+v", c)
	}
}

func TestRedirectHeadIsNotAClick(t *testing.T) {
	h := newHarness()
	h.links.links["abc"] = &link.Link{Slug: "abc", URL: "https://example.com"}
	if rec := h.do("HEAD", "/abc", ""); rec.Code != http.StatusFound {
		t.Fatalf("status = %d", rec.Code)
	}
	if len(h.clicks.got) != 0 {
		t.Error("HEAD must not record a click")
	}
}

func TestRedirectNotFoundAndExpiredEmitNoClick(t *testing.T) {
	h := newHarness()
	past := time.Now().Add(-time.Hour)
	h.links.links["old"] = &link.Link{Slug: "old", URL: "https://example.com", ExpiresAt: &past}
	if rec := h.do("GET", "/missing", ""); rec.Code != http.StatusNotFound {
		t.Errorf("missing = %d", rec.Code)
	}
	if rec := h.do("GET", "/old", ""); rec.Code != http.StatusGone {
		t.Errorf("expired = %d", rec.Code)
	}
	if len(h.clicks.got) != 0 {
		t.Error("failed resolutions must not record clicks")
	}
}

func TestRedirectOverlongSlugRejectedBeforeResolve(t *testing.T) {
	h := newHarness()
	if rec := h.do("GET", "/"+strings.Repeat("a", 65), ""); rec.Code != http.StatusNotFound {
		t.Errorf("status = %d", rec.Code)
	}
	if h.links.resolves != 0 {
		t.Error("resolver must not be called for impossible slugs")
	}
}

func TestRedirectResolveErrorIs500(t *testing.T) {
	h := newHarness()
	h.srv.Links = errLinks{h.links}
	if rec := h.do("GET", "/abc", ""); rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d", rec.Code)
	}
}

type errLinks struct{ *fakeLinks }

func (errLinks) Resolve(context.Context, string) (string, error) { return "", errors.New("boom") }

func TestClientIPProxyTrust(t *testing.T) {
	h := newHarness()
	h.links.links["abc"] = &link.Link{Slug: "abc", URL: "https://example.com"}
	h.do("GET", "/abc", "", "X-Forwarded-For", "203.0.113.7, 10.0.0.1")
	if ip := h.clicks.got[0].IP; ip != "192.0.2.1" {
		t.Errorf("untrusted proxy: ip = %q, XFF must be ignored", ip)
	}
	h.srv.TrustProxy = true
	h.do("GET", "/abc", "", "X-Forwarded-For", "203.0.113.7, 10.0.0.1")
	if ip := h.clicks.got[1].IP; ip != "203.0.113.7" {
		t.Errorf("trusted proxy: ip = %q", ip)
	}
}

// ---- create ----

func TestCreateRequiresValidKey(t *testing.T) {
	h := newHarness()
	body := `{"url":"https://example.com"}`
	if rec := h.do("POST", "/v1/links", body); rec.Code != 401 {
		t.Errorf("no key = %d", rec.Code)
	}
	if rec := h.do("POST", "/v1/links", body, "X-API-Key", "nope"); rec.Code != 401 {
		t.Errorf("bad key = %d", rec.Code)
	}
	if len(h.lim.keys) != 0 {
		t.Error("limiter must not run for unauthenticated requests")
	}
}

func TestCreateSuccessShapeAndBearer(t *testing.T) {
	h := newHarness()
	rec := h.do("POST", "/v1/links", `{"url":"https://example.com","alias":"my-link"}`, "Authorization", "Bearer key-a")
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d body %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"short_url":"http://short.test/my-link"`) {
		t.Errorf("body = %s", body)
	}
	if rec.Header().Get("Location") != "http://short.test/my-link" {
		t.Errorf("Location = %q", rec.Header().Get("Location"))
	}
	if h.links.links["my-link"].APIKeyID != 1 {
		t.Error("link must be owned by the authenticated key")
	}
	if len(h.lim.keys) != 1 || h.lim.keys[0] != "create:1" {
		t.Errorf("limiter keys = %v", h.lim.keys)
	}
}

func TestCreateErrorMapping(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{link.ErrInvalidURL, 400}, {link.ErrInvalidAlias, 400}, {link.ErrInvalidExpiry, 400},
		{link.ErrSlugTaken, 409}, {errors.New("db"), 500},
	}
	for _, tc := range cases {
		h := newHarness()
		h.links.createErr = tc.err
		if rec := h.do("POST", "/v1/links", `{"url":"https://example.com"}`, "X-API-Key", "key-a"); rec.Code != tc.want {
			t.Errorf("%v -> %d, want %d", tc.err, rec.Code, tc.want)
		}
	}
}

func TestCreateBadBodies(t *testing.T) {
	h := newHarness()
	for name, body := range map[string]string{
		"not json": `nope`, "unknown field": `{"url":"https://a.io","evil":1}`,
		"bad expiry": `{"url":"https://a.io","expires_at":"yesterday"}`,
	} {
		if rec := h.do("POST", "/v1/links", body, "X-API-Key", "key-a"); rec.Code != 400 {
			t.Errorf("%s = %d", name, rec.Code)
		}
	}
	big := `{"url":"https://a.io/` + strings.Repeat("a", 9000) + `"}`
	if rec := h.do("POST", "/v1/links", big, "X-API-Key", "key-a"); rec.Code != 413 {
		t.Errorf("oversize = %d", rec.Code)
	}
}

func TestCreateRateLimited(t *testing.T) {
	h := newHarness()
	h.lim.res = ratelimit.Result{Allowed: false, RetryAfter: 2500 * time.Millisecond}
	rec := h.do("POST", "/v1/links", `{"url":"https://example.com"}`, "X-API-Key", "key-a")
	if rec.Code != 429 || rec.Header().Get("Retry-After") != "3" {
		t.Errorf("status %d Retry-After %q", rec.Code, rec.Header().Get("Retry-After"))
	}
	if len(h.links.links) != 0 {
		t.Error("rate-limited request must not create a link")
	}
}

func TestCreateLimiterFailsOpen(t *testing.T) {
	h := newHarness()
	h.lim.err = errors.New("redis down")
	if rec := h.do("POST", "/v1/links", `{"url":"https://example.com"}`, "X-API-Key", "key-a"); rec.Code != 201 {
		t.Errorf("status = %d", rec.Code)
	}
}

// ---- stats ----

func TestStatsOwnershipAndAuth(t *testing.T) {
	h := newHarness()
	h.links.links["abc"] = &link.Link{Slug: "abc", URL: "https://example.com", APIKeyID: 1}
	if rec := h.do("GET", "/v1/links/abc/stats", ""); rec.Code != 401 {
		t.Errorf("no key = %d", rec.Code)
	}
	if rec := h.do("GET", "/v1/links/abc/stats", "", "X-API-Key", "key-b"); rec.Code != 404 {
		t.Errorf("other owner = %d, want 404", rec.Code)
	}
	if rec := h.do("GET", "/v1/links/nope/stats", "", "X-API-Key", "key-a"); rec.Code != 404 {
		t.Errorf("unknown = %d", rec.Code)
	}
	rec := h.do("GET", "/v1/links/abc/stats", "", "X-API-Key", "key-a")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"total_clicks":42`) {
		t.Errorf("owner = %d %s", rec.Code, rec.Body)
	}
}

// ---- ops / docs ----

func TestOpsEndpointsAndRoutingPrecedence(t *testing.T) {
	h := newHarness()
	h.links.links["healthz2"] = &link.Link{Slug: "healthz2", URL: "https://example.com"}
	if rec := h.do("GET", "/healthz", ""); rec.Code != 200 {
		t.Errorf("healthz = %d", rec.Code)
	}
	if rec := h.do("GET", "/readyz", ""); rec.Code != 200 {
		t.Errorf("readyz = %d", rec.Code)
	}
	h.srv.Readiness["db"] = func(context.Context) error { return errors.New("down") }
	if rec := h.do("GET", "/readyz", ""); rec.Code != 503 {
		t.Errorf("readyz with failing dep = %d", rec.Code)
	}
	rec := h.do("GET", "/openapi.yaml", "")
	b, _ := io.ReadAll(rec.Body)
	if rec.Code != 200 || !strings.Contains(string(b), "openapi: 3.0.3") {
		t.Errorf("openapi = %d", rec.Code)
	}
	if rec := h.do("GET", "/docs", ""); rec.Code != 200 || !strings.Contains(rec.Body.String(), "swagger-ui") {
		t.Errorf("docs = %d", rec.Code)
	}
	if rec := h.do("GET", "/", ""); rec.Code != http.StatusFound || rec.Header().Get("Location") != "/docs" {
		t.Errorf("root = %d %q", rec.Code, rec.Header().Get("Location"))
	}
	if h.links.resolves != 0 {
		t.Error("literal routes must not fall through to the slug resolver")
	}
}

func TestPanicRecovered(t *testing.T) {
	h := newHarness()
	h.srv.Links = panicLinks{h.links}
	if rec := h.do("GET", "/abc", ""); rec.Code != 500 {
		t.Errorf("status = %d", rec.Code)
	}
}

type panicLinks struct{ *fakeLinks }

func (panicLinks) Resolve(context.Context, string) (string, error) { panic("kaboom") }

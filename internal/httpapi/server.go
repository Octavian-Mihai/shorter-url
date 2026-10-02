// Package httpapi is the thin HTTP layer over the link service.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/Octavian-Mihai/shorter-url/api"
	"github.com/Octavian-Mihai/shorter-url/internal/analytics"
	"github.com/Octavian-Mihai/shorter-url/internal/auth"
	"github.com/Octavian-Mihai/shorter-url/internal/events"
	"github.com/Octavian-Mihai/shorter-url/internal/link"
	"github.com/Octavian-Mihai/shorter-url/internal/ratelimit"
)

// Narrow interfaces so the handlers can be tested with fakes.
type (
	// LinkService is satisfied by *link.Service.
	LinkService interface {
		Create(ctx context.Context, in link.CreateInput) (*link.Link, error)
		Resolve(ctx context.Context, slug string) (string, error)
		Get(ctx context.Context, slug string) (*link.Link, error)
	}
	// ClickEmitter is satisfied by *events.Async. Emit must never block.
	ClickEmitter interface{ Emit(events.Click) }
	// Limiter is satisfied by *ratelimit.Limiter.
	Limiter interface {
		Allow(ctx context.Context, key string) (ratelimit.Result, error)
	}
)

type Deps struct {
	Links     LinkService
	Clicks    ClickEmitter
	Auth      auth.Authenticator
	Limiter   Limiter
	Stats     analytics.Repository
	Readiness map[string]func(context.Context) error // name -> check, used by /readyz
	BaseURL   string
	// TrustProxy makes the client IP come from X-Forwarded-For. Only enable
	// behind a proxy that overwrites that header; otherwise it is spoofable.
	TrustProxy bool
	Logger     *slog.Logger
	Now        func() time.Time
	// Instrument wraps a handler with metrics; route is a low-cardinality name
	// (never the raw path). Nil means no instrumentation.
	Instrument func(route string, next http.HandlerFunc) http.HandlerFunc
}

type Server struct {
	Deps
	mux *http.ServeMux
}

// New wires routes. Go 1.22+ patterns: literal routes win over "/{slug}".
func New(d Deps) *Server {
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Instrument == nil {
		d.Instrument = func(_ string, next http.HandlerFunc) http.HandlerFunc { return next }
	}
	s := &Server{Deps: d, mux: http.NewServeMux()}
	in := d.Instrument

	s.mux.HandleFunc("POST /v1/links", in("create", s.requireKey(s.handleCreate)))
	s.mux.HandleFunc("GET /v1/links/{slug}/stats", in("stats", s.requireKey(s.handleStats)))

	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	s.mux.HandleFunc("GET /readyz", s.handleReady)
	s.mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(api.Spec)
	})
	s.mux.HandleFunc("GET /docs", handleDocs)
	s.mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/docs", http.StatusFound)
	})

	s.mux.HandleFunc("GET /{slug}", in("redirect", s.handleRedirect)) // also serves HEAD
	return s
}

func (s *Server) Handler() http.Handler {
	return recoverer(s.Logger, s.logRequests(s.mux))
}

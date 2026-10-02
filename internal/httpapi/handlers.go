package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Octavian-Mihai/shorter-url/internal/events"
	"github.com/Octavian-Mihai/shorter-url/internal/link"
)

const (
	maxBodyBytes = 8 << 10
	maxSlugLen   = 64 // anything longer cannot exist; reject before touching cache/db
	maxHeaderLen = 512
)

// ---- redirect (hot path) ----

func (s *Server) handleRedirect(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	if len(slug) > maxSlugLen {
		writeError(w, http.StatusNotFound, "not_found", "link not found")
		return
	}
	target, err := s.Links.Resolve(r.Context(), slug)
	switch {
	case errors.Is(err, link.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "link not found")
		return
	case errors.Is(err, link.ErrExpired):
		writeError(w, http.StatusGone, "expired", "link has expired")
		return
	case err != nil:
		s.Logger.Error("resolve", "slug", slug, "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}

	// Fire-and-forget: Emit is non-blocking (bounded queue, drops on overflow),
	// so a slow or dead Kafka can never delay or fail the redirect.
	// HEAD requests (link previews, uptime probes) are not clicks.
	if r.Method == http.MethodGet {
		c := events.NewClick(slug, s.Now())
		c.Referer = truncate(r.Referer(), maxHeaderLen)
		c.UserAgent = truncate(r.UserAgent(), maxHeaderLen)
		c.IP = s.clientIP(r)
		s.Clicks.Emit(c)
	}

	// 302, not 301: browsers and CDNs must not cache the redirect or clicks vanish.
	w.Header().Set("Cache-Control", "no-store")
	http.Redirect(w, r, target, http.StatusFound)
}

func (s *Server) clientIP(r *http.Request) string {
	if s.TrustProxy {
		// Use the RIGHTMOST entry: that is the address our own proxy saw and
		// appended (AWS ALB appends; nginx here overwrites, so there is only one).
		// Earlier entries are client-supplied and can be forged.
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			return strings.TrimSpace(parts[len(parts)-1])
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// ---- create ----

type createRequest struct {
	URL       string     `json:"url"`
	Alias     string     `json:"alias"`
	ExpiresAt *time.Time `json:"expires_at"`
}

type linkResponse struct {
	Slug      string     `json:"slug"`
	ShortURL  string     `json:"short_url"`
	URL       string     `json:"url"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	keyID := keyIDFrom(r.Context())

	// Rate limit per API key. Fail open: if Redis is down we'd rather accept
	// writes than reject all of them (creation is low volume and authenticated).
	if res, err := s.Limiter.Allow(r.Context(), "create:"+strconv.FormatInt(keyID, 10)); err != nil {
		s.Logger.Warn("rate limiter unavailable; failing open", "err", err)
	} else if !res.Allowed {
		secs := int(res.RetryAfter.Seconds()) + 1
		w.Header().Set("Retry-After", strconv.Itoa(secs))
		writeError(w, http.StatusTooManyRequests, "rate_limited", "too many link creations; retry later")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req createRequest
	if err := dec.Decode(&req); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusRequestEntityTooLarge, "too_large", "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON body: "+err.Error())
		return
	}

	l, err := s.Links.Create(r.Context(), link.CreateInput{
		URL: req.URL, Alias: req.Alias, ExpiresAt: req.ExpiresAt, APIKeyID: keyID,
	})
	switch {
	case errors.Is(err, link.ErrInvalidURL), errors.Is(err, link.ErrInvalidAlias), errors.Is(err, link.ErrInvalidExpiry):
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	case errors.Is(err, link.ErrSlugTaken):
		writeError(w, http.StatusConflict, "alias_taken", "alias already in use")
		return
	case err != nil:
		s.Logger.Error("create link", "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}
	w.Header().Set("Location", strings.TrimRight(s.BaseURL, "/")+"/"+l.Slug)
	writeJSON(w, http.StatusCreated, linkResponse{
		Slug: l.Slug, ShortURL: strings.TrimRight(s.BaseURL, "/") + "/" + l.Slug,
		URL: l.URL, ExpiresAt: l.ExpiresAt, CreatedAt: l.CreatedAt,
	})
}

// ---- stats ----

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	l, err := s.Links.Get(r.Context(), slug)
	// Someone else's link is reported as 404, not 403, so slugs can't be probed.
	if errors.Is(err, link.ErrNotFound) || (err == nil && l.APIKeyID != keyIDFrom(r.Context())) {
		writeError(w, http.StatusNotFound, "not_found", "link not found")
		return
	}
	if err != nil {
		s.Logger.Error("stats: get link", "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}
	st, err := s.Stats.Stats(r.Context(), slug)
	if err != nil {
		s.Logger.Error("stats", "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}
	writeJSON(w, http.StatusOK, st)
}

// ---- ops ----

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	failed := map[string]string{}
	for name, check := range s.Readiness {
		if err := check(ctx); err != nil {
			failed[name] = err.Error()
		}
	}
	if len(failed) > 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "unavailable", "failed": failed})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func handleDocs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(docsHTML))
}

const docsHTML = `<!doctype html>
<html><head><meta charset="utf-8"><title>URL Shortener API</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"></head>
<body><div id="ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>SwaggerUIBundle({url: "/openapi.yaml", dom_id: "#ui"});</script>
</body></html>`

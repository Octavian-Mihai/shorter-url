package link

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"
)

// SlugGenerator yields fresh unique short codes (idgen.Generator).
type SlugGenerator interface {
	NewSlug(ctx context.Context) (string, error)
}

type Options struct {
	CacheTTL         time.Duration
	NegativeCacheTTL time.Duration
	SelfHost         string // host of the shortener itself; links to it are rejected (redirect loops)
	Now              func() time.Time
	Logger           *slog.Logger
}

type Service struct {
	repo  Repository
	cache Cache
	gen   SlugGenerator
	opt   Options
	sf    singleflight.Group
}

func NewService(repo Repository, cache Cache, gen SlugGenerator, opt Options) *Service {
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	return &Service{repo: repo, cache: cache, gen: gen, opt: opt}
}

type CreateInput struct {
	URL       string
	Alias     string // optional custom slug
	ExpiresAt *time.Time
	APIKeyID  int64
}

const (
	maxURLLen      = 2048
	maxGenAttempts = 5
	minAlias       = 3
	maxAlias       = 32
)

var reservedSlugs = map[string]bool{
	"api": true, "v1": true, "docs": true, "health": true, "healthz": true,
	"readyz": true, "metrics": true, "openapi.yaml": true, "admin": true,
	"favicon.ico": true, "robots.txt": true,
}

// Create validates input, allocates (or reserves) a slug, and persists the link.
func (s *Service) Create(ctx context.Context, in CreateInput) (*Link, error) {
	if err := s.validateURL(in.URL); err != nil {
		return nil, err
	}
	now := s.opt.Now()
	if in.ExpiresAt != nil && !in.ExpiresAt.After(now) {
		return nil, ErrInvalidExpiry
	}
	l := &Link{URL: in.URL, APIKeyID: in.APIKeyID, ExpiresAt: in.ExpiresAt}

	if in.Alias != "" {
		if err := validateAlias(in.Alias); err != nil {
			return nil, err
		}
		l.Slug, l.IsCustom = in.Alias, true
		// ErrSlugTaken is surfaced to the caller (409).
		return l, s.repo.Create(ctx, l)
	}

	// Generated slugs can still collide with a previously chosen custom alias
	// that happens to look like one; the unique constraint catches it, we retry.
	for i := 0; i < maxGenAttempts; i++ {
		slug, err := s.gen.NewSlug(ctx)
		if err != nil {
			return nil, fmt.Errorf("generate slug: %w", err)
		}
		l.Slug = slug
		err = s.repo.Create(ctx, l)
		if errors.Is(err, ErrSlugTaken) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return l, nil
	}
	return nil, fmt.Errorf("could not allocate a free slug after %d attempts", maxGenAttempts)
}

// Resolve returns the destination URL for slug. This is the hot path:
// cache hit -> return; miss -> one coalesced DB read -> fill cache.
// ErrNotFound / ErrExpired are returned for unresolvable slugs.
func (s *Service) Resolve(ctx context.Context, slug string) (string, error) {
	now := s.opt.Now()

	if e, err := s.cache.Get(ctx, slug); err != nil {
		s.opt.Logger.Warn("cache get failed; falling back to db", "err", err)
	} else if e != nil {
		return s.fromEntry(e, now)
	}

	// Coalesce concurrent misses for the same slug into one DB query so a
	// viral link whose cache entry just expired can't stampede Postgres.
	v, err, _ := s.sf.Do(slug, func() (any, error) {
		return s.loadAndCache(context.WithoutCancel(ctx), slug, now)
	})
	if err != nil {
		return "", err
	}
	return s.fromEntry(v.(*CacheEntry), now)
}

func (s *Service) loadAndCache(ctx context.Context, slug string, now time.Time) (*CacheEntry, error) {
	l, err := s.repo.Get(ctx, slug)
	if errors.Is(err, ErrNotFound) {
		e := &CacheEntry{Missing: true}
		s.put(ctx, slug, e, s.opt.NegativeCacheTTL)
		return e, nil
	}
	if err != nil {
		return nil, err
	}
	e := &CacheEntry{URL: l.URL, ExpiresAt: l.ExpiresAt}
	ttl := s.opt.CacheTTL
	if l.ExpiresAt != nil {
		remaining := l.ExpiresAt.Sub(now)
		if remaining <= 0 {
			// Expired: cache the fact (with expiry kept so we report ErrExpired).
			s.put(ctx, slug, e, s.opt.NegativeCacheTTL)
			return e, nil
		}
		// Never cache past the expiry, so no stale redirect outlives the link.
		ttl = min(ttl, remaining)
	}
	s.put(ctx, slug, e, ttl)
	return e, nil
}

func (s *Service) put(ctx context.Context, slug string, e *CacheEntry, ttl time.Duration) {
	if err := s.cache.Set(ctx, slug, *e, ttl); err != nil {
		s.opt.Logger.Warn("cache set failed", "err", err)
	}
}

func (s *Service) fromEntry(e *CacheEntry, now time.Time) (string, error) {
	switch {
	case e.Missing:
		return "", ErrNotFound
	case e.ExpiresAt != nil && !now.Before(*e.ExpiresAt):
		return "", ErrExpired
	default:
		return e.URL, nil
	}
}

func (s *Service) validateURL(raw string) error {
	if raw == "" || len(raw) > maxURLLen {
		return ErrInvalidURL
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return ErrInvalidURL
	}
	if s.opt.SelfHost != "" && strings.EqualFold(u.Hostname(), s.opt.SelfHost) {
		return ErrInvalidURL
	}
	return nil
}

func validateAlias(a string) error {
	if len(a) < minAlias || len(a) > maxAlias || reservedSlugs[strings.ToLower(a)] {
		return ErrInvalidAlias
	}
	for i := 0; i < len(a); i++ {
		c := a[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_'
		if !ok {
			return ErrInvalidAlias
		}
	}
	return nil
}

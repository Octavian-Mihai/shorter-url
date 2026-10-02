package link

import (
	"context"
	"time"
)

// CacheEntry is what the redirect path keeps in the cache. Missing marks a
// negative entry ("this slug does not resolve"), which shields Postgres from
// repeated lookups of unknown or expired slugs.
type CacheEntry struct {
	URL       string     `json:"u,omitempty"`
	ExpiresAt *time.Time `json:"e,omitempty"`
	Missing   bool       `json:"m,omitempty"`
}

// Cache is a best-effort slug -> entry cache. Implementations return
// (nil, nil) on a miss. Errors are never fatal to the caller.
type Cache interface {
	Get(ctx context.Context, slug string) (*CacheEntry, error)
	Set(ctx context.Context, slug string, e CacheEntry, ttl time.Duration) error
}

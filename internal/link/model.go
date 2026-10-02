// Package link holds the core domain: the Link model, errors, and the
// repository abstraction the service layer depends on.
package link

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound  = errors.New("link not found")
	ErrExpired   = errors.New("link expired")
	ErrSlugTaken = errors.New("slug already taken")
)

type Link struct {
	Slug      string
	URL       string
	APIKeyID  int64
	IsCustom  bool
	CreatedAt time.Time
	ExpiresAt *time.Time // nil = never expires
}

// Expired reports whether the link is past its expiry at time now.
func (l *Link) Expired(now time.Time) bool {
	return l.ExpiresAt != nil && !now.Before(*l.ExpiresAt)
}

// Repository is the source of truth for links.
type Repository interface {
	// Create inserts l. It returns ErrSlugTaken if the slug already exists.
	Create(ctx context.Context, l *Link) error
	// Get returns ErrNotFound if the slug does not exist. It does NOT filter
	// on expiry; callers decide, so expired links can be cached negatively.
	Get(ctx context.Context, slug string) (*Link, error)
}

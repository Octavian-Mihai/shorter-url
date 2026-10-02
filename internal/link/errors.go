package link

import "errors"

// Validation errors; the HTTP layer maps all of these to 4xx.
var (
	ErrInvalidURL    = errors.New("url must be an absolute http(s) URL (max 2048 chars)")
	ErrInvalidAlias  = errors.New("alias must be 3-32 chars of [A-Za-z0-9_-] and not reserved")
	ErrInvalidExpiry = errors.New("expires_at must be in the future")
)

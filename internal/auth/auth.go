// Package auth holds the API-key authentication contract shared by the HTTP
// layer and the storage layer.
package auth

import (
	"context"
	"errors"
)

// ErrInvalidKey is returned for unknown or revoked API keys.
var ErrInvalidKey = errors.New("invalid api key")

// Authenticator resolves a raw API key to the key's ID.
type Authenticator interface {
	Authenticate(ctx context.Context, rawKey string) (keyID int64, err error)
}

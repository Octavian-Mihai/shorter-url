package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Octavian-Mihai/shorter-url/internal/auth"
)

// ErrInvalidAPIKey is returned when a key is unknown or revoked.
var ErrInvalidAPIKey = auth.ErrInvalidKey

type APIKeyRepo struct{ pool *pgxpool.Pool }

var _ auth.Authenticator = (*APIKeyRepo)(nil)

func NewAPIKeyRepo(pool *pgxpool.Pool) *APIKeyRepo { return &APIKeyRepo{pool: pool} }

// HashKey is the stored form of a raw key. SHA-256 (not bcrypt) is deliberate:
// API keys are high-entropy random strings, so a fast hash is safe, and
// authentication sits on every create request.
func HashKey(raw string) []byte {
	h := sha256.Sum256([]byte(raw))
	return h[:]
}

// Authenticate returns the key's ID for a raw key, or ErrInvalidAPIKey.
func (r *APIKeyRepo) Authenticate(ctx context.Context, raw string) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx,
		`SELECT id FROM api_keys WHERE key_hash = $1 AND NOT revoked`, HashKey(raw)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrInvalidAPIKey
	}
	if err != nil {
		return 0, fmt.Errorf("postgres: authenticate: %w", err)
	}
	return id, nil
}

// Seed inserts a key if absent (idempotent), used to bootstrap demos.
func (r *APIKeyRepo) Seed(ctx context.Context, name, raw string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO api_keys (name, key_hash) VALUES ($1, $2) ON CONFLICT (key_hash) DO NOTHING`,
		name, HashKey(raw))
	if err != nil {
		return fmt.Errorf("postgres: seed api key: %w", err)
	}
	return nil
}

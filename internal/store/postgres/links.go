package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Octavian-Mihai/shorter-url/internal/link"
)

const uniqueViolation = "23505"

type LinkRepo struct{ pool *pgxpool.Pool }

func NewLinkRepo(pool *pgxpool.Pool) *LinkRepo { return &LinkRepo{pool: pool} }

var _ link.Repository = (*LinkRepo)(nil)

func (r *LinkRepo) Create(ctx context.Context, l *link.Link) error {
	err := r.pool.QueryRow(ctx,
		`INSERT INTO links (slug, url, api_key_id, is_custom, expires_at)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING created_at`,
		l.Slug, l.URL, l.APIKeyID, l.IsCustom, l.ExpiresAt).Scan(&l.CreatedAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return link.ErrSlugTaken
	}
	if err != nil {
		return fmt.Errorf("postgres: create link: %w", err)
	}
	return nil
}

func (r *LinkRepo) Get(ctx context.Context, slug string) (*link.Link, error) {
	l := &link.Link{}
	err := r.pool.QueryRow(ctx,
		`SELECT slug, url, api_key_id, is_custom, created_at, expires_at
		 FROM links WHERE slug = $1`, slug).
		Scan(&l.Slug, &l.URL, &l.APIKeyID, &l.IsCustom, &l.CreatedAt, &l.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, link.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get link: %w", err)
	}
	return l, nil
}

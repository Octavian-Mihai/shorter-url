package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Octavian-Mihai/shorter-url/internal/analytics"
)

type StatsRepo struct{ pool *pgxpool.Pool }

func NewStatsRepo(pool *pgxpool.Pool) *StatsRepo { return &StatsRepo{pool: pool} }

var _ analytics.Repository = (*StatsRepo)(nil)

func (r *StatsRepo) Stats(ctx context.Context, slug string) (*analytics.Stats, error) {
	s := &analytics.Stats{Slug: slug, ByDay: []analytics.DayCount{}, TopReferers: []analytics.RefererCount{}}

	if err := r.pool.QueryRow(ctx, `SELECT count(*) FROM clicks WHERE slug = $1`, slug).Scan(&s.TotalClicks); err != nil {
		return nil, fmt.Errorf("postgres: stats total: %w", err)
	}

	rows, err := r.pool.Query(ctx, `
		SELECT (clicked_at AT TIME ZONE 'UTC')::date AS d, count(*)
		FROM clicks WHERE slug = $1 AND clicked_at >= now() - interval '30 days'
		GROUP BY d ORDER BY d`, slug)
	if err != nil {
		return nil, fmt.Errorf("postgres: stats by day: %w", err)
	}
	for rows.Next() {
		var d time.Time
		var n int64
		if err := rows.Scan(&d, &n); err != nil {
			rows.Close()
			return nil, err
		}
		s.ByDay = append(s.ByDay, analytics.DayCount{Date: d.Format("2006-01-02"), Clicks: n})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = r.pool.Query(ctx, `
		SELECT referer, count(*) AS n FROM clicks
		WHERE slug = $1 AND referer <> ''
		GROUP BY referer ORDER BY n DESC, referer LIMIT 5`, slug)
	if err != nil {
		return nil, fmt.Errorf("postgres: stats referers: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var rc analytics.RefererCount
		if err := rows.Scan(&rc.Referer, &rc.Clicks); err != nil {
			return nil, err
		}
		s.TopReferers = append(s.TopReferers, rc)
	}
	return s, rows.Err()
}

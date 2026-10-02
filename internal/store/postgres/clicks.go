package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Octavian-Mihai/shorter-url/internal/consumer"
	"github.com/Octavian-Mihai/shorter-url/internal/events"
)

type ClickSink struct{ pool *pgxpool.Pool }

func NewClickSink(pool *pgxpool.Pool) *ClickSink { return &ClickSink{pool: pool} }

var _ consumer.Sink = (*ClickSink)(nil)

// InsertClicks writes the whole batch in ONE statement (a single round trip,
// via unnest) and is idempotent: rows whose event_id already exists are
// skipped, so Kafka re-deliveries never double count. Returns rows newly added.
func (s *ClickSink) InsertClicks(ctx context.Context, clicks []events.Click) (int64, error) {
	n := len(clicks)
	ids := make([]string, n)
	slugs := make([]string, n)
	ats := make([]time.Time, n)
	refs := make([]string, n)
	uas := make([]string, n)
	ips := make([]string, n)
	for i, c := range clicks {
		ids[i], slugs[i], ats[i] = c.EventID, clean(c.Slug), c.At
		refs[i], uas[i], ips[i] = clean(c.Referer), clean(c.UserAgent), clean(c.IP)
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO clicks (event_id, slug, clicked_at, referer, user_agent, ip)
		SELECT * FROM unnest($1::uuid[], $2::text[], $3::timestamptz[], $4::text[], $5::text[], $6::text[])
		ON CONFLICT (event_id) DO NOTHING`,
		ids, slugs, ats, refs, uas, ips)
	if err != nil {
		return 0, fmt.Errorf("postgres: insert clicks: %w", err)
	}
	return tag.RowsAffected(), nil
}

// clean strips what Postgres text columns reject (NUL bytes, invalid UTF-8),
// which would otherwise fail the entire batch on every retry.
func clean(s string) string {
	return strings.ToValidUTF8(strings.ReplaceAll(s, "\x00", ""), "")
}

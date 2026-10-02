//go:build integration

package postgres

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Octavian-Mihai/shorter-url/internal/events"
	"github.com/Octavian-Mihai/shorter-url/internal/link"
)

// Run: TEST_DATABASE_URL=postgres://... go test -tags integration ./internal/store/postgres
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	down, _ := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "000001_init.down.sql"))
	up, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "000001_init.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{string(down), string(up)} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	return pool
}

func TestClaimBlockConcurrentDisjoint(t *testing.T) {
	pool := testPool(t)
	bs := NewBlockSource(pool)
	const n, size = 20, 100
	type blk struct{ s, e int64 }
	out := make(chan blk, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, e, err := bs.ClaimBlock(context.Background(), size)
			if err != nil {
				t.Error(err)
				return
			}
			out <- blk{s, e}
		}()
	}
	wg.Wait()
	close(out)
	covered := map[int64]bool{}
	for b := range out {
		if b.e-b.s != size {
			t.Fatalf("block size %d", b.e-b.s)
		}
		for id := b.s; id < b.e; id++ {
			if covered[id] {
				t.Fatalf("id %d claimed twice", id)
			}
			covered[id] = true
		}
	}
	if len(covered) != n*size {
		t.Errorf("covered %d ids, want %d", len(covered), n*size)
	}
}

func TestLinkRepoAndAPIKeys(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	keys := NewAPIKeyRepo(pool)
	if err := keys.Seed(ctx, "demo", "secret-key"); err != nil {
		t.Fatal(err)
	}
	if err := keys.Seed(ctx, "demo", "secret-key"); err != nil { // idempotent
		t.Fatal(err)
	}
	id, err := keys.Authenticate(ctx, "secret-key")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := keys.Authenticate(ctx, "wrong"); !errors.Is(err, ErrInvalidAPIKey) {
		t.Errorf("err = %v", err)
	}

	repo := NewLinkRepo(pool)
	exp := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	l := &link.Link{Slug: "abc", URL: "https://example.com", APIKeyID: id, IsCustom: true, ExpiresAt: &exp}
	if err := repo.Create(ctx, l); err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(ctx, &link.Link{Slug: "abc", URL: "https://x.io", APIKeyID: id}); !errors.Is(err, link.ErrSlugTaken) {
		t.Errorf("duplicate err = %v", err)
	}
	got, err := repo.Get(ctx, "abc")
	if err != nil || got.URL != l.URL || got.ExpiresAt == nil || !got.ExpiresAt.Equal(exp) {
		t.Fatalf("got %+v, %v", got, err)
	}
	if _, err := repo.Get(ctx, "missing"); !errors.Is(err, link.ErrNotFound) {
		t.Errorf("missing err = %v", err)
	}
}

func TestStats(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	rows := []struct {
		id, ref string
		ago     time.Duration
	}{
		{"00000000-0000-0000-0000-000000000001", "https://a.example", time.Hour},
		{"00000000-0000-0000-0000-000000000002", "https://a.example", 2 * time.Hour},
		{"00000000-0000-0000-0000-000000000003", "https://b.example", 3 * time.Hour},
		{"00000000-0000-0000-0000-000000000004", "", 4 * time.Hour},
		{"00000000-0000-0000-0000-000000000005", "https://old.example", 45 * 24 * time.Hour},
	}
	for _, r := range rows {
		if _, err := pool.Exec(ctx, `INSERT INTO clicks (event_id, slug, clicked_at, referer) VALUES ($1,'s1',$2,$3)`,
			r.id, time.Now().Add(-r.ago), r.ref); err != nil {
			t.Fatal(err)
		}
	}
	s, err := NewStatsRepo(pool).Stats(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if s.TotalClicks != 5 {
		t.Errorf("total = %d", s.TotalClicks)
	}
	var recent int64
	for _, d := range s.ByDay {
		recent += d.Clicks
	}
	if recent != 4 {
		t.Errorf("30d clicks = %d, want 4", recent)
	}
	if len(s.TopReferers) == 0 || s.TopReferers[0].Referer != "https://a.example" || s.TopReferers[0].Clicks != 2 {
		t.Errorf("referers = %+v", s.TopReferers)
	}
	empty, err := NewStatsRepo(pool).Stats(ctx, "none")
	if err != nil || empty.TotalClicks != 0 || empty.ByDay == nil || empty.TopReferers == nil {
		t.Errorf("empty = %+v, %v", empty, err)
	}
}

func TestInsertClicksIdempotent(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	sink := NewClickSink(pool)
	mk := func(id string) events.Click {
		return events.Click{EventID: id, Slug: "s", At: time.Now(), Referer: "r\x00x", UserAgent: "ua\xff", IP: "1.2.3.4"}
	}
	batch := []events.Click{
		mk("10000000-0000-0000-0000-000000000001"),
		mk("10000000-0000-0000-0000-000000000002"),
		mk("10000000-0000-0000-0000-000000000002"), // duplicate inside the batch
	}
	n, err := sink.InsertClicks(ctx, batch)
	if err != nil || n != 2 {
		t.Fatalf("first insert = %d, %v", n, err)
	}
	n, err = sink.InsertClicks(ctx, batch) // full redelivery
	if err != nil || n != 0 {
		t.Fatalf("redelivery = %d, %v", n, err)
	}
	var total int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM clicks`).Scan(&total); err != nil || total != 2 {
		t.Errorf("rows = %d, %v", total, err)
	}
}

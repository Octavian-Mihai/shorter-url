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

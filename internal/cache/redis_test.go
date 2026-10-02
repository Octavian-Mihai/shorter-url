package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Octavian-Mihai/shorter-url/internal/link"
)

func newTestCache(t *testing.T) (*Redis, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	return NewRedis(redis.NewClient(&redis.Options{Addr: mr.Addr()})), mr
}

func TestRedisCacheRoundTripAndTTL(t *testing.T) {
	c, mr := newTestCache(t)
	ctx := context.Background()

	if e, err := c.Get(ctx, "nope"); e != nil || err != nil {
		t.Fatalf("miss = %v, %v", e, err)
	}
	exp := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	want := link.CacheEntry{URL: "https://example.com/x", ExpiresAt: &exp}
	if err := c.Set(ctx, "abc", want, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx, "abc")
	if err != nil || got == nil || got.URL != want.URL || !got.ExpiresAt.Equal(exp) {
		t.Fatalf("got %+v, %v", got, err)
	}
	mr.FastForward(2 * time.Minute)
	if e, _ := c.Get(ctx, "abc"); e != nil {
		t.Error("entry should have expired")
	}
}

func TestRedisCacheNegativeEntry(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()
	_ = c.Set(ctx, "ghost", link.CacheEntry{Missing: true}, time.Minute)
	e, err := c.Get(ctx, "ghost")
	if err != nil || e == nil || !e.Missing {
		t.Fatalf("got %+v, %v", e, err)
	}
}

func TestRedisCacheZeroTTLIsNoop(t *testing.T) {
	c, _ := newTestCache(t)
	_ = c.Set(context.Background(), "z", link.CacheEntry{URL: "https://a.b"}, 0)
	if e, _ := c.Get(context.Background(), "z"); e != nil {
		t.Error("zero ttl must not store")
	}
}

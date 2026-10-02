package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setup(t *testing.T, burst, perMin int) (*Limiter, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	mr.SetTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	l, err := New(redis.NewClient(&redis.Options{Addr: mr.Addr()}), burst, perMin)
	if err != nil {
		t.Fatal(err)
	}
	return l, mr
}

func TestBurstThenReject(t *testing.T) {
	l, _ := setup(t, 3, 60)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		r, err := l.Allow(ctx, "k")
		if err != nil || !r.Allowed {
			t.Fatalf("request %d: %+v, %v", i, r, err)
		}
	}
	r, err := l.Allow(ctx, "k")
	if err != nil || r.Allowed {
		t.Fatalf("4th should be rejected: %+v, %v", r, err)
	}
	if r.RetryAfter <= 0 || r.RetryAfter > time.Second+10*time.Millisecond {
		t.Errorf("RetryAfter = %v, want ~1s at 1 token/sec", r.RetryAfter)
	}
}

func TestRefillOverTime(t *testing.T) {
	l, mr := setup(t, 2, 60) // 1 token/sec
	ctx := context.Background()
	l.Allow(ctx, "k")
	l.Allow(ctx, "k")
	if r, _ := l.Allow(ctx, "k"); r.Allowed {
		t.Fatal("bucket should be empty")
	}
	mr.SetTime(time.Date(2026, 1, 1, 0, 0, 2, 0, time.UTC)) // +2s => 2 tokens
	for i := 0; i < 2; i++ {
		if r, _ := l.Allow(ctx, "k"); !r.Allowed {
			t.Fatalf("token %d should have refilled", i)
		}
	}
	if r, _ := l.Allow(ctx, "k"); r.Allowed {
		t.Error("refill must not exceed elapsed time")
	}
}

func TestRefillCappedAtCapacity(t *testing.T) {
	l, mr := setup(t, 2, 60)
	ctx := context.Background()
	l.Allow(ctx, "k")
	mr.SetTime(time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)) // an hour later
	allowed := 0
	for i := 0; i < 5; i++ {
		if r, _ := l.Allow(ctx, "k"); r.Allowed {
			allowed++
		}
	}
	if allowed != 2 {
		t.Errorf("allowed %d after long idle, want capacity 2", allowed)
	}
}

func TestKeysAreIndependent(t *testing.T) {
	l, _ := setup(t, 1, 60)
	ctx := context.Background()
	if r, _ := l.Allow(ctx, "a"); !r.Allowed {
		t.Fatal("a first")
	}
	if r, _ := l.Allow(ctx, "a"); r.Allowed {
		t.Fatal("a second should be limited")
	}
	if r, _ := l.Allow(ctx, "b"); !r.Allowed {
		t.Error("b must be unaffected by a")
	}
}

func TestInvalidConfig(t *testing.T) {
	if _, err := New(nil, 0, 10); err == nil {
		t.Error("expected error")
	}
}

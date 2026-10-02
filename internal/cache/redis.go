// Package cache provides the Redis-backed cache-aside layer for slug lookups.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Octavian-Mihai/shorter-url/internal/link"
)

type Redis struct{ rdb redis.UniversalClient }

var _ link.Cache = (*Redis)(nil)

func NewRedis(rdb redis.UniversalClient) *Redis { return &Redis{rdb: rdb} }

func key(slug string) string { return "link:" + slug }

func (r *Redis) Get(ctx context.Context, slug string) (*link.CacheEntry, error) {
	b, err := r.rdb.Get(ctx, key(slug)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis get: %w", err)
	}
	var e link.CacheEntry
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("redis decode: %w", err)
	}
	return &e, nil
}

func (r *Redis) Set(ctx context.Context, slug string, e link.CacheEntry, ttl time.Duration) error {
	if ttl <= 0 {
		return nil
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, key(slug), b, ttl).Err(); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}

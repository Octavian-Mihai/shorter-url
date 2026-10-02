// Package ratelimit implements a distributed token bucket on Redis.
package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// The whole read-modify-write runs inside one Lua script, so it is atomic
// across all API instances without locks. Time comes from Redis (TIME), not
// from the callers, so clock skew between instances cannot grant extra tokens.
var script = redis.NewScript(`
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then tokens = capacity; ts = now end
tokens = math.min(capacity, tokens + math.max(0, now - ts) * rate)
local allowed = 0
local retry = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry = (1 - tokens) / rate
end
redis.call('HSET', KEYS[1], 'tokens', string.format('%.6f', tokens), 'ts', string.format('%.6f', now))
redis.call('EXPIRE', KEYS[1], math.ceil(capacity / rate) + 1)
return {allowed, string.format('%.6f', retry)}
`)

type Result struct {
	Allowed    bool
	RetryAfter time.Duration // only meaningful when !Allowed
}

type Limiter struct {
	rdb      redis.Scripter
	capacity int
	perSec   float64
	prefix   string
}

// New builds a limiter allowing bursts up to `burst` and a sustained
// `perMinute` requests per key.
func New(rdb redis.Scripter, burst, perMinute int) (*Limiter, error) {
	if burst <= 0 || perMinute <= 0 {
		return nil, fmt.Errorf("ratelimit: burst and perMinute must be positive")
	}
	return &Limiter{rdb: rdb, capacity: burst, perSec: float64(perMinute) / 60, prefix: "rl:"}, nil
}

// Allow consumes one token for key.
func (l *Limiter) Allow(ctx context.Context, key string) (Result, error) {
	res, err := script.Run(ctx, l.rdb, []string{l.prefix + key}, l.capacity, l.perSec).Slice()
	if err != nil {
		return Result{}, fmt.Errorf("ratelimit: %w", err)
	}
	if len(res) != 2 {
		return Result{}, fmt.Errorf("ratelimit: unexpected script result %v", res)
	}
	allowed, _ := res[0].(int64)
	retryStr, _ := res[1].(string)
	retry, _ := strconv.ParseFloat(retryStr, 64)
	return Result{Allowed: allowed == 1, RetryAfter: time.Duration(retry * float64(time.Second))}, nil
}

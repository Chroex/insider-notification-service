// Package ratelimit is a distributed per-channel rate limiter in Redis: a
// sliding window log that allows at most PerSecond sends in any 1-second
// window.
//
// The window lives in Redis and is updated by a Lua script, which Redis
// executes atomically. That makes the limit global: running 1 or 10 worker
// replicas, the provider still sees at most PerSecond msg/s per channel.
package ratelimit

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"notification-service/internal/domain"
)

var (
	//go:embed acquire.lua
	acquireSource string
	//go:embed refund.lua
	refundSource string

	// redis.Script sends EVALSHA and falls back to EVAL on NOSCRIPT, so the
	// script body crosses the network only once per Redis instance.
	acquireScript = redis.NewScript(acquireSource)
	refundScript  = redis.NewScript(refundSource)
)

// Limit is one channel's limit: sends allowed in any 1-second window.
type Limit struct {
	PerSecond int
}

// Limiter holds the Redis client and per-channel limits. Safe for
// concurrent use.
type Limiter struct {
	rdb       redis.Scripter
	limits    map[domain.Channel]Limit
	keyPrefix string
}

// New returns a limiter. keyPrefix namespaces the window keys (tests use a
// unique prefix so they don't share windows).
func New(rdb redis.Scripter, limits map[domain.Channel]Limit, keyPrefix string) *Limiter {
	return &Limiter{rdb: rdb, limits: limits, keyPrefix: keyPrefix}
}

// TryAcquire makes one attempt to take up to n send slots ("tokens"). It
// returns how many were granted (possibly fewer than n) and, when none
// were, how long until the oldest grant leaves the window.
func (l *Limiter) TryAcquire(ctx context.Context, ch domain.Channel, n int) (int, time.Duration, error) {
	limit, err := l.limit(ch)
	if err != nil {
		return 0, 0, err
	}
	res, err := acquireScript.Run(ctx, l.rdb, l.keys(ch), limit.PerSecond, n).Int64Slice()
	if err != nil {
		return 0, 0, fmt.Errorf("rate limit acquire (%s): %w", ch, err)
	}
	if len(res) != 2 {
		return 0, 0, fmt.Errorf("rate limit acquire (%s): unexpected reply %v", ch, res)
	}
	return int(res[0]), time.Duration(res[1]) * time.Millisecond, nil
}

// Acquire blocks until at least one slot is granted (returning up to n) or
// ctx is done. When the window is full it sleeps exactly until the oldest
// grant expires, so a waiting worker costs one Redis call per wake-up, not
// a busy loop.
func (l *Limiter) Acquire(ctx context.Context, ch domain.Channel, n int) (int, error) {
	for {
		granted, wait, err := l.TryAcquire(ctx, ch, n)
		if err != nil {
			return 0, err
		}
		if granted > 0 {
			return granted, nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
}

// Refund gives back n slots that were granted but not used.
func (l *Limiter) Refund(ctx context.Context, ch domain.Channel, n int) error {
	if n <= 0 {
		return nil
	}
	if _, err := l.limit(ch); err != nil {
		return err
	}
	if err := refundScript.Run(ctx, l.rdb, l.keys(ch)[:1], n).Err(); err != nil {
		return fmt.Errorf("rate limit refund (%s): %w", ch, err)
	}
	return nil
}

func (l *Limiter) limit(ch domain.Channel) (Limit, error) {
	limit, ok := l.limits[ch]
	if !ok {
		return Limit{}, fmt.Errorf("no rate limit configured for channel %q", ch)
	}
	return limit, nil
}

// keys returns the window key and its sequence key. The {braces} hash tag
// keeps both in one Redis Cluster slot, as a multi-key script requires.
func (l *Limiter) keys(ch domain.Channel) []string {
	base := l.keyPrefix + "ratelimit:{" + string(ch) + "}"
	return []string{base, base + ":seq"}
}

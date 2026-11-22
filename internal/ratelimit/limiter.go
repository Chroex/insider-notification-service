// Package ratelimit is a distributed per-channel rate limiter in Redis: a
// sliding window of sends. A send's slot is held while it is in flight and
// for one second after it finishes, so the provider, whenever it receives a
// request, never sees more than PerSecond in any 1-second window.
//
// The window lives in Redis and is updated by Lua scripts, which Redis
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
	//go:embed done.lua
	doneSource string

	// redis.Script sends EVALSHA and falls back to EVAL on NOSCRIPT, so the
	// script body crosses the network only once per Redis instance.
	acquireScript = redis.NewScript(acquireSource)
	doneScript    = redis.NewScript(doneSource)
)

// DefaultMaxSend bounds how long a granted send may stay in flight before
// its slot is reclaimed anyway (a worker that died mid-send). The worker
// sets it from PROVIDER_TIMEOUT with WithMaxSend.
const DefaultMaxSend = 10 * time.Second

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
	maxSend   time.Duration
}

// New returns a limiter. keyPrefix namespaces the window keys (tests use a
// unique prefix so they don't share windows).
func New(rdb redis.Scripter, limits map[domain.Channel]Limit, keyPrefix string) *Limiter {
	return &Limiter{rdb: rdb, limits: limits, keyPrefix: keyPrefix, maxSend: DefaultMaxSend}
}

// WithMaxSend sets the longest a send can take (the provider timeout plus a
// margin): a slot whose send never reports back is freed after it.
func (l *Limiter) WithMaxSend(d time.Duration) *Limiter {
	l.maxSend = d
	return l
}

// TryAcquire makes one attempt to take a send slot. When granted, done is
// non-nil and must be called as soon as the send returns (successfully or
// not); the slot then frees one second later. When not granted, wait says
// how long to sleep before trying again.
func (l *Limiter) TryAcquire(ctx context.Context, ch domain.Channel) (done func(), wait time.Duration, err error) {
	limit, err := l.limit(ch)
	if err != nil {
		return nil, 0, err
	}
	keys := l.keys(ch)
	res, err := acquireScript.Run(ctx, l.rdb, keys, limit.PerSecond, l.maxSend.Microseconds()).Int64Slice()
	if err != nil {
		return nil, 0, fmt.Errorf("rate limit acquire (%s): %w", ch, err)
	}
	if len(res) != 2 {
		return nil, 0, fmt.Errorf("rate limit acquire (%s): unexpected reply %v", ch, res)
	}
	if member := res[0]; member > 0 {
		return func() { l.done(keys[0], member) }, 0, nil
	}
	return nil, time.Duration(res[1]) * time.Millisecond, nil
}

// Acquire blocks until a send slot is granted or ctx is done. While the
// window is full it sleeps until a slot may have freed, so a waiting worker
// costs a few Redis calls per second, not a busy loop.
func (l *Limiter) Acquire(ctx context.Context, ch domain.Channel) (done func(), err error) {
	for {
		done, wait, err := l.TryAcquire(ctx, ch)
		if err != nil || done != nil {
			return done, err
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// done records that a send finished. If it fails, the slot is still freed
// once the max send time has passed, so an error only costs throughput.
func (l *Limiter) done(key string, member int64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = doneScript.Run(ctx, l.rdb, []string{key}, member).Err()
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

package worker

import (
	"sync"
	"time"

	"notification-service/internal/domain"
)

// BreakerState is the circuit breaker state of one channel.
type BreakerState int

const (
	// BreakerClosed: normal operation, sends flow.
	BreakerClosed BreakerState = iota
	// BreakerOpen: the provider looks down; no sends until the cooldown ends.
	BreakerOpen
	// BreakerHalfOpen: cooldown over; exactly one probe send is allowed to
	// test whether the provider has recovered.
	BreakerHalfOpen
)

func (s BreakerState) String() string {
	switch s {
	case BreakerClosed:
		return "closed"
	case BreakerOpen:
		return "open"
	case BreakerHalfOpen:
		return "half_open"
	}
	return "unknown"
}

// Unlimited permits: the breaker adds no limit beyond slots and rate limit.
const breakerNoLimit = -1

// Breaker stops a channel from hammering a provider that is failing.
//
//	closed --(threshold consecutive retryable failures, see below)--> open
//	open   --(cooldown elapsed)--> half_open (one probe allowed)
//	half_open --(probe succeeds)--> closed
//	half_open --(probe fails)-----> open (cooldown restarts)
//
// Without it, a provider outage turns every queued notification into a
// fast stream of failed attempts: each burns retry budget and the extra
// traffic slows the provider's recovery. While the breaker is open nothing
// is claimed, so notifications simply stay queued and lose no attempts.
//
// What counts as "threshold consecutive failures":
//   - Only retryable failures (5xx, 429, timeouts, network). A permanent 4xx
//     means the provider is up and judged that one request bad, so it resets
//     the streak like a success.
//   - They must come from threshold DISTINCT notifications. One poison
//     message that always gets a 503 would otherwise open the breaker on
//     its own through its retries, pausing the whole channel at low traffic.
//   - Failures more than `window` apart are not consecutive. Without this, a
//     streak left over from an hour ago plus two fresh failures would open
//     the breaker. (Both cases were found by the end-to-end run; see ADR-024.)
type Breaker struct {
	threshold int
	window    time.Duration
	cooldown  time.Duration
	now       func() time.Time
	onChange  func(from, to BreakerState)

	mu          sync.Mutex
	state       BreakerState
	failing     map[string]struct{} // distinct notifications in the current failure streak
	lastFailure time.Time
	openedAt    time.Time
	probing     bool // half-open probe permit handed out and not yet resolved
	// generation counts state changes. A job carries the generation it was
	// claimed in; a send or result from an older generation belongs to a
	// period that is over, so it is not sent (Current) or not counted
	// (Record). Without it, a slow send from before the breaker opened
	// could finish during half-open and close the breaker before the
	// real probe answered.
	generation uint64
}

// NewBreaker builds a closed breaker. onChange (optional) is called after
// every state transition, outside the lock.
func NewBreaker(threshold int, window, cooldown time.Duration, onChange func(from, to BreakerState)) *Breaker {
	return &Breaker{
		threshold: threshold, window: window, cooldown: cooldown,
		now: time.Now, onChange: onChange, failing: map[string]struct{}{},
	}
}

// Permits reports how many new sends may start: breakerNoLimit when closed,
// 1 for the half-open probe, 0 otherwise. With 0, wait is the remaining
// cooldown (or 0 if a probe is already in flight).
//
// gen is the breaker period the permits belong to; pass it to Current
// before sending and to Record with the result.
func (b *Breaker) Permits() (permits int, wait time.Duration, gen uint64) {
	b.mu.Lock()
	from := b.state
	switch b.state {
	case BreakerClosed:
		defer b.mu.Unlock()
		return breakerNoLimit, 0, b.generation
	case BreakerOpen:
		if remaining := b.cooldown - b.now().Sub(b.openedAt); remaining > 0 {
			defer b.mu.Unlock()
			return 0, remaining, b.generation
		}
		b.state, b.probing = BreakerHalfOpen, true
		b.generation++
		gen := b.generation
		b.mu.Unlock()
		b.notify(from, BreakerHalfOpen)
		return 1, 0, gen
	default: // half-open
		defer b.mu.Unlock()
		if b.probing {
			return 0, 0, b.generation
		}
		b.probing = true
		return 1, 0, b.generation
	}
}

// ReleaseProbe returns a half-open probe permit that ended without a
// result: nothing was due when we claimed, or the claimed job was handed
// back before its send (shutdown, lost lease, failed ownership check). The
// next loop iteration can then probe again; without this the breaker would
// stay "probing" forever and the channel would never send again. gen is the
// period the permit came from (Permits), so a job from an older period
// cannot free the slot of the probe that is in flight now.
func (b *Breaker) ReleaseProbe(gen uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == BreakerHalfOpen && gen == b.generation {
		b.probing = false
	}
}

// Record feeds the outcome of one provider call into the breaker. key
// identifies the notification, so retries of the same one count once.
//
// gen is the generation the send's permits came from: a result from an
// earlier period is ignored.
func (b *Breaker) Record(outcome domain.AttemptOutcome, key string, gen uint64) {
	failed := outcome == domain.OutcomeRetryableError

	b.mu.Lock()
	if gen != b.generation {
		b.mu.Unlock()
		return
	}
	from, to := b.state, b.state
	switch b.state {
	case BreakerClosed:
		if !failed {
			clear(b.failing)
			break
		}
		now := b.now()
		if now.Sub(b.lastFailure) > b.window {
			clear(b.failing) // the previous streak went quiet: start over
		}
		b.lastFailure = now
		b.failing[key] = struct{}{}
		if len(b.failing) >= b.threshold {
			to = BreakerOpen
		}
	case BreakerHalfOpen:
		b.probing = false
		if failed {
			to = BreakerOpen
		} else {
			to = BreakerClosed
		}
	case BreakerOpen:
		// Unreachable: opening starts a new generation and no sends are
		// permitted while open, so every result here was dropped above.
	}
	if to != from {
		b.state = to
		b.generation++
		clear(b.failing)
		if to == BreakerOpen {
			b.openedAt = b.now()
		}
	}
	b.mu.Unlock()

	if to != from {
		b.notify(from, to)
	}
}

// Generation is the current breaker period.
func (b *Breaker) Generation() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.generation
}

// Current reports whether the breaker is still in the period gen came
// from, i.e. no state change happened since those permits were given.
func (b *Breaker) Current(gen uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return gen == b.generation
}

// IsOpen reports whether sends must be held back right now.
func (b *Breaker) IsOpen() bool {
	return b.State() == BreakerOpen
}

func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) notify(from, to BreakerState) {
	if b.onChange != nil {
		b.onChange(from, to)
	}
}

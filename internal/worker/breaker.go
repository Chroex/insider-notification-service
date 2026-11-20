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
//	closed --(threshold consecutive retryable failures)--> open
//	open   --(cooldown elapsed)--> half_open (one probe allowed)
//	half_open --(probe succeeds)--> closed
//	half_open --(probe fails)-----> open (cooldown restarts)
//
// Without it, a provider outage turns every queued notification into a
// fast stream of failed attempts: each burns retry budget and the extra
// traffic slows the provider's recovery. While the breaker is open nothing
// is claimed, so notifications simply stay queued and lose no attempts.
//
// Only retryable failures (5xx, 429, timeouts, network) count. A permanent
// 4xx means the provider is up and judged that one request bad, so it
// resets the streak like a success.
type Breaker struct {
	threshold int
	cooldown  time.Duration
	now       func() time.Time
	onChange  func(from, to BreakerState)

	mu       sync.Mutex
	state    BreakerState
	failures int // consecutive retryable failures while closed
	openedAt time.Time
	probing  bool // half-open probe permit handed out and not yet resolved
}

// NewBreaker builds a closed breaker. onChange (optional) is called after
// every state transition, outside the lock.
func NewBreaker(threshold int, cooldown time.Duration, onChange func(from, to BreakerState)) *Breaker {
	return &Breaker{threshold: threshold, cooldown: cooldown, now: time.Now, onChange: onChange}
}

// Permits reports how many new sends may start: breakerNoLimit when closed,
// 1 for the half-open probe, 0 otherwise. With 0, wait is the remaining
// cooldown (or 0 if a probe is already in flight).
func (b *Breaker) Permits() (permits int, wait time.Duration) {
	b.mu.Lock()
	from := b.state
	switch b.state {
	case BreakerClosed:
		b.mu.Unlock()
		return breakerNoLimit, 0
	case BreakerOpen:
		if remaining := b.cooldown - b.now().Sub(b.openedAt); remaining > 0 {
			b.mu.Unlock()
			return 0, remaining
		}
		b.state, b.probing = BreakerHalfOpen, true
		b.mu.Unlock()
		b.notify(from, BreakerHalfOpen)
		return 1, 0
	default: // half-open
		defer b.mu.Unlock()
		if b.probing {
			return 0, 0
		}
		b.probing = true
		return 1, 0
	}
}

// ReleaseProbe returns an unused half-open probe permit (e.g. nothing was
// due when we tried to claim), so the next loop iteration can probe again.
func (b *Breaker) ReleaseProbe() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == BreakerHalfOpen {
		b.probing = false
	}
}

// Record feeds the outcome of one provider call into the breaker.
func (b *Breaker) Record(outcome domain.AttemptOutcome) {
	failed := outcome == domain.OutcomeRetryableError

	b.mu.Lock()
	from, to := b.state, b.state
	switch b.state {
	case BreakerClosed:
		if !failed {
			b.failures = 0
			break
		}
		b.failures++
		if b.failures >= b.threshold {
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
		// Results of sends that started before the breaker opened carry no
		// new information; the half-open probe decides recovery.
	}
	if to != from {
		b.state = to
		b.failures = 0
		if to == BreakerOpen {
			b.openedAt = b.now()
		}
	}
	b.mu.Unlock()

	if to != from {
		b.notify(from, to)
	}
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

package worker

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"notification-service/internal/domain"
)

// maxRetryAfter bounds a provider's Retry-After. We honour it, but a buggy
// or hostile header ("Retry-After: 31536000") must not park a message for a
// year.
const maxRetryAfter = time.Hour

// Backoff computes retry delays: exponential growth with full jitter.
//
//	ceiling(k) = min(Max, Base * Factor^(k-1))   for the k-th failed attempt
//	delay(k)   = uniform random in [0, ceiling(k))
//
// Why full jitter: when a provider has an outage, thousands of notifications
// fail at the same moment. Plain exponential backoff retries them all in
// synchronized waves (thundering herd) that knock the provider over again.
// Spreading each retry uniformly across the whole window de-correlates them;
// AWS's analysis found full jitter gives the least total work and the
// fastest recovery among the common variants.
type Backoff struct {
	Base   time.Duration
	Factor float64
	Max    time.Duration
	// Random returns a uniform value in [0, 1); injectable for tests.
	Random func() float64
}

// Ceiling is the upper bound of the delay after the attempt-th failure.
func (b Backoff) Ceiling(attempt int) time.Duration {
	ceiling := float64(b.Base)
	// Multiply step by step and stop at Max, so huge attempt numbers can't
	// overflow time.Duration.
	for i := 1; i < attempt && ceiling < float64(b.Max); i++ {
		ceiling *= b.Factor
	}
	return time.Duration(math.Min(ceiling, float64(b.Max)))
}

// Delay returns a jittered delay for the attempt-th failure.
func (b Backoff) Delay(attempt int) time.Duration {
	random := b.Random
	if random == nil {
		random = rand.Float64
	}
	return time.Duration(random() * float64(b.Ceiling(attempt)))
}

// Decision is the state a notification moves to after an attempt.
type Decision struct {
	Status domain.Status
	// RetryDelay is how long to wait before the next attempt (StatusRetrying
	// only). It is stored as now() + delay on the database clock.
	RetryDelay *time.Duration
	// NextAttemptAt is the same moment by the worker's clock, for logs.
	NextAttemptAt *time.Time
	LastError     *string
	FailReason    string // "permanent_error" or "max_attempts" when failed
}

// RetryPolicy turns an attempt's classification into the next state.
type RetryPolicy struct {
	Backoff Backoff
}

// Decide applies the policy. attempt is the 1-based number of the attempt
// just made; maxAttempts is the notification's budget.
func (p RetryPolicy) Decide(attempt, maxAttempts int, c Classification, now time.Time) Decision {
	switch c.Outcome {
	case domain.OutcomeSuccess:
		return Decision{Status: domain.StatusSent}

	case domain.OutcomePermanentError:
		msg := c.Err
		return Decision{Status: domain.StatusFailed, LastError: &msg, FailReason: "permanent_error"}

	default: // retryable
		if attempt >= maxAttempts {
			msg := fmt.Sprintf("giving up after %d attempts: %s", attempt, c.Err)
			return Decision{Status: domain.StatusFailed, LastError: &msg, FailReason: "max_attempts"}
		}
		delay := p.delay(attempt, c)
		next := now.Add(delay)
		msg := c.Err
		return Decision{Status: domain.StatusRetrying, RetryDelay: &delay, NextAttemptAt: &next, LastError: &msg}
	}
}

// delay is our jittered backoff, unless the provider asked (Retry-After) for
// longer: the provider knows its own capacity better than we do.
func (p RetryPolicy) delay(attempt int, c Classification) time.Duration {
	return max(p.Backoff.Delay(attempt), min(c.RetryAfter, maxRetryAfter))
}

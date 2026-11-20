package worker

import (
	"fmt"
	"time"

	"notification-service/internal/domain"
)

// Decision is the state a notification moves to after an attempt.
type Decision struct {
	Status        domain.Status
	NextAttemptAt *time.Time // set only for StatusRetrying
	LastError     *string
	FailReason    string // "permanent_error" or "max_attempts" when failed
}

// RetryPolicy turns an attempt's classification into the next state.
type RetryPolicy struct {
	// BaseDelay is the minimum wait before a retry.
	BaseDelay time.Duration
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
		next := now.Add(p.delay(c))
		msg := c.Err
		return Decision{Status: domain.StatusRetrying, NextAttemptAt: &next, LastError: &msg}
	}
}

// delay honours a provider's Retry-After when it asks for longer than our
// own delay: the provider knows its own capacity better than we do.
func (p RetryPolicy) delay(c Classification) time.Duration {
	return max(p.BaseDelay, c.RetryAfter)
}

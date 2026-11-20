// Package worker is the delivery engine: it claims due notifications,
// calls the provider and records each outcome.
package worker

import (
	"net/http"
	"time"

	"notification-service/internal/domain"
	"notification-service/internal/provider"
)

// Classification is the interpretation of one provider call.
type Classification struct {
	Outcome    domain.AttemptOutcome
	HTTPStatus *int          // nil when no HTTP response was received
	RetryAfter time.Duration // provider-requested delay, 0 if none
	Err        string        // human-readable error, "" on success
}

// Classify decides whether a provider error is worth retrying.
//
//	Retryable: network errors, timeouts, 5xx, 429 Too Many Requests,
//	           408 Request Timeout. The same request may succeed later.
//	Permanent: every other 4xx (bad recipient, bad payload, auth) and
//	           unexpected 1xx/3xx. Retrying an identical request cannot
//	           succeed, it only wastes provider quota, so fail immediately.
func Classify(err error) Classification {
	if err == nil {
		return Classification{Outcome: domain.OutcomeSuccess}
	}

	se, ok := provider.IsStatusError(err)
	if !ok {
		// No HTTP response: DNS failure, connection refused/reset, TLS error,
		// per-attempt timeout, or shutdown cancellation. All transient.
		return Classification{Outcome: domain.OutcomeRetryableError, Err: err.Error()}
	}

	status := se.StatusCode
	c := Classification{HTTPStatus: &status, RetryAfter: se.RetryAfter, Err: se.Error()}
	if isRetryableStatus(status) {
		c.Outcome = domain.OutcomeRetryableError
	} else {
		c.Outcome = domain.OutcomePermanentError
	}
	return c
}

func isRetryableStatus(code int) bool {
	switch {
	case code >= 500 && code <= 599:
		return true
	case code == http.StatusTooManyRequests, code == http.StatusRequestTimeout:
		return true
	default:
		return false
	}
}

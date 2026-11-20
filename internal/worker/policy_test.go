package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"notification-service/internal/domain"
	"notification-service/internal/provider"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		want       domain.AttemptOutcome
		wantStatus int // 0 = no HTTP status
	}{
		{"success", nil, domain.OutcomeSuccess, 0},
		{"timeout", context.DeadlineExceeded, domain.OutcomeRetryableError, 0},
		{"connection refused", errors.New("dial tcp: connection refused"), domain.OutcomeRetryableError, 0},
		{"500", &provider.StatusError{StatusCode: 500}, domain.OutcomeRetryableError, 500},
		{"503", &provider.StatusError{StatusCode: 503}, domain.OutcomeRetryableError, 503},
		{"429", &provider.StatusError{StatusCode: 429}, domain.OutcomeRetryableError, 429},
		{"408", &provider.StatusError{StatusCode: 408}, domain.OutcomeRetryableError, 408},
		{"400", &provider.StatusError{StatusCode: 400}, domain.OutcomePermanentError, 400},
		{"401", &provider.StatusError{StatusCode: 401}, domain.OutcomePermanentError, 401},
		{"404", &provider.StatusError{StatusCode: 404}, domain.OutcomePermanentError, 404},
		{"422", &provider.StatusError{StatusCode: 422}, domain.OutcomePermanentError, 422},
		{"301 redirect", &provider.StatusError{StatusCode: 301}, domain.OutcomePermanentError, 301},
		{"wrapped 503", fmt.Errorf("send: %w", &provider.StatusError{StatusCode: 503}), domain.OutcomeRetryableError, 503},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Classify(tt.err)
			if c.Outcome != tt.want {
				t.Errorf("outcome = %s, want %s", c.Outcome, tt.want)
			}
			gotStatus := 0
			if c.HTTPStatus != nil {
				gotStatus = *c.HTTPStatus
			}
			if gotStatus != tt.wantStatus {
				t.Errorf("http status = %d, want %d", gotStatus, tt.wantStatus)
			}
		})
	}
}

func TestClassify_KeepsRetryAfter(t *testing.T) {
	c := Classify(&provider.StatusError{StatusCode: 429, RetryAfter: 9 * time.Second})
	if c.RetryAfter != 9*time.Second {
		t.Errorf("RetryAfter = %v", c.RetryAfter)
	}
}

func TestRetryPolicy_Decide(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p := RetryPolicy{BaseDelay: 2 * time.Second}
	retryable := Classification{Outcome: domain.OutcomeRetryableError, Err: "HTTP 503"}

	if d := p.Decide(1, 5, Classification{Outcome: domain.OutcomeSuccess}, now); d.Status != domain.StatusSent {
		t.Errorf("success -> %s", d.Status)
	}

	d := p.Decide(1, 5, Classification{Outcome: domain.OutcomePermanentError, Err: "HTTP 400"}, now)
	if d.Status != domain.StatusFailed || d.FailReason != "permanent_error" || *d.LastError != "HTTP 400" {
		t.Errorf("permanent -> %+v", d)
	}

	d = p.Decide(2, 5, retryable, now)
	if d.Status != domain.StatusRetrying || !d.NextAttemptAt.Equal(now.Add(2*time.Second)) {
		t.Errorf("retryable -> %+v", d)
	}

	d = p.Decide(5, 5, retryable, now)
	if d.Status != domain.StatusFailed || d.FailReason != "max_attempts" || !strings.Contains(*d.LastError, "5 attempts") {
		t.Errorf("exhausted -> %+v", d)
	}

	longer := retryable
	longer.RetryAfter = 30 * time.Second
	if d := p.Decide(1, 5, longer, now); !d.NextAttemptAt.Equal(now.Add(30 * time.Second)) {
		t.Errorf("Retry-After not honoured: next = %v", d.NextAttemptAt)
	}
}

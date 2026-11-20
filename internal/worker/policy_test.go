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

func TestBackoff_CeilingGrowsExponentiallyUpToMax(t *testing.T) {
	b := Backoff{Base: 2 * time.Second, Factor: 2, Max: 5 * time.Minute}
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{5, 32 * time.Second},
		{8, 256 * time.Second},
		{9, 5 * time.Minute},      // 512s capped at 300s
		{10_000, 5 * time.Minute}, // no overflow
	}
	for _, tt := range tests {
		if got := b.Ceiling(tt.attempt); got != tt.want {
			t.Errorf("Ceiling(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestBackoff_FullJitterBounds(t *testing.T) {
	b := Backoff{Base: 2 * time.Second, Factor: 2, Max: 5 * time.Minute}
	// Deterministic edges of the random source.
	for _, tt := range []struct {
		random float64
		want   time.Duration
	}{
		{0, 0},
		{0.5, 4 * time.Second}, // half of the 8s ceiling at attempt 3
		{0.999999, 8*time.Second - 8*time.Microsecond},
	} {
		b.Random = func() float64 { return tt.random }
		if got := b.Delay(3); got != tt.want {
			t.Errorf("Delay(3) with random=%v = %v, want %v", tt.random, got, tt.want)
		}
	}

	// Real randomness: always within [0, ceiling) and actually spread out.
	b.Random = nil
	seen := map[time.Duration]bool{}
	for attempt := 1; attempt <= 12; attempt++ {
		ceiling := b.Ceiling(attempt)
		for range 200 {
			d := b.Delay(attempt)
			if d < 0 || d >= ceiling {
				t.Fatalf("Delay(%d) = %v outside [0, %v)", attempt, d, ceiling)
			}
			seen[d] = true
		}
	}
	if len(seen) < 1000 {
		t.Errorf("only %d distinct delays in 2400 draws: jitter is not spreading retries", len(seen))
	}
}

func TestRetryPolicy_Decide(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	half := func() float64 { return 0.5 }
	p := RetryPolicy{Backoff: Backoff{Base: 2 * time.Second, Factor: 2, Max: 5 * time.Minute, Random: half}}
	retryable := Classification{Outcome: domain.OutcomeRetryableError, Err: "HTTP 503"}

	if d := p.Decide(1, 5, Classification{Outcome: domain.OutcomeSuccess}, now); d.Status != domain.StatusSent {
		t.Errorf("success -> %s", d.Status)
	}

	d := p.Decide(1, 5, Classification{Outcome: domain.OutcomePermanentError, Err: "HTTP 400"}, now)
	if d.Status != domain.StatusFailed || d.FailReason != "permanent_error" || *d.LastError != "HTTP 400" {
		t.Errorf("permanent -> %+v", d)
	}

	// Attempt 2 failed: ceiling 4s, random 0.5 -> 2s.
	d = p.Decide(2, 5, retryable, now)
	if d.Status != domain.StatusRetrying || !d.NextAttemptAt.Equal(now.Add(2*time.Second)) {
		t.Errorf("retryable -> %+v", d)
	}

	d = p.Decide(5, 5, retryable, now)
	if d.Status != domain.StatusFailed || d.FailReason != "max_attempts" || !strings.Contains(*d.LastError, "5 attempts") {
		t.Errorf("exhausted -> %+v", d)
	}
}

func TestRetryPolicy_HonoursRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p := RetryPolicy{Backoff: Backoff{Base: 2 * time.Second, Factor: 2, Max: 5 * time.Minute, Random: func() float64 { return 0.5 }}}
	c := Classification{Outcome: domain.OutcomeRetryableError, Err: "HTTP 429"}

	c.RetryAfter = 30 * time.Second // longer than our 1s jittered delay: provider wins
	if d := p.Decide(1, 5, c, now); !d.NextAttemptAt.Equal(now.Add(30 * time.Second)) {
		t.Errorf("Retry-After 30s: next = %v", d.NextAttemptAt.Sub(now))
	}

	c.RetryAfter = 100 * time.Millisecond // shorter than our delay: ours wins
	if d := p.Decide(1, 5, c, now); !d.NextAttemptAt.Equal(now.Add(time.Second)) {
		t.Errorf("short Retry-After: next = %v, want 1s", d.NextAttemptAt.Sub(now))
	}

	c.RetryAfter = 365 * 24 * time.Hour // absurd header is bounded
	if d := p.Decide(1, 5, c, now); !d.NextAttemptAt.Equal(now.Add(time.Hour)) {
		t.Errorf("huge Retry-After: next = %v, want capped at 1h", d.NextAttemptAt.Sub(now))
	}
}

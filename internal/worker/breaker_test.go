package worker

import (
	"strings"
	"sync"
	"testing"
	"time"

	"notification-service/internal/domain"
)

const (
	ok        = domain.OutcomeSuccess
	retryable = domain.OutcomeRetryableError
	permanent = domain.OutcomePermanentError
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestBreaker(threshold int, cooldown time.Duration) (*Breaker, *fakeClock, *[]string) {
	clock := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	var transitions []string
	b := NewBreaker(threshold, cooldown, func(from, to BreakerState) {
		transitions = append(transitions, from.String()+"->"+to.String())
	})
	b.now = clock.now
	return b, clock, &transitions
}

func record(b *Breaker, outcomes ...domain.AttemptOutcome) {
	for _, o := range outcomes {
		b.Record(o)
	}
}

func TestBreaker_OpensAfterConsecutiveRetryableFailures(t *testing.T) {
	tests := []struct {
		name     string
		outcomes []domain.AttemptOutcome
		want     BreakerState
	}{
		{"below threshold", []domain.AttemptOutcome{retryable, retryable}, BreakerClosed},
		{"at threshold", []domain.AttemptOutcome{retryable, retryable, retryable}, BreakerOpen},
		{"success resets the streak", []domain.AttemptOutcome{retryable, retryable, ok, retryable, retryable}, BreakerClosed},
		{"permanent 4xx resets the streak", []domain.AttemptOutcome{retryable, retryable, permanent, retryable, retryable}, BreakerClosed},
		{"only permanent errors never open", []domain.AttemptOutcome{permanent, permanent, permanent, permanent}, BreakerClosed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, _, _ := newTestBreaker(3, time.Minute)
			record(b, tt.outcomes...)
			if got := b.State(); got != tt.want {
				t.Errorf("state = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestBreaker_FullCycle(t *testing.T) {
	b, clock, transitions := newTestBreaker(2, 30*time.Second)

	if p, _ := b.Permits(); p != breakerNoLimit {
		t.Fatalf("closed permits = %d, want unlimited", p)
	}

	record(b, retryable, retryable) // -> open
	p, wait := b.Permits()
	if p != 0 || wait != 30*time.Second || !b.IsOpen() {
		t.Fatalf("open: permits=%d wait=%v", p, wait)
	}

	clock.advance(20 * time.Second)
	if p, wait := b.Permits(); p != 0 || wait != 10*time.Second {
		t.Fatalf("mid-cooldown: permits=%d wait=%v, want 0 and 10s", p, wait)
	}

	clock.advance(10 * time.Second) // cooldown over -> half-open, one probe
	if p, _ := b.Permits(); p != 1 || b.State() != BreakerHalfOpen {
		t.Fatalf("after cooldown: permits=%d state=%s", p, b.State())
	}
	if p, _ := b.Permits(); p != 0 {
		t.Fatalf("second caller during probe got %d permits, want 0", p)
	}

	b.Record(retryable) // probe fails -> open again, cooldown restarts
	if p, wait := b.Permits(); p != 0 || wait != 30*time.Second {
		t.Fatalf("failed probe: permits=%d wait=%v", p, wait)
	}

	clock.advance(30 * time.Second)
	if p, _ := b.Permits(); p != 1 {
		t.Fatal("expected a second probe")
	}
	b.Record(ok) // probe succeeds -> closed
	if p, _ := b.Permits(); p != breakerNoLimit || b.State() != BreakerClosed {
		t.Fatalf("after good probe: permits=%d state=%s", p, b.State())
	}

	want := "closed->open,open->half_open,half_open->open,open->half_open,half_open->closed"
	if got := strings.Join(*transitions, ","); got != want {
		t.Errorf("transitions = %s\nwant          %s", got, want)
	}
}

func TestBreaker_ReleaseProbeAllowsAnotherProbe(t *testing.T) {
	b, clock, _ := newTestBreaker(1, time.Second)
	b.Record(retryable)
	clock.advance(time.Second)
	if p, _ := b.Permits(); p != 1 {
		t.Fatal("expected probe permit")
	}
	b.ReleaseProbe() // nothing was due to send
	if p, _ := b.Permits(); p != 1 {
		t.Error("released probe permit was not handed out again")
	}
}

func TestBreaker_IgnoresLateResultsWhileOpen(t *testing.T) {
	b, _, transitions := newTestBreaker(1, time.Minute)
	b.Record(retryable) // open
	record(b, ok, ok, retryable)
	if b.State() != BreakerOpen || len(*transitions) != 1 {
		t.Errorf("state = %s, transitions = %v; late results must not change an open breaker",
			b.State(), *transitions)
	}
}

func TestBreaker_ConcurrentUse(t *testing.T) {
	b := NewBreaker(5, time.Millisecond, nil)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 1000 {
				b.Permits()
				if (i+j)%3 == 0 {
					b.Record(ok)
				} else {
					b.Record(retryable)
				}
				b.ReleaseProbe()
			}
		}()
	}
	wg.Wait() // run with -race
}

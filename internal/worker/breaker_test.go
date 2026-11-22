package worker

import (
	"fmt"
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
	b := NewBreaker(threshold, time.Minute, cooldown, func(from, to BreakerState) {
		transitions = append(transitions, from.String()+"->"+to.String())
	})
	b.now = clock.now
	return b, clock, &transitions
}

// record feeds outcomes for distinct notifications n1, n2, ...
var keySeq int

func record(b *Breaker, outcomes ...domain.AttemptOutcome) {
	for _, o := range outcomes {
		keySeq++
		b.Record(o, fmt.Sprintf("n%d", keySeq), b.Generation())
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

	if p, _, _ := b.Permits(); p != breakerNoLimit {
		t.Fatalf("closed permits = %d, want unlimited", p)
	}

	record(b, retryable, retryable) // -> open
	p, wait, _ := b.Permits()
	if p != 0 || wait != 30*time.Second || !b.IsOpen() {
		t.Fatalf("open: permits=%d wait=%v", p, wait)
	}

	clock.advance(20 * time.Second)
	if p, wait, _ := b.Permits(); p != 0 || wait != 10*time.Second {
		t.Fatalf("mid-cooldown: permits=%d wait=%v, want 0 and 10s", p, wait)
	}

	clock.advance(10 * time.Second) // cooldown over -> half-open, one probe
	if p, _, _ := b.Permits(); p != 1 || b.State() != BreakerHalfOpen {
		t.Fatalf("after cooldown: permits=%d state=%s", p, b.State())
	}
	if p, _, _ := b.Permits(); p != 0 {
		t.Fatalf("second caller during probe got %d permits, want 0", p)
	}

	record(b, retryable) // probe fails -> open again, cooldown restarts
	if p, wait, _ := b.Permits(); p != 0 || wait != 30*time.Second {
		t.Fatalf("failed probe: permits=%d wait=%v", p, wait)
	}

	clock.advance(30 * time.Second)
	if p, _, _ := b.Permits(); p != 1 {
		t.Fatal("expected a second probe")
	}
	record(b, ok) // probe succeeds -> closed
	if p, _, _ := b.Permits(); p != breakerNoLimit || b.State() != BreakerClosed {
		t.Fatalf("after good probe: permits=%d state=%s", p, b.State())
	}

	want := "closed->open,open->half_open,half_open->open,open->half_open,half_open->closed"
	if got := strings.Join(*transitions, ","); got != want {
		t.Errorf("transitions = %s\nwant          %s", got, want)
	}
}

func TestBreaker_ReleaseProbeAllowsAnotherProbe(t *testing.T) {
	b, clock, _ := newTestBreaker(1, time.Second)
	record(b, retryable)
	clock.advance(time.Second)
	if p, _, _ := b.Permits(); p != 1 {
		t.Fatal("expected probe permit")
	}
	b.ReleaseProbe(b.Generation()) // nothing was due to send
	if p, _, _ := b.Permits(); p != 1 {
		t.Error("released probe permit was not handed out again")
	}
}

func TestBreaker_IgnoresLateResultsWhileOpen(t *testing.T) {
	b, _, transitions := newTestBreaker(1, time.Minute)
	record(b, retryable) // open
	record(b, ok, ok, retryable)
	if b.State() != BreakerOpen || len(*transitions) != 1 {
		t.Errorf("state = %s, transitions = %v; late results must not change an open breaker",
			b.State(), *transitions)
	}
}

func TestBreaker_ConcurrentUse(t *testing.T) {
	b := NewBreaker(5, time.Second, time.Millisecond, nil)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 1000 {
				b.Permits()
				key := fmt.Sprintf("g%d-%d", i, j)
				if (i+j)%3 == 0 {
					b.Record(ok, key, b.Generation())
				} else {
					b.Record(retryable, key, b.Generation())
				}
				b.ReleaseProbe(b.Generation())
			}
		}()
	}
	wg.Wait() // run with -race
}

// A single poison message (e.g. always 503) retried many times must not
// open the breaker for the whole channel on its own.
func TestBreaker_RetriesOfOneNotificationCountOnce(t *testing.T) {
	b, _, _ := newTestBreaker(3, time.Minute)
	for range 10 {
		b.Record(retryable, "poison", b.Generation())
	}
	if b.State() != BreakerClosed {
		t.Fatal("one notification's retries opened the breaker")
	}
	b.Record(retryable, "a", b.Generation())
	b.Record(retryable, "b", b.Generation()) // poison + a + b = 3 distinct
	if b.State() != BreakerOpen {
		t.Errorf("state = %s, want open after 3 distinct failing notifications", b.State())
	}
}

// Failures separated by a quiet period are not consecutive.
func TestBreaker_StaleFailuresExpire(t *testing.T) {
	b, clock, _ := newTestBreaker(3, time.Minute) // window is 1 minute
	record(b, retryable, retryable)
	clock.advance(2 * time.Minute)
	record(b, retryable, retryable)
	if b.State() != BreakerClosed {
		t.Fatal("failures from 2 minutes ago were counted with new ones")
	}
	clock.advance(30 * time.Second) // within the window of the last failure
	record(b, retryable)
	if b.State() != BreakerOpen {
		t.Errorf("state = %s, want open (3 failures, each within 1m of the previous)", b.State())
	}
}

// Review finding (Codex comparison): with a short cooldown, a send that
// started while the breaker was closed could finish during half-open and
// close the breaker before the real probe answered. Results and sends are
// tied to the breaker period (generation) they started in.
func TestBreaker_StaleResultCannotResolveTheProbe(t *testing.T) {
	b, clock, _ := newTestBreaker(1, 3*time.Second)
	_, _, oldGen := b.Permits() // a send starts while closed
	b.Record(retryable, "n-fail", oldGen)
	if b.Current(oldGen) {
		t.Error("jobs claimed before the breaker opened must not be sent")
	}

	clock.advance(3 * time.Second)
	_, _, probeGen := b.Permits() // half-open: the probe starts
	b.Record(ok, "n-old", oldGen) // the old send finally succeeds
	if b.State() != BreakerHalfOpen {
		t.Fatalf("a stale result changed the breaker to %s; only the probe may decide", b.State())
	}
	b.Record(ok, "n-probe", probeGen)
	if b.State() != BreakerClosed {
		t.Errorf("probe success: state = %s, want closed", b.State())
	}
}

// A probe permit is handed back only for its own breaker period: a job from
// an older period that ends without sending must not free the slot of the
// probe that is in flight now.
func TestBreaker_ReleaseProbeIgnoresOtherPeriods(t *testing.T) {
	b, clock, _ := newTestBreaker(1, time.Second)
	record(b, domain.OutcomeRetryableError) // opens
	clock.advance(2 * time.Second)
	_, _, oldGen := b.Permits() // half-open: this is the probe
	if p, _, _ := b.Permits(); p != 0 {
		t.Fatalf("second permit while probing = %d, want 0", p)
	}
	b.ReleaseProbe(oldGen - 1) // a stale job ending without a send
	if p, _, _ := b.Permits(); p != 0 {
		t.Error("a stale release freed the in-flight probe's slot")
	}
	b.ReleaseProbe(oldGen)
	if p, _, _ := b.Permits(); p != 1 {
		t.Error("the probe's own release did not allow a new probe")
	}
}

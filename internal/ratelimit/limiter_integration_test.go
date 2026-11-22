//go:build integration

package ratelimit_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"notification-service/internal/domain"
	"notification-service/internal/ratelimit"
	"notification-service/internal/testutil"
)

var rdb *testutil.Redis

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	if rdb, err = testutil.StartRedis(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "integration setup:", err)
		os.Exit(1)
	}
	code := m.Run()
	rdb.Close(ctx)
	os.Exit(code)
}

// newLimiter returns a limiter with its own key namespace so tests never
// share windows.
func newLimiter(perSecond int) (*ratelimit.Limiter, string) {
	prefix := "test-" + uuid.NewString() + ":"
	limits := map[domain.Channel]ratelimit.Limit{
		domain.ChannelSMS:   {PerSecond: perSecond},
		domain.ChannelEmail: {PerSecond: perSecond},
	}
	return ratelimit.New(rdb.Client, limits, prefix), prefix
}

// take grants n slots and finishes each send immediately.
func take(t *testing.T, l *ratelimit.Limiter, ch domain.Channel, n int) int {
	t.Helper()
	got := 0
	for range n {
		done, _, err := l.TryAcquire(context.Background(), ch)
		if err != nil {
			t.Fatal(err)
		}
		if done == nil {
			break
		}
		done()
		got++
	}
	return got
}

func TestTryAcquire_GrantsUpToTheLimitThenWaits(t *testing.T) {
	l, _ := newLimiter(5)
	if got := take(t, l, domain.ChannelSMS, 7); got != 5 {
		t.Fatalf("granted %d of 7, want the limit of 5", got)
	}
	// Full: nothing granted, and the wait says when to look again.
	done, wait, _ := l.TryAcquire(context.Background(), domain.ChannelSMS)
	if done != nil || wait <= 0 || wait > time.Second {
		t.Errorf("full window = granted %v wait %v; want none and (0, 1s]", done != nil, wait)
	}
}

// The brief: "maximum 100 messages per second per channel". 100 sends may
// start at once, but the 101st waits until the first finished a full second
// ago, so no 1-second window ever holds more than 100. Checked against the
// limiter itself, so network jitter cannot blur the boundary.
func TestTryAcquire_NeverMoreThanTheLimitInOneSecond(t *testing.T) {
	l, _ := newLimiter(100)
	start := time.Now()
	if got := take(t, l, domain.ChannelSMS, 150); got != 100 {
		t.Fatalf("granted %d, want the full 100", got)
	}
	end := time.Now() // every send finished between start and end
	time.Sleep(time.Until(start.Add(900 * time.Millisecond)))
	if got := take(t, l, domain.ChannelSMS, 1); got != 0 {
		t.Errorf("granted %d before the first sends were a second old", got)
	}
	time.Sleep(time.Until(end.Add(1050 * time.Millisecond)))
	if got := take(t, l, domain.ChannelSMS, 150); got != 100 {
		t.Errorf("after the window slid, granted %d, want 100 again", got)
	}
}

// A send still in flight keeps its slot, however long it takes: the
// provider may receive it at any moment until it answers. Its slot frees one
// second after it reports done.
func TestTryAcquire_InFlightSendsHoldTheirSlots(t *testing.T) {
	l, _ := newLimiter(2)
	ctx := context.Background()
	slow, _, _ := l.TryAcquire(ctx, domain.ChannelSMS) // never reported done yet
	if got := take(t, l, domain.ChannelSMS, 1); got != 1 {
		t.Fatalf("second slot = %d", got)
	}
	time.Sleep(1100 * time.Millisecond)
	// The fast send's slot has freed; the slow one is still in flight.
	if got := take(t, l, domain.ChannelSMS, 5); got != 1 {
		t.Errorf("after 1.1s granted %d, want 1 (the in-flight send holds the other)", got)
	}
	slow()
	time.Sleep(1050 * time.Millisecond)
	if got := take(t, l, domain.ChannelSMS, 5); got != 2 {
		t.Errorf("one second after the slow send finished, granted %d, want 2", got)
	}
}

// A worker that dies mid-send never reports done; its slot is reclaimed
// after the max send time.
func TestTryAcquire_AbandonedSendFreesAfterMaxSend(t *testing.T) {
	l, _ := newLimiter(1)
	l.WithMaxSend(200 * time.Millisecond)
	if done, _, _ := l.TryAcquire(context.Background(), domain.ChannelSMS); done == nil {
		t.Fatal("first acquire not granted")
	}
	time.Sleep(1300 * time.Millisecond) // max send 200ms + 1s window
	if got := take(t, l, domain.ChannelSMS, 1); got != 1 {
		t.Error("the abandoned slot was never reclaimed")
	}
}

func TestChannelsHaveIndependentWindows(t *testing.T) {
	l, _ := newLimiter(2)
	if got := take(t, l, domain.ChannelSMS, 2); got != 2 {
		t.Fatalf("sms fill = %d", got)
	}
	if got := take(t, l, domain.ChannelEmail, 2); got != 2 {
		t.Errorf("email granted %d after sms filled, want 2", got)
	}
}

func TestUnknownChannelIsAnError(t *testing.T) {
	l, _ := newLimiter(1)
	if _, _, err := l.TryAcquire(context.Background(), domain.ChannelPush); err == nil {
		t.Error("expected an error for a channel without a configured limit")
	}
}

func TestAcquire_RespectsContextWhileWaiting(t *testing.T) {
	l, _ := newLimiter(1) // the next slot frees 1s after this send finishes
	ctx := context.Background()
	done, err := l.Acquire(ctx, domain.ChannelSMS)
	if err != nil {
		t.Fatal(err)
	}
	done()
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := l.Acquire(ctx, domain.ChannelSMS); err == nil {
		t.Fatal("expected context deadline error")
	}
	if time.Since(start) > time.Second {
		t.Error("Acquire ignored context cancellation")
	}
}

// Several limiter instances model several worker replicas: they share one
// window, so their combined throughput is capped, not each one separately.
// Over 2 seconds that is at most 2 x 100, and with instant sends nothing is
// wasted: the full 200 are used.
func TestAcquire_CapsCombinedThroughputAcrossReplicas(t *testing.T) {
	const (
		rate    = 100
		window  = 2 * time.Second
		clients = 4 // independent Limiter instances = replicas
	)
	_, prefix := newLimiter(rate)
	limits := map[domain.Channel]ratelimit.Limit{domain.ChannelSMS: {PerSecond: rate}}

	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	var total, redisCalls atomic.Int64
	var wg sync.WaitGroup
	for range clients {
		replica := ratelimit.New(rdb.Client, limits, prefix)
		for range 5 { // goroutines per replica
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					done, err := replica.Acquire(ctx, domain.ChannelSMS)
					redisCalls.Add(1)
					if err != nil {
						return
					}
					done() // an instant send
					total.Add(1)
				}
			}()
		}
	}
	wg.Wait()

	want := float64(rate) * window.Seconds() // 200
	got := float64(total.Load())
	// A call already inside Redis when the deadline passes can still be
	// granted, hence the small allowance above 200.
	if got < want*0.95 || got > want+5 {
		t.Errorf("combined grants over %v = %.0f, want ~%.0f (%d/s)", window, got, want, rate)
	}
	// No busy spinning: waiters sleep until a slot may have freed, so
	// acquire calls stay within a small multiple of the slots granted.
	if calls := redisCalls.Load(); float64(calls) > got*4 {
		t.Errorf("%d Redis calls for %.0f tokens: callers are spinning", calls, got)
	}
	t.Logf("granted %.0f tokens in %v (expected ~%.0f), %d acquire calls", got, window, want, redisCalls.Load())
}

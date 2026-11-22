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

func TestTryAcquire_GrantsUpToTheLimitThenWaits(t *testing.T) {
	l, _ := newLimiter(5)
	ctx := context.Background()

	granted, _, err := l.TryAcquire(ctx, domain.ChannelSMS, 3)
	if err != nil || granted != 3 {
		t.Fatalf("first acquire = %d, %v; want 3", granted, err)
	}
	// Only 2 left in this second: a request for 5 is partially granted.
	if granted, _, _ := l.TryAcquire(ctx, domain.ChannelSMS, 5); granted != 2 {
		t.Errorf("partial acquire = %d, want 2", granted)
	}
	// Full: nothing granted, and the wait says when the oldest grant leaves
	// the 1-second window.
	granted, wait, _ := l.TryAcquire(ctx, domain.ChannelSMS, 1)
	if granted != 0 || wait <= 0 || wait > time.Second {
		t.Errorf("full window = granted %d wait %v; want 0 and (0, 1s]", granted, wait)
	}
}

// The brief: "maximum 100 messages per second per channel". All 100 may go
// at once, but the 101st waits until the first is a full second old, so no
// 1-second window ever holds more than 100. Checked against the limiter
// itself, so network jitter cannot blur the boundary.
func TestTryAcquire_NeverMoreThanTheLimitInOneSecond(t *testing.T) {
	l, _ := newLimiter(100)
	ctx := context.Background()

	if g, _, _ := l.TryAcquire(ctx, domain.ChannelSMS, 150); g != 100 {
		t.Fatalf("first acquire granted %d, want the full 100", g)
	}
	g, wait, _ := l.TryAcquire(ctx, domain.ChannelSMS, 1)
	if g != 0 || wait < 900*time.Millisecond || wait > time.Second {
		t.Fatalf("101st = granted %d wait %v; want 0 and a wait of almost 1s", g, wait)
	}
	time.Sleep(wait - 100*time.Millisecond)
	if g, _, _ := l.TryAcquire(ctx, domain.ChannelSMS, 1); g != 0 {
		t.Errorf("granted %d before the first grants were a second old", g)
	}
	time.Sleep(150 * time.Millisecond)
	if g, _, _ := l.TryAcquire(ctx, domain.ChannelSMS, 150); g != 100 {
		t.Errorf("after the window slid, granted %d, want 100 again", g)
	}
}

// Unlike a token bucket, unused capacity is never lost: whatever left the
// window can be used, however late the caller comes back.
func TestTryAcquire_SlotsFreeUpAsTheWindowSlides(t *testing.T) {
	l, _ := newLimiter(2)
	ctx := context.Background()
	if g, _, _ := l.TryAcquire(ctx, domain.ChannelSMS, 2); g != 2 {
		t.Fatalf("fill = %d", g)
	}
	time.Sleep(1050 * time.Millisecond)
	if g, _, _ := l.TryAcquire(ctx, domain.ChannelSMS, 5); g != 2 {
		t.Errorf("after the window slid granted %d, want 2", g)
	}
}

// A refund frees slots for sends that never happened, never more than the
// limit.
func TestRefund_FreesUnusedSlots(t *testing.T) {
	l, _ := newLimiter(5)
	ctx := context.Background()
	if g, _, _ := l.TryAcquire(ctx, domain.ChannelSMS, 5); g != 5 {
		t.Fatalf("fill = %d", g)
	}
	if err := l.Refund(ctx, domain.ChannelSMS, 3); err != nil {
		t.Fatal(err)
	}
	if g, _, _ := l.TryAcquire(ctx, domain.ChannelSMS, 5); g != 3 {
		t.Errorf("after refund of 3 granted %d, want 3", g)
	}
	if err := l.Refund(ctx, domain.ChannelSMS, 100); err != nil {
		t.Fatal(err)
	}
	if g, _, _ := l.TryAcquire(ctx, domain.ChannelSMS, 100); g != 5 {
		t.Errorf("refund above the limit granted %d, want 5 (capped)", g)
	}
}

func TestChannelsHaveIndependentBuckets(t *testing.T) {
	l, _ := newLimiter(2)
	ctx := context.Background()
	if g, _, _ := l.TryAcquire(ctx, domain.ChannelSMS, 2); g != 2 {
		t.Fatalf("sms drain = %d", g)
	}
	if g, _, _ := l.TryAcquire(ctx, domain.ChannelEmail, 2); g != 2 {
		t.Errorf("email granted %d after sms drained, want 2", g)
	}
}

func TestUnknownChannelIsAnError(t *testing.T) {
	l, _ := newLimiter(1)
	if _, _, err := l.TryAcquire(context.Background(), domain.ChannelPush, 1); err == nil {
		t.Error("expected an error for a channel without a configured limit")
	}
}

func TestAcquire_RespectsContextWhileWaiting(t *testing.T) {
	l, _ := newLimiter(1) // the next slot frees in 1s
	ctx := context.Background()
	if _, err := l.Acquire(ctx, domain.ChannelSMS, 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := l.Acquire(ctx, domain.ChannelSMS, 1); err == nil {
		t.Fatal("expected context deadline error")
	}
	if time.Since(start) > time.Second {
		t.Error("Acquire ignored context cancellation")
	}
}

// Several limiter instances model several worker replicas: they share one
// window, so their combined throughput is capped, not each one separately.
// Over 2 seconds that is at most 2 x 100, and nothing is wasted: the full
// 200 are used.
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
					n, err := replica.Acquire(ctx, domain.ChannelSMS, 1)
					redisCalls.Add(1)
					if err != nil {
						return
					}
					total.Add(int64(n))
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
	// No busy spinning: waiters sleep until the next token, so Redis calls
	// stay within a small multiple of the tokens granted.
	if calls := redisCalls.Load(); float64(calls) > got*4 {
		t.Errorf("%d Redis calls for %.0f tokens: callers are spinning", calls, got)
	}
	t.Logf("granted %.0f tokens in %v (expected ~%.0f), %d acquire calls", got, window, want, redisCalls.Load())
}

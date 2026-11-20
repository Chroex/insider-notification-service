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
// share buckets.
func newLimiter(perSecond float64, burst int) (*ratelimit.Limiter, string) {
	prefix := "test-" + uuid.NewString() + ":"
	limits := map[domain.Channel]ratelimit.Limit{
		domain.ChannelSMS:   {PerSecond: perSecond, Burst: burst},
		domain.ChannelEmail: {PerSecond: perSecond, Burst: burst},
	}
	return ratelimit.New(rdb.Client, limits, prefix), prefix
}

func TestTryAcquire_BurstThenWait(t *testing.T) {
	l, _ := newLimiter(10, 5) // 10/s -> one token every 100ms
	ctx := context.Background()

	granted, _, err := l.TryAcquire(ctx, domain.ChannelSMS, 3)
	if err != nil || granted != 3 {
		t.Fatalf("first acquire = %d, %v; want 3 from a full bucket", granted, err)
	}
	// Only 2 left: a request for 5 is partially granted.
	if granted, _, _ := l.TryAcquire(ctx, domain.ChannelSMS, 5); granted != 2 {
		t.Errorf("partial acquire = %d, want 2", granted)
	}
	// Empty: nothing granted, and the wait says when the next token exists.
	granted, wait, _ := l.TryAcquire(ctx, domain.ChannelSMS, 1)
	if granted != 0 || wait <= 0 || wait > 100*time.Millisecond {
		t.Errorf("empty bucket = granted %d wait %v; want 0 and (0, 100ms]", granted, wait)
	}
}

func TestTryAcquire_RefillsOverTime(t *testing.T) {
	l, _ := newLimiter(20, 2) // a token every 50ms
	ctx := context.Background()
	if g, _, _ := l.TryAcquire(ctx, domain.ChannelSMS, 2); g != 2 {
		t.Fatalf("drain = %d", g)
	}
	time.Sleep(120 * time.Millisecond) // ~2.4 tokens, capped at burst 2
	if g, _, _ := l.TryAcquire(ctx, domain.ChannelSMS, 5); g != 2 {
		t.Errorf("after refill granted %d, want 2 (capped at burst)", g)
	}
}

func TestRefund_ReturnsTokensUpToBurst(t *testing.T) {
	l, _ := newLimiter(1, 5)
	ctx := context.Background()
	if g, _, _ := l.TryAcquire(ctx, domain.ChannelSMS, 5); g != 5 {
		t.Fatalf("drain = %d", g)
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
		t.Errorf("refund above burst granted %d, want 5 (capped)", g)
	}
}

func TestChannelsHaveIndependentBuckets(t *testing.T) {
	l, _ := newLimiter(1, 2)
	ctx := context.Background()
	if g, _, _ := l.TryAcquire(ctx, domain.ChannelSMS, 2); g != 2 {
		t.Fatalf("sms drain = %d", g)
	}
	if g, _, _ := l.TryAcquire(ctx, domain.ChannelEmail, 2); g != 2 {
		t.Errorf("email granted %d after sms drained, want 2", g)
	}
}

func TestUnknownChannelIsAnError(t *testing.T) {
	l, _ := newLimiter(1, 1)
	if _, _, err := l.TryAcquire(context.Background(), domain.ChannelPush, 1); err == nil {
		t.Error("expected an error for a channel without a configured limit")
	}
}

func TestAcquire_RespectsContextWhileWaiting(t *testing.T) {
	l, _ := newLimiter(0.5, 1) // next token in 2s
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
// bucket, so their combined throughput is capped, not each one separately.
// Over a window W the expected total is burst + rate*W.
func TestAcquire_CapsCombinedThroughputAcrossReplicas(t *testing.T) {
	const (
		rate    = 100.0
		burst   = 10
		window  = 2 * time.Second
		clients = 4 // independent Limiter instances = replicas
	)
	_, prefix := newLimiter(rate, burst)
	limits := map[domain.Channel]ratelimit.Limit{domain.ChannelSMS: {PerSecond: rate, Burst: burst}}

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

	want := float64(burst) + rate*window.Seconds() // 210
	got := float64(total.Load())
	if got < want*0.9 || got > want*1.05 {
		t.Errorf("combined grants over %v = %.0f, want ~%.0f (rate %v/s + burst %d)", window, got, want, rate, burst)
	}
	// No busy spinning: waiters sleep until the next token, so Redis calls
	// stay within a small multiple of the tokens granted.
	if calls := redisCalls.Load(); float64(calls) > got*4 {
		t.Errorf("%d Redis calls for %.0f tokens: callers are spinning", calls, got)
	}
	t.Logf("granted %.0f tokens in %v (expected ~%.0f), %d acquire calls", got, window, want, redisCalls.Load())
}

//go:build integration

package worker_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"notification-service/internal/domain"
	"notification-service/internal/provider"
	"notification-service/internal/queue"
	"notification-service/internal/ratelimit"
	"notification-service/internal/worker"
)

func permitLimits(perSecond int) map[domain.Channel]ratelimit.Limit {
	limits := map[domain.Channel]ratelimit.Limit{}
	for _, ch := range domain.Channels {
		limits[ch] = ratelimit.Limit{PerSecond: perSecond}
	}
	return limits
}

// Found by the failure-hunter agent (round 2). ADR-024: "A job already
// claimed when the breaker trips is released before sending, with its
// attempt refunded". The breaker check ran before the rate-limit wait, so a
// job waiting for a permit while the breaker tripped sent anyway.
func TestPermit_BreakerOpeningDuringTheWaitStopsTheSend(t *testing.T) {
	db.Reset(t)
	fp := newFakeProvider(t, always(http.StatusServiceUnavailable))
	const total = 4
	for range total {
		insertNotification(t, domain.ChannelSMS, withMaxAttempts(10))
	}
	opened := make(chan struct{}, 10)
	cfg := testConfig()
	cfg.Concurrency, cfg.ClaimBatchSize = total, total // all 4 claimed at once
	cfg.BreakerThreshold, cfg.BreakerCooldown = 3, 30*time.Second
	cfg.Observer = breakerWatcher{opened: opened}
	// 3 sends per second: three go out at once, the fourth waits ~1s.
	limiter := ratelimit.New(rdb.Client, permitLimits(3), "test-"+uuid.NewString()+":").WithMaxSend(3 * time.Second)
	w, _ := startWorkerHandle(t, fp.URL, cfg, limiter)

	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("breaker never opened")
	}
	callsAtOpen := fp.totalCalls()
	time.Sleep(2 * time.Second) // the fourth job's permit arrives meanwhile
	if s := w.BreakerState(domain.ChannelSMS); s != worker.BreakerOpen {
		t.Fatalf("breaker state = %s, want open", s)
	}
	if c := fp.totalCalls(); c != callsAtOpen {
		t.Errorf("provider got %d call(s) while the breaker was open (calls at open %d, now %d)",
			c-callsAtOpen, callsAtOpen, c)
	}
	var untouched int
	if err := db.Pool.QueryRow(context.Background(),
		"SELECT count(*) FROM notifications WHERE attempts = 0 AND status = 'pending'").Scan(&untouched); err != nil {
		t.Fatal(err)
	}
	if untouched != total-3 {
		t.Errorf("%d notification(s) released with attempt refunded, want %d", untouched, total-3)
	}
}

// Found by the failure-hunter agent (round 2). ADR-017 said a claimed row
// waits for a permit "for about a second at the full rate, far inside the
// lease". The wait was unbounded and ownership was checked before it, not
// after. With more
// waiters (replicas x concurrency) than the rate can serve within the
// lease, the reaper re-queues a waiting row, another claim sends it, and
// the stale owner sends it too once its permit comes.
//
// Scaled: 2/s, 2 replicas x 10, lease 4s (>= 2 x the 2s provider timeout).
// Same shape as 100/s with replicas x concurrency > 100 x (lease - timeout),
// or any channel with a low RATE_LIMIT_<CH>_PER_SECOND (e.g. 1/s with 3
// replicas at the defaults: 30 waiters, 30s > 25s).
func TestPermit_LongWaitDoesNotLetTwoWorkersSendOneRow(t *testing.T) {
	db.Reset(t)
	fp := newFakeProvider(t, always(http.StatusAccepted))
	const total = 20
	for range total {
		insertNotification(t, domain.ChannelSMS)
	}
	cfg := testConfig()
	cfg.Concurrency, cfg.ClaimBatchSize = 10, 10
	cfg.LeaseDuration = 4 * time.Second
	cfg.ReaperInterval = 200 * time.Millisecond
	prefix := "test-" + uuid.NewString() + ":"
	var stops []func()
	for range 2 {
		l := ratelimit.New(rdb.Client, permitLimits(2), prefix).WithMaxSend(3 * time.Second)
		stops = append(stops, startWorkerWithLimiter(t, fp.URL, cfg, l))
	}
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		var sent int
		_ = db.Pool.QueryRow(context.Background(),
			"SELECT count(*) FROM notifications WHERE status = 'sent'").Scan(&sent)
		if sent == total {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(2 * time.Second) // let stale owners finish their sends
	for _, s := range stops {
		s()
	}
	fp.mu.Lock()
	dups := 0
	for id, n := range fp.calls {
		if n > 1 {
			dups++
			t.Logf("notification %s delivered %d times", id, n)
		}
	}
	fp.mu.Unlock()
	if dups > 0 {
		t.Errorf("%d of %d notifications reached the provider more than once without any crash", dups, total)
	}
}

// Written by the failure-hunter agent (round 2; the guarantee held). with the limiter failing (Redis down), jobs must not
// burn attempts, the loop must not spin, and once Redis is back all send.
type toggleLimiter struct {
	down  atomic.Bool
	calls atomic.Int64
	inner worker.RateLimiter
}

func (l *toggleLimiter) Acquire(ctx context.Context, ch domain.Channel) (func(), error) {
	l.calls.Add(1)
	if l.down.Load() {
		return nil, errors.New("dial tcp: connection refused")
	}
	return l.inner.Acquire(ctx, ch)
}

func TestPermit_RedisOutageDoesNotBurnAttemptsOrSpin(t *testing.T) {
	db.Reset(t)
	fp := newFakeProvider(t, always(http.StatusAccepted))
	const total = 5
	var ids []uuid.UUID
	for range total {
		ids = append(ids, insertNotification(t, domain.ChannelSMS, withMaxAttempts(2)).ID)
	}
	cfg := testConfig()
	cfg.Concurrency, cfg.ClaimBatchSize = 2, 2
	cfg.PollInterval = 50 * time.Millisecond
	l := &toggleLimiter{inner: worker.Unlimited{}}
	l.down.Store(true)
	startWorkerWithLimiter(t, fp.URL, cfg, l)
	time.Sleep(1500 * time.Millisecond)
	calls := l.calls.Load()
	if fp.totalCalls() != 0 {
		t.Fatalf("provider called %d times with the limiter down", fp.totalCalls())
	}
	// 2 slots, each paused one poll interval per attempt: ~60 over 1.5s.
	if calls > 120 {
		t.Errorf("%d limiter calls in 1.5s: claim-release loop spins", calls)
	}
	l.down.Store(false)
	for _, id := range ids {
		n := waitForStatus(t, id, domain.StatusSent, 5*time.Second)
		if n.Attempts != 1 {
			t.Errorf("%s: attempts = %d after outage, want 1 (outage refunded)", id, n.Attempts)
		}
	}
	t.Logf("limiter calls during outage: %d", calls)
}

// Found by the failure-hunter agent (round 2). A provider that stalls
// (accepts connections but reads nothing) and recovers 3s later processes
// every request queued in its accept backlog at once, even those we already
// gave up on: no client-side limiter can know when a request it timed out
// on will be read. Two defenses keep that burst within the limit:
//   - a request without an answer keeps its slot for the full max send
//     time instead of freeing one second after our timeout;
//   - the circuit breaker (production default: 5 distinct failures) stops
//     sending after the first timeouts, so the backlog the provider can
//     build is what was in flight then, at most 100.
//
// Faithful to production wiring: maxSend = PROVIDER_TIMEOUT + 1s and the
// default breaker settings.
func TestPermit_StalledProviderSeesAtMost100InASecond(t *testing.T) {
	db.Reset(t)
	for range 1500 {
		insertNotification(t, domain.ChannelSMS, withMaxAttempts(50))
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var arrivals []time.Time
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		arrivals = append(arrivals, time.Now())
		mu.Unlock()
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"m","status":"accepted"}`))
	})}
	// The provider stalls for 3s (accepts nothing: connections queue in the
	// kernel backlog), then recovers.
	go func() {
		time.Sleep(3 * time.Second)
		_ = srv.Serve(ln)
	}()
	t.Cleanup(func() { _ = srv.Close() })

	const timeout = 300 * time.Millisecond
	cfg := testConfig()
	cfg.Concurrency, cfg.ClaimBatchSize = 200, 100
	cfg.PollInterval = 5 * time.Millisecond
	cfg.BreakerThreshold, cfg.BreakerWindow, cfg.BreakerCooldown = 5, time.Minute, 30*time.Second
	limiter := ratelimit.New(rdb.Client, permitLimits(100), "test-"+uuid.NewString()+":").WithMaxSend(timeout + time.Second)
	w := worker.New(queue.New(db.Pool), provider.NewClient("http://"+ln.Addr().String(), timeout, http.StatusAccepted),
		limiter, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	time.Sleep(6 * time.Second)
	cancel()
	<-done

	ap := &arrivalProvider{times: arrivals}
	mu.Lock()
	ap.times = append([]time.Time(nil), arrivals...)
	mu.Unlock()
	best, n := ap.busiest()
	t.Logf("arrivals %d, busiest 1s window %d", n, best)
	if best > 100 {
		t.Errorf("provider saw %d requests in one second (limit 100)", best)
	}
}

// Written by the failure-hunter agent (round 2; the guarantee held). the provider fails with 503 (breaker opens), then comes
// back misconfigured answering 200. The half-open probe's 200 must reopen
// the breaker, so only one notification per cooldown is failed, not the
// backlog drained.
func TestPermit_ContractViolationProbeReopensBreaker(t *testing.T) {
	db.Reset(t)
	var mode atomic.Int32 // 0: 503, 1: 200
	fp := newFakeProvider(t, func(string, int) int {
		if mode.Load() == 0 {
			return http.StatusServiceUnavailable
		}
		return http.StatusOK
	})
	const total = 20
	for range total {
		insertNotification(t, domain.ChannelSMS, withMaxAttempts(10))
	}
	opened := make(chan struct{}, 50)
	cfg := testConfig()
	cfg.Concurrency, cfg.ClaimBatchSize = 4, 4
	cfg.BreakerThreshold, cfg.BreakerCooldown = 3, 400*time.Millisecond
	cfg.Observer = breakerWatcher{opened: opened}
	w, _ := startWorkerHandle(t, fp.URL, cfg, worker.Unlimited{})
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("breaker never opened")
	}
	mode.Store(1)
	time.Sleep(1300 * time.Millisecond) // ~3 cooldowns: at most ~3 probes
	var failed int
	_ = db.Pool.QueryRow(context.Background(),
		"SELECT count(*) FROM notifications WHERE status = 'failed'").Scan(&failed)
	t.Logf("failed %d, calls %d, state %s", failed, fp.totalCalls(), w.BreakerState(domain.ChannelSMS))
	if failed > 4 {
		t.Errorf("%d notifications failed by 200s while the breaker cycled; want one probe per cooldown", failed)
	}
}

// A request that got no answer (timeout, connection error) may still reach
// the provider later, so its slot is held for the whole max send time, not
// released one second after we gave up on it.
func TestPermit_UnansweredRequestKeepsItsSlotForTheMaxSendTime(t *testing.T) {
	db.Reset(t)
	var mu sync.Mutex
	var arrivals []time.Time
	hang := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		arrivals = append(arrivals, time.Now())
		mu.Unlock()
		_, _ = io.Copy(io.Discard, r.Body)
		select { // never answers within the client's timeout
		case <-hang:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)              // runs second
	t.Cleanup(func() { close(hang) }) // runs first: free the handlers
	for range 2 {
		insertNotification(t, domain.ChannelSMS)
	}

	const timeout = 200 * time.Millisecond
	maxSend := timeout + time.Second
	cfg := testConfig()
	cfg.Concurrency, cfg.ClaimBatchSize = 2, 2
	limiter := ratelimit.New(rdb.Client, permitLimits(1), "test-"+uuid.NewString()+":").WithMaxSend(maxSend)
	w := worker.New(queue.New(db.Pool), provider.NewClient(srv.URL, timeout, http.StatusAccepted),
		limiter, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	time.Sleep(maxSend + 2*time.Second)
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(arrivals) < 2 {
		t.Fatalf("only %d request(s) reached the provider", len(arrivals))
	}
	// Freed one second after the timeout, the gap would be ~1.2s; held for
	// the max send time it is ~2.2s.
	if gap := arrivals[1].Sub(arrivals[0]); gap < maxSend+900*time.Millisecond {
		t.Errorf("second request %v after the first; the unanswered first should hold the slot ~%v",
			gap, maxSend+time.Second)
	}
}

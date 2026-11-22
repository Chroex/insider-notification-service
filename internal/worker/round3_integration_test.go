//go:build integration

package worker_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
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

// --- from the failure-hunter agent, round 3 (ratelimit) ---
// slowRenewAfterQueue makes every Renew that starts before `until` take
// `delay` (a database hiccup shorter than persistTimeout, 5s, so the Renew
// still succeeds).
type slowRenewAfterQueue struct {
	*queue.Queue
	until time.Time
	delay time.Duration
}

func (q slowRenewAfterQueue) Renew(ctx context.Context, id uuid.UUID, attempt int, lease time.Duration) (bool, error) {
	if time.Now().Before(q.until) {
		time.Sleep(q.delay)
	}
	return q.Queue.Renew(ctx, id, attempt, lease)
}

// Found by the failure-hunter agent (round 3). The permit is taken before
// the ownership check (Renew) and the "processing" publish. A slot was held
// for PROVIDER_TIMEOUT + 1s from its grant, which did not cover that time:
// with a slow Renew the entry left the window before the request went out,
// so the send was not counted and a fresh batch was granted alongside it.
// The hold now covers Renew + publish + the call (worker.SlotHold).
//
// PROVIDER_TIMEOUT 500ms, Renew 3s (under the 5s persist timeout), limit 5/s
// scaled down from 100/s.
func TestPermit_SlowOwnershipCheckAfterThePermitStaysCounted(t *testing.T) {
	db.Reset(t)
	ap := newArrivalProvider(t)
	for range 60 {
		insertNotification(t, domain.ChannelSMS)
	}
	const perSecond = 5
	const timeout = 500 * time.Millisecond
	cfg := testConfig()
	cfg.Concurrency, cfg.ClaimBatchSize = 30, 30
	cfg.PollInterval = 5 * time.Millisecond
	limiter := ratelimit.New(rdb.Client, permitLimits(perSecond), "test-"+uuid.NewString()+":").
		WithMaxSend(worker.SlotHold(timeout)) // as cmd/worker wires it
	q := slowRenewAfterQueue{Queue: queue.New(db.Pool), until: time.Now().Add(300 * time.Millisecond), delay: 3 * time.Second}
	w := worker.New(q, provider.NewClient(ap.URL, timeout, http.StatusAccepted), limiter, cfg,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	time.Sleep(5 * time.Second)
	cancel()
	<-done

	best, total := ap.busiest()
	t.Logf("arrivals %d, busiest 1s window %d (limit %d)", total, best, perSecond)
	if best > perSecond {
		t.Errorf("provider saw %d requests in one second (limit %d)", best, perSecond)
	}
}

// --- from the failure-hunter agent, round 3 (breaker) ---
// slowRenewForID makes the ownership check of one notification slow (a
// row lock or a slow database round trip on that one statement).
type slowRenewForID struct {
	*queue.Queue
	id    uuid.UUID
	delay time.Duration
}

func (q slowRenewForID) Renew(ctx context.Context, id uuid.UUID, attempt int, lease time.Duration) (bool, error) {
	if id == q.id {
		time.Sleep(q.delay)
	}
	return q.Queue.Renew(ctx, id, attempt, lease)
}

// Found by the failure-hunter agent (round 3): ADR-024 says "A job already claimed when the breaker
// trips is released before sending, with its attempt refunded". process
// re-checks the breaker, THEN calls Renew (a database round trip, up to 5s)
// and publishes an event (up to 500ms), and only then sends without looking
// at the breaker again. A breaker that trips during the Renew does not stop
// the send.
func TestPermit_BreakerOpeningDuringTheOwnershipCheckStopsTheSend(t *testing.T) {
	db.Reset(t)
	fp := newFakeProvider(t, always(http.StatusServiceUnavailable))
	fp.delay = 100 * time.Millisecond
	var last domain.Notification
	for range 4 {
		last = insertNotification(t, domain.ChannelSMS, withMaxAttempts(10))
	}
	opened := make(chan struct{}, 10)
	cfg := testConfig()
	cfg.Concurrency, cfg.ClaimBatchSize = 4, 4
	cfg.BreakerThreshold, cfg.BreakerCooldown = 3, 30*time.Second
	cfg.Observer = breakerWatcher{opened: opened}
	q := slowRenewForID{Queue: queue.New(db.Pool), id: last.ID, delay: time.Second}
	w, _ := startWorkerOn(t, q, fp.URL, cfg, worker.Unlimited{})

	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("breaker never opened")
	}
	callsAtOpen := fp.totalCalls()
	time.Sleep(1500 * time.Millisecond)
	if s := w.BreakerState(domain.ChannelSMS); s != worker.BreakerOpen {
		t.Fatalf("breaker state = %s, want open", s)
	}
	if c := fp.totalCalls(); c != callsAtOpen {
		t.Errorf("provider got %d call(s) after the breaker opened (calls at open %d, now %d); slow-renew row attempts=%d",
			c-callsAtOpen, callsAtOpen, c, get(t, last.ID).Attempts)
	}
}

// --- from the failure-hunter agent, round 3 (claimcancel) ---
// Found by the failure-hunter agent (round 3): ADR-020 says a graceful shutdown hands back every
// job whose send had not started, attempt refunded. The claim loop calls
// Claim with the run context; SIGTERM cancels it. If the cancel lands after
// Postgres committed the UPDATE ... RETURNING but before pgx read the rows,
// Claim returns an error and the claimed rows are dropped on the floor:
// they stay 'processing' until the lease expires, then the reaper records
// an 'interrupted' attempt ("the provider may have received it") and keeps
// the attempt, although no call was ever made.
func TestQueue_CancelledClaimNeverStrandsCommittedRows(t *testing.T) {
	q := queue.New(db.Pool)
	stranded, errs := 0, 0
	for i := range 300 {
		db.Reset(t)
		for range 30 {
			insertNotification(t, domain.ChannelSMS)
		}
		// Cancel somewhere inside the claim round trip.
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(200+i*10)*time.Microsecond)
		jobs, err := q.Claim(ctx, domain.ChannelSMS, queue.Strict(30), time.Minute)
		cancel()
		if err == nil {
			continue
		}
		errs++
		var processing int
		if err := db.Pool.QueryRow(context.Background(),
			"SELECT count(*) FROM notifications WHERE status = 'processing'").Scan(&processing); err != nil {
			t.Fatal(err)
		}
		if processing > len(jobs) {
			stranded++
			if stranded == 1 {
				t.Logf("iteration %d: Claim returned %v with %d job(s), but %d row(s) are 'processing' (no owner)",
					i, err, len(jobs), processing)
			}
		}
	}
	t.Logf("claims that errored: %d, claims that stranded rows: %d", errs, stranded)
	if stranded > 0 {
		t.Errorf("%d cancelled claim(s) committed rows the caller never received", stranded)
	}
}

// --- from the failure-hunter agent, round 3 (complete) ---
// flakyCompleteQueue fails the first Complete with a transient error
// (one dropped connection), as a database failover or a pool reset would.
type flakyCompleteQueue struct {
	*queue.Queue
	calls *atomic.Int64
}

func (q flakyCompleteQueue) Complete(ctx context.Context, c queue.Completion) error {
	if q.calls.Add(1) == 1 {
		return errors.New("complete: commit: read tcp: connection reset by peer")
	}
	return q.Queue.Complete(ctx, c)
}

// Found by the failure-hunter agent (round 3): ADR-018 says "only crashes between the provider's
// answer and our commit (milliseconds) produce duplicates". process does not
// retry Complete: one transient database error after a 202 leaves the row
// 'processing', the reaper re-queues it when the lease runs out, and the
// message is sent a second time, with no crash anywhere.
func TestDelivery_TransientCompleteErrorDoesNotSendTwice(t *testing.T) {
	db.Reset(t)
	fp := newFakeProvider(t, always(http.StatusAccepted))
	n := insertNotification(t, domain.ChannelSMS)
	cfg := testConfig()
	cfg.LeaseDuration = 4 * time.Second // >= 2 x the 2s provider timeout
	cfg.ReaperInterval = 200 * time.Millisecond
	q := flakyCompleteQueue{Queue: queue.New(db.Pool), calls: new(atomic.Int64)}
	startWorkerOn(t, q, fp.URL, cfg, worker.Unlimited{})

	got := waitForStatus(t, n.ID, domain.StatusSent, 15*time.Second)
	if calls := fp.callsFor(n.ID); calls != 1 {
		t.Errorf("provider received the notification %d times (one 202, one transient DB error, no crash); attempts=%d history=%s",
			calls, got.Attempts, outcomes(attempts(t, n.ID)))
	}
}

// --- from the failure-hunter agent, round 3 (probe) ---
// huntBlockingLimiter blocks every Acquire until ctx ends while block is set.
type huntBlockingLimiter struct {
	block    atomic.Bool
	timeouts atomic.Int64
}

func (l *huntBlockingLimiter) Acquire(ctx context.Context, _ domain.Channel) (func(), error) {
	for l.block.Load() {
		select {
		case <-ctx.Done():
			l.timeouts.Add(1)
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	return func() {}, nil
}

// Written by the failure-hunter agent (round 3; the guarantee held): a half-open probe that gives up waiting for a
// rate-limit permit (half the lease) is released; its probe permit must come
// back through the deferred ReleaseProbe so the channel probes again and
// recovers once permits flow, with every released attempt refunded.
func TestPermit_ProbeReleasedAfterTheWaitDoesNotWedgeTheChannel(t *testing.T) {
	db.Reset(t)
	var down atomic.Bool
	down.Store(true)
	fp := newFakeProvider(t, func(string, int) int {
		if down.Load() {
			return http.StatusServiceUnavailable
		}
		return http.StatusAccepted
	})
	const total = 6
	for range total {
		insertNotification(t, domain.ChannelSMS, withMaxAttempts(10))
	}
	opened := make(chan struct{}, 10)
	cfg := testConfig()
	cfg.Concurrency, cfg.ClaimBatchSize = 3, 3
	cfg.LeaseDuration = 600 * time.Millisecond // permit wait bound 300ms
	cfg.BreakerThreshold, cfg.BreakerCooldown = 3, 200*time.Millisecond
	cfg.Observer = breakerWatcher{opened: opened}
	l := &huntBlockingLimiter{}
	w, _ := startWorkerOn(t, queue.New(db.Pool), fp.URL, cfg, l)
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("breaker never opened")
	}
	l.block.Store(true)
	down.Store(false)
	time.Sleep(1500 * time.Millisecond) // several probes time out on the permit
	l.block.Store(false)
	deadline := time.Now().Add(5 * time.Second)
	for {
		var sent int
		_ = db.Pool.QueryRow(context.Background(), "SELECT count(*) FROM notifications WHERE status = 'sent'").Scan(&sent)
		if sent == total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d/%d sent; breaker %s, permit timeouts %d", sent, total, w.BreakerState(domain.ChannelSMS), l.timeouts.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if l.timeouts.Load() < 2 {
		t.Fatalf("only %d permit timeouts: the scenario did not happen", l.timeouts.Load())
	}
	t.Logf("permit timeouts while half-open: %d", l.timeouts.Load())
	_ = worker.BreakerClosed
}

// --- from the failure-hunter agent, round 3 (release) ---
// Written by the failure-hunter agent (round 3; the guarantee held): a stale worker that releases a row after it
// was reaped and re-claimed (or reaped, re-claimed and released again) must
// neither refund the newer claim's attempt nor hand the newer claim's row
// back to the queue.
func TestQueue_StaleReleaseAfterReapAndReclaimIsANoOp(t *testing.T) {
	db.Reset(t)
	ctx := context.Background()
	q := queue.New(db.Pool)
	insertNotification(t, domain.ChannelSMS)

	stale, err := q.Claim(ctx, domain.ChannelSMS, queue.Strict(1), time.Millisecond)
	if err != nil || len(stale) != 1 {
		t.Fatalf("claim: %v %d", err, len(stale))
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := q.ReapExpired(ctx); err != nil {
		t.Fatal(err)
	}
	b, err := q.Claim(ctx, domain.ChannelSMS, queue.Strict(1), time.Minute)
	if err != nil || len(b) != 1 {
		t.Fatalf("reclaim: %v %d", err, len(b))
	}
	// B gives up waiting for a permit: refund to the stale worker's number.
	if n, err := q.Release(ctx, b); err != nil || n != 1 {
		t.Fatalf("release b: %v %d", err, n)
	}
	c, err := q.Claim(ctx, domain.ChannelSMS, queue.Strict(1), time.Minute)
	if err != nil || len(c) != 1 {
		t.Fatalf("claim c: %v %d", err, len(c))
	}
	if n, err := q.Release(ctx, stale); err != nil || n != 0 {
		t.Errorf("stale release affected %d row(s) (err %v)", n, err)
	}
	if owned, err := q.Renew(ctx, stale[0].ID, stale[0].Attempts, time.Minute); err != nil || owned {
		t.Errorf("stale worker still owns the row: owned=%v err=%v (stale attempt %d, current %d)",
			owned, err, stale[0].Attempts, c[0].Attempts)
	}
	got := get(t, c[0].ID)
	if got.Status != domain.StatusProcessing || got.Attempts != c[0].Attempts {
		t.Errorf("row %s/attempts %d, want processing/%d", got.Status, got.Attempts, c[0].Attempts)
	}
}

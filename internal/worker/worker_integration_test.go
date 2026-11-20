//go:build integration

package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"notification-service/internal/domain"
	"notification-service/internal/provider"
	"notification-service/internal/queue"
	"notification-service/internal/ratelimit"
	"notification-service/internal/store"
	"notification-service/internal/testutil"
	"notification-service/internal/worker"
)

var (
	db  *testutil.Postgres
	rdb *testutil.Redis
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	if db, err = testutil.StartPostgres(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "integration setup:", err)
		os.Exit(1)
	}
	if rdb, err = testutil.StartRedis(ctx); err != nil {
		db.Close(ctx)
		fmt.Fprintln(os.Stderr, "integration setup:", err)
		os.Exit(1)
	}
	code := m.Run()
	rdb.Close(ctx)
	db.Close(ctx)
	os.Exit(code)
}

// --- fake provider ---

// fakeProvider records every call and answers with whatever respond returns
// for (notification id, 1-based call number for that id).
type fakeProvider struct {
	*httptest.Server
	mu      sync.Mutex
	calls   map[string]int
	headers []http.Header
	bodies  []map[string]string
	started chan struct{} // receives once per request, before any delay
	delay   time.Duration
	respond func(id string, call int) int
}

func newFakeProvider(t *testing.T, respond func(id string, call int) int) *fakeProvider {
	t.Helper()
	fp := &fakeProvider{calls: map[string]int{}, respond: respond, started: make(chan struct{}, 1000)}
	fp.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Notification-ID")
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)

		fp.mu.Lock()
		fp.calls[id]++
		call := fp.calls[id]
		fp.headers = append(fp.headers, r.Header.Clone())
		fp.bodies = append(fp.bodies, body)
		fp.mu.Unlock()
		fp.started <- struct{}{}

		if fp.delay > 0 {
			select {
			case <-time.After(fp.delay):
			case <-r.Context().Done():
				return
			}
		}
		status := fp.respond(id, call)
		w.WriteHeader(status)
		if status == http.StatusAccepted {
			_, _ = fmt.Fprintf(w, `{"messageId":"msg-%s-%d","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`, id, call)
		}
	}))
	t.Cleanup(fp.Close)
	return fp
}

func (fp *fakeProvider) callsFor(id uuid.UUID) int {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return fp.calls[id.String()]
}

// order returns notification ids in the order requests arrived.
func (fp *fakeProvider) order() []string {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	ids := make([]string, len(fp.headers))
	for i, h := range fp.headers {
		ids[i] = h.Get("X-Notification-ID")
	}
	return ids
}

func (fp *fakeProvider) totalCalls() int {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	total := 0
	for _, n := range fp.calls {
		total += n
	}
	return total
}

func always(status int) func(string, int) int {
	return func(string, int) int { return status }
}

// --- helpers ---

func testConfig() worker.Config {
	return worker.Config{
		Channels:        domain.Channels,
		Concurrency:     5,
		ClaimBatchSize:  5,
		PollInterval:    20 * time.Millisecond,
		LeaseDuration:   10 * time.Second,
		ReaperInterval:  time.Hour, // reaper is exercised directly in its own test
		ShutdownTimeout: 2 * time.Second,
		// Constant 20ms (Factor 1, Random 1) keeps retry tests fast and deterministic.
		Policy: worker.RetryPolicy{Backoff: worker.Backoff{
			Base: 20 * time.Millisecond, Factor: 1, Max: 20 * time.Millisecond,
			Random: func() float64 { return 1 },
		}},
	}
}

// startWorker runs an unlimited worker until the test ends (or stop is
// called) and returns a stop function that blocks until Run has returned.
func startWorker(t *testing.T, providerURL string, cfg worker.Config) (stop func()) {
	t.Helper()
	return startWorkerWithLimiter(t, providerURL, cfg, worker.Unlimited{})
}

func startWorkerWithLimiter(t *testing.T, providerURL string, cfg worker.Config, limiter worker.RateLimiter) (stop func()) {
	t.Helper()
	w := worker.New(queue.New(db.Pool), provider.NewClient(providerURL, 2*time.Second), limiter, cfg,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx)
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	t.Cleanup(stop)
	return stop
}

type notificationOpt func(*domain.Notification)

func withPriority(p domain.Priority) notificationOpt {
	return func(x *domain.Notification) { x.Priority = p }
}

func withMaxAttempts(n int) notificationOpt {
	return func(x *domain.Notification) { x.MaxAttempts = n }
}

func scheduledAt(at time.Time) notificationOpt {
	return func(x *domain.Notification) {
		x.Status, x.ScheduledAt, x.NextAttemptAt = domain.StatusScheduled, &at, at
	}
}

func insertNotification(t *testing.T, ch domain.Channel, opts ...notificationOpt) domain.Notification {
	t.Helper()
	n := domain.Notification{
		ID:            uuid.Must(uuid.NewV7()),
		CorrelationID: "corr-" + uuid.NewString()[:8],
		Channel:       ch,
		Priority:      domain.PriorityNormal,
		Recipient:     "+905551234567",
		Content:       "hello",
		Status:        domain.StatusPending,
		MaxAttempts:   5,
		NextAttemptAt: time.Now(),
	}
	for _, opt := range opts {
		opt(&n)
	}
	stored, _, err := store.New(db.Pool).InsertNotification(context.Background(), n)
	if err != nil {
		t.Fatal(err)
	}
	return stored
}

func get(t *testing.T, id uuid.UUID) domain.Notification {
	t.Helper()
	n, err := store.New(db.Pool).GetNotification(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func attempts(t *testing.T, id uuid.UUID) []domain.DeliveryAttempt {
	t.Helper()
	a, err := store.New(db.Pool).ListAttempts(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// waitForStatus polls until the notification reaches want, or fails the test.
func waitForStatus(t *testing.T, id uuid.UUID, want domain.Status, timeout time.Duration) domain.Notification {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		n := get(t, id)
		if n.Status == want {
			return n
		}
		if time.Now().After(deadline) {
			t.Fatalf("notification %s: status = %s after %v, want %s (last_error=%v)",
				id, n.Status, timeout, want, deref(n.LastError))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func outcomes(as []domain.DeliveryAttempt) string {
	parts := make([]string, len(as))
	for i, a := range as {
		parts[i] = string(a.Outcome)
	}
	return strings.Join(parts, ",")
}

// --- delivery ---

func TestDelivery_Sent(t *testing.T) {
	db.Reset(t)
	fp := newFakeProvider(t, always(http.StatusAccepted))
	n := insertNotification(t, domain.ChannelEmail, func(x *domain.Notification) {
		x.Recipient, x.Content = "user@example.com", "Welcome aboard"
	})
	startWorker(t, fp.URL, testConfig())

	sent := waitForStatus(t, n.ID, domain.StatusSent, 5*time.Second)
	if deref(sent.ProviderMessageID) != "msg-"+n.ID.String()+"-1" || sent.SentAt == nil || sent.LockedUntil != nil {
		t.Errorf("sent row = message_id %v sent_at %v locked_until %v",
			deref(sent.ProviderMessageID), sent.SentAt, sent.LockedUntil)
	}
	as := attempts(t, n.ID)
	if len(as) != 1 || as[0].Outcome != domain.OutcomeSuccess || as[0].HTTPStatus == nil || *as[0].HTTPStatus != 202 {
		t.Errorf("attempts = %+v", as)
	}

	fp.mu.Lock()
	defer fp.mu.Unlock()
	h, body := fp.headers[0], fp.bodies[0]
	if h.Get("X-Notification-ID") != n.ID.String() || h.Get("X-Correlation-ID") != n.CorrelationID {
		t.Errorf("provider headers = %v", h)
	}
	if body["to"] != "user@example.com" || body["channel"] != "email" || body["content"] != "Welcome aboard" {
		t.Errorf("provider body = %v", body)
	}
}

func TestDelivery_RetriesThenSucceeds(t *testing.T) {
	db.Reset(t)
	fp := newFakeProvider(t, func(_ string, call int) int {
		if call <= 2 {
			return http.StatusServiceUnavailable
		}
		return http.StatusAccepted
	})
	n := insertNotification(t, domain.ChannelSMS)
	startWorker(t, fp.URL, testConfig())

	sent := waitForStatus(t, n.ID, domain.StatusSent, 5*time.Second)
	if sent.Attempts != 3 || sent.LastError != nil {
		t.Errorf("attempts = %d, last_error = %v; want 3, nil", sent.Attempts, deref(sent.LastError))
	}
	as := attempts(t, n.ID)
	if got := outcomes(as); got != "retryable_error,retryable_error,success" {
		t.Errorf("attempt outcomes = %s", got)
	}
	for i, a := range as {
		if a.AttemptNo != i+1 {
			t.Errorf("attempt %d has attempt_no %d", i, a.AttemptNo)
		}
	}
}

func TestDelivery_PermanentFailureIsNotRetried(t *testing.T) {
	db.Reset(t)
	fp := newFakeProvider(t, always(http.StatusBadRequest))
	n := insertNotification(t, domain.ChannelPush)
	startWorker(t, fp.URL, testConfig())

	failed := waitForStatus(t, n.ID, domain.StatusFailed, 5*time.Second)
	time.Sleep(200 * time.Millisecond) // give a buggy worker time to retry
	if fp.callsFor(n.ID) != 1 || failed.Attempts != 1 {
		t.Errorf("provider calls = %d, attempts = %d; want 1, 1", fp.callsFor(n.ID), failed.Attempts)
	}
	if !strings.Contains(deref(failed.LastError), "400") {
		t.Errorf("last_error = %q", deref(failed.LastError))
	}
	if got := outcomes(attempts(t, n.ID)); got != "permanent_error" {
		t.Errorf("outcomes = %s", got)
	}
}

func TestDelivery_FailsAfterMaxAttempts(t *testing.T) {
	db.Reset(t)
	fp := newFakeProvider(t, always(http.StatusInternalServerError))
	n := insertNotification(t, domain.ChannelSMS, withMaxAttempts(3))
	startWorker(t, fp.URL, testConfig())

	failed := waitForStatus(t, n.ID, domain.StatusFailed, 5*time.Second)
	if failed.Attempts != 3 || fp.callsFor(n.ID) != 3 {
		t.Errorf("attempts = %d, calls = %d; want 3, 3", failed.Attempts, fp.callsFor(n.ID))
	}
	if !strings.Contains(deref(failed.LastError), "giving up after 3 attempts") {
		t.Errorf("last_error = %q", deref(failed.LastError))
	}
}

func TestDelivery_CancelledIsNeverSent(t *testing.T) {
	db.Reset(t)
	fp := newFakeProvider(t, always(http.StatusAccepted))
	n := insertNotification(t, domain.ChannelSMS)
	if _, err := store.New(db.Pool).CancelNotification(context.Background(), n.ID); err != nil {
		t.Fatal(err)
	}
	startWorker(t, fp.URL, testConfig())

	time.Sleep(300 * time.Millisecond)
	if fp.totalCalls() != 0 || get(t, n.ID).Status != domain.StatusCancelled {
		t.Errorf("cancelled notification was sent: calls=%d status=%s", fp.totalCalls(), get(t, n.ID).Status)
	}
}

func TestDelivery_ScheduledWaitsUntilDue(t *testing.T) {
	db.Reset(t)
	fp := newFakeProvider(t, always(http.StatusAccepted))
	n := insertNotification(t, domain.ChannelSMS, scheduledAt(time.Now().Add(700*time.Millisecond)))
	startWorker(t, fp.URL, testConfig())

	time.Sleep(300 * time.Millisecond)
	if s := get(t, n.ID).Status; s != domain.StatusScheduled {
		t.Fatalf("status before due = %s, want scheduled", s)
	}
	waitForStatus(t, n.ID, domain.StatusSent, 5*time.Second)
}

func TestDelivery_TwoWorkersNeverSendTheSameNotificationTwice(t *testing.T) {
	db.Reset(t)
	fp := newFakeProvider(t, always(http.StatusAccepted))
	const total = 300
	ids := make([]uuid.UUID, total)
	for i := range ids {
		ids[i] = insertNotification(t, domain.Channels[i%len(domain.Channels)]).ID
	}

	cfg := testConfig()
	cfg.Concurrency, cfg.ClaimBatchSize = 10, 10
	startWorker(t, fp.URL, cfg)
	startWorker(t, fp.URL, cfg)

	deadline := time.Now().Add(15 * time.Second)
	for {
		var sent int
		if err := db.Pool.QueryRow(context.Background(),
			"SELECT count(*) FROM notifications WHERE status = 'sent'").Scan(&sent); err != nil {
			t.Fatal(err)
		}
		if sent == total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d sent", sent, total)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, id := range ids {
		if c := fp.callsFor(id); c != 1 {
			t.Errorf("notification %s sent %d times", id, c)
		}
	}
}

// --- graceful shutdown ---

func TestShutdown_FinishesInFlightSend(t *testing.T) {
	db.Reset(t)
	fp := newFakeProvider(t, always(http.StatusAccepted))
	fp.delay = 300 * time.Millisecond
	n := insertNotification(t, domain.ChannelSMS)
	stop := startWorker(t, fp.URL, testConfig())

	<-fp.started // the provider call is in flight
	stop()       // SIGTERM equivalent; blocks until Run returns

	if s := get(t, n.ID).Status; s != domain.StatusSent {
		t.Errorf("status after graceful shutdown = %s, want sent (in-flight send should finish)", s)
	}
}

func TestShutdown_TimeoutCancelsSendAndRecordsIt(t *testing.T) {
	db.Reset(t)
	fp := newFakeProvider(t, always(http.StatusAccepted))
	fp.delay = 5 * time.Second
	n := insertNotification(t, domain.ChannelSMS)
	cfg := testConfig()
	cfg.ShutdownTimeout = 100 * time.Millisecond
	stop := startWorker(t, fp.URL, cfg)

	<-fp.started
	start := time.Now()
	stop()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("shutdown took %v; the shutdown timeout was not enforced", elapsed)
	}

	// The aborted call may or may not have reached the provider, so it is
	// recorded as a retryable attempt rather than silently dropped.
	got := get(t, n.ID)
	if got.Status != domain.StatusRetrying || got.LockedUntil != nil {
		t.Errorf("status = %s locked_until = %v, want retrying with no lease", got.Status, got.LockedUntil)
	}
	if as := attempts(t, n.ID); len(as) != 1 || as[0].Outcome != domain.OutcomeRetryableError {
		t.Errorf("attempts = %+v", as)
	}
}

// --- queue mechanics ---

func TestQueue_ReapExpiredLeases(t *testing.T) {
	db.Reset(t)
	ctx := context.Background()
	q := queue.New(db.Pool)
	recoverable := insertNotification(t, domain.ChannelSMS)
	exhausted := insertNotification(t, domain.ChannelSMS, withMaxAttempts(2))
	healthy := insertNotification(t, domain.ChannelSMS)

	// Simulate crashed workers: processing with an expired lease.
	for _, tc := range []struct {
		id       uuid.UUID
		attempts int
		lease    string
	}{
		{recoverable.ID, 1, "-1 minute"},
		{exhausted.ID, 2, "-1 minute"},
		{healthy.ID, 1, "+1 minute"}, // still leased: must not be touched
	} {
		if _, err := db.Pool.Exec(ctx, `UPDATE notifications
			SET status = 'processing', attempts = $2, locked_until = now() + $3::interval WHERE id = $1`,
			tc.id, tc.attempts, tc.lease); err != nil {
			t.Fatal(err)
		}
	}

	res, err := q.ReapExpired(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Requeued != 1 || res.Failed != 1 {
		t.Errorf("reap result = %+v, want 1 requeued, 1 failed", res)
	}
	if s := get(t, recoverable.ID).Status; s != domain.StatusPending {
		t.Errorf("recoverable status = %s, want pending", s)
	}
	if s := get(t, exhausted.ID).Status; s != domain.StatusFailed {
		t.Errorf("exhausted status = %s, want failed", s)
	}
	if s := get(t, healthy.ID).Status; s != domain.StatusProcessing {
		t.Errorf("healthy lease status = %s, want processing", s)
	}
}

func TestQueue_StaleWorkerCannotOverwriteNewerClaim(t *testing.T) {
	db.Reset(t)
	ctx := context.Background()
	q := queue.New(db.Pool)
	n := insertNotification(t, domain.ChannelSMS)

	first, err := q.Claim(ctx, domain.ChannelSMS, 1, -time.Second) // lease already expired
	if err != nil || len(first) != 1 {
		t.Fatalf("claim = %v, %v", first, err)
	}
	if _, err := q.ReapExpired(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := q.Claim(ctx, domain.ChannelSMS, 1, time.Minute)
	if err != nil || len(second) != 1 || second[0].Attempts != 2 {
		t.Fatalf("re-claim = %+v, %v", second, err)
	}

	// The stalled first worker finally reports success with attempt 1.
	err = q.Complete(ctx, queue.Completion{
		ID: n.ID, AttemptNo: first[0].Attempts, Status: domain.StatusSent,
		Attempt: domain.DeliveryAttempt{StartedAt: time.Now(), Outcome: domain.OutcomeSuccess},
	})
	if !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("stale complete err = %v, want ErrLeaseLost", err)
	}
	got := get(t, n.ID)
	if got.Status != domain.StatusProcessing || got.Attempts != 2 {
		t.Errorf("row = %s attempts %d; the newer claim must be untouched", got.Status, got.Attempts)
	}
	if as := attempts(t, n.ID); len(as) != 1 || as[0].AttemptNo != 1 {
		t.Errorf("the stale attempt should still be recorded in history: %+v", as)
	}
}

func TestQueue_ReleaseRefundsTheAttempt(t *testing.T) {
	db.Reset(t)
	ctx := context.Background()
	q := queue.New(db.Pool)
	n := insertNotification(t, domain.ChannelEmail, func(x *domain.Notification) { x.Recipient = "u@example.com" })

	jobs, err := q.Claim(ctx, domain.ChannelEmail, 1, time.Minute)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim = %v, %v", jobs, err)
	}
	released, err := q.Release(ctx, jobs)
	if err != nil || released != 1 {
		t.Fatalf("release = %d, %v", released, err)
	}
	got := get(t, n.ID)
	if got.Status != domain.StatusPending || got.Attempts != 0 || got.LockedUntil != nil {
		t.Errorf("after release: status %s attempts %d locked_until %v", got.Status, got.Attempts, got.LockedUntil)
	}
}

func TestQueue_ClaimIsPerChannel(t *testing.T) {
	db.Reset(t)
	ctx := context.Background()
	insertNotification(t, domain.ChannelSMS)
	jobs, err := queue.New(db.Pool).Claim(ctx, domain.ChannelPush, 10, time.Minute)
	if err != nil || len(jobs) != 0 {
		t.Errorf("push claim returned %d sms jobs (err %v)", len(jobs), err)
	}
}

// --- rate limiting ---

// Two worker replicas share one Redis bucket, so together they must stay
// near rate*window + burst, not 2x that.
func TestRateLimit_CapsChannelThroughputAcrossWorkers(t *testing.T) {
	db.Reset(t)
	fp := newFakeProvider(t, always(http.StatusAccepted))
	for range 500 {
		insertNotification(t, domain.ChannelSMS)
	}

	const rate, burst = 100.0, 10
	limits := map[domain.Channel]ratelimit.Limit{}
	for _, ch := range domain.Channels {
		limits[ch] = ratelimit.Limit{PerSecond: rate, Burst: burst}
	}
	prefix := "test-" + uuid.NewString() + ":"
	cfg := testConfig()
	cfg.Concurrency, cfg.ClaimBatchSize = 20, 10

	stopA := startWorkerWithLimiter(t, fp.URL, cfg, ratelimit.New(rdb.Client, limits, prefix))
	stopB := startWorkerWithLimiter(t, fp.URL, cfg, ratelimit.New(rdb.Client, limits, prefix))
	const window = 2 * time.Second
	time.Sleep(window)
	stopA()
	stopB()

	want := float64(burst) + rate*window.Seconds() // ~210
	got := float64(fp.totalCalls())
	if got < want*0.8 || got > want*1.15 {
		t.Errorf("provider received %.0f sms in %v with 2 workers, want ~%.0f", got, window, want)
	}
	t.Logf("2 workers sent %.0f sms in %v (cap ~%.0f)", got, window, want)
}

// --- priority ---

func TestPriority_ClaimOrderIsStrict(t *testing.T) {
	db.Reset(t)
	ctx := context.Background()
	q := queue.New(db.Pool)
	// Insert in the "wrong" order: the low ones are the oldest.
	want := map[domain.Priority]map[uuid.UUID]bool{}
	for _, p := range []domain.Priority{domain.PriorityLow, domain.PriorityNormal, domain.PriorityHigh} {
		want[p] = map[uuid.UUID]bool{}
		for range 3 {
			want[p][insertNotification(t, domain.ChannelSMS, withPriority(p)).ID] = true
		}
	}

	for _, p := range []domain.Priority{domain.PriorityHigh, domain.PriorityNormal, domain.PriorityLow} {
		jobs, err := q.Claim(ctx, domain.ChannelSMS, 3, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range jobs {
			if !want[p][j.ID] {
				t.Errorf("claim for %s returned a %s notification", p, j.Priority)
			}
		}
	}
}

func TestPriority_HighIsSentBeforeOlderLow(t *testing.T) {
	db.Reset(t)
	fp := newFakeProvider(t, always(http.StatusAccepted))
	low := insertNotification(t, domain.ChannelSMS, withPriority(domain.PriorityLow))
	normal := insertNotification(t, domain.ChannelSMS)
	high := insertNotification(t, domain.ChannelSMS, withPriority(domain.PriorityHigh))

	cfg := testConfig()
	cfg.Concurrency, cfg.ClaimBatchSize = 1, 1 // one at a time, so order is observable
	startWorker(t, fp.URL, cfg)
	waitForStatus(t, low.ID, domain.StatusSent, 5*time.Second)

	got := strings.Join(fp.order(), ",")
	want := strings.Join([]string{high.ID.String(), normal.ID.String(), low.ID.String()}, ",")
	if got != want {
		t.Errorf("send order = %s\nwant         %s (high, normal, low)", got, want)
	}
}

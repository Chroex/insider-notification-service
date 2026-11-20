package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"notification-service/internal/domain"
	"notification-service/internal/observability"
	"notification-service/internal/provider"
	"notification-service/internal/queue"
)

// Queue is the subset of queue.Queue the worker needs (an interface so the
// dispatch loop can be unit tested without Postgres).
type Queue interface {
	Claim(ctx context.Context, channel domain.Channel, limit int, lease time.Duration) ([]domain.Notification, error)
	Complete(ctx context.Context, c queue.Completion) error
	Release(ctx context.Context, jobs []domain.Notification) (int64, error)
	ReapExpired(ctx context.Context) (queue.ReapResult, error)
}

// Sender delivers one message; implemented by provider.Client.
type Sender interface {
	Send(ctx context.Context, msg provider.Message) (provider.Result, error)
}

// RateLimiter hands out per-channel send permits; implemented by
// ratelimit.Limiter (Redis token bucket shared by all replicas).
type RateLimiter interface {
	// Acquire blocks until it can grant between 1 and n permits.
	Acquire(ctx context.Context, ch domain.Channel, n int) (int, error)
	// Refund returns permits that were acquired but not used.
	Refund(ctx context.Context, ch domain.Channel, n int) error
}

// Unlimited is a RateLimiter that never limits (tests, local experiments).
type Unlimited struct{}

func (Unlimited) Acquire(_ context.Context, _ domain.Channel, n int) (int, error) { return n, nil }
func (Unlimited) Refund(context.Context, domain.Channel, int) error               { return nil }

// Config tunes the worker.
type Config struct {
	Channels        []domain.Channel
	Concurrency     int // parallel sends per channel
	ClaimBatchSize  int
	PollInterval    time.Duration
	LeaseDuration   time.Duration
	ReaperInterval  time.Duration
	ShutdownTimeout time.Duration
	Policy          RetryPolicy

	// Circuit breaker per channel (see Breaker).
	BreakerThreshold int
	BreakerCooldown  time.Duration

	// Observer receives delivery events for metrics. Optional.
	Observer Observer
}

// Observer is notified of delivery events; implemented by
// observability.WorkerMetrics. Defined here, on the consumer side, so the
// engine has no Prometheus dependency and tests can plug in a fake.
type Observer interface {
	// AttemptFinished is called for every provider call, whether or not its
	// result could be recorded (the call happened either way).
	AttemptFinished(ch domain.Channel, outcome domain.AttemptOutcome, providerLatency time.Duration)
	// NotificationSent / NotificationFailed are called once the new status
	// is committed.
	NotificationSent(ch domain.Channel, endToEnd time.Duration)
	NotificationFailed(ch domain.Channel, reason string)
	LeasesReaped(requeued int, failedByChannel map[domain.Channel]int)
	// BreakerStateChanged receives int(BreakerState): 0 closed, 1 open, 2 half-open.
	BreakerStateChanged(ch domain.Channel, state int)
}

// NopObserver ignores every event.
type NopObserver struct{}

func (NopObserver) AttemptFinished(domain.Channel, domain.AttemptOutcome, time.Duration) {}
func (NopObserver) NotificationSent(domain.Channel, time.Duration)                       {}
func (NopObserver) NotificationFailed(domain.Channel, string)                            {}
func (NopObserver) LeasesReaped(int, map[domain.Channel]int)                             {}
func (NopObserver) BreakerStateChanged(domain.Channel, int)                              {}

// persistTimeout bounds the DB write after a send. It uses a context that is
// not cancelled by shutdown: once the provider has answered, losing the
// result would cause a needless duplicate send after the lease expires.
const persistTimeout = 5 * time.Second

type Worker struct {
	queue    Queue
	sender   Sender
	limiter  RateLimiter
	breakers map[domain.Channel]*Breaker
	cfg      Config
	log      *slog.Logger
	now      func() time.Time
}

func New(q Queue, s Sender, limiter RateLimiter, cfg Config, log *slog.Logger) *Worker {
	if cfg.Observer == nil {
		cfg.Observer = NopObserver{}
	}
	w := &Worker{queue: q, sender: s, limiter: limiter, cfg: cfg, log: log, now: time.Now,
		breakers: make(map[domain.Channel]*Breaker, len(cfg.Channels))}
	for _, ch := range cfg.Channels {
		w.breakers[ch] = NewBreaker(cfg.BreakerThreshold, cfg.BreakerCooldown, func(from, to BreakerState) {
			level := slog.LevelWarn
			if to == BreakerClosed {
				level = slog.LevelInfo
			}
			log.Log(context.Background(), level, "circuit breaker state changed",
				observability.KeyChannel, ch, "from", from.String(), "to", to.String())
			cfg.Observer.BreakerStateChanged(ch, int(to))
		})
	}
	return w
}

// BreakerState reports a channel's circuit breaker state.
func (w *Worker) BreakerState(ch domain.Channel) BreakerState {
	return w.breakers[ch].State()
}

// Run processes notifications until ctx is cancelled, then shuts down
// gracefully:
//  1. stop claiming new work (the claim loops exit),
//  2. wait up to ShutdownTimeout for in-flight sends to finish,
//  3. if they don't, cancel them; their results are still recorded.
func (w *Worker) Run(ctx context.Context) {
	// Sends get their own context so SIGTERM does not abort a provider call
	// that is about to succeed; only the shutdown deadline cancels them.
	sendCtx, cancelSends := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSends()

	var loops, inflight sync.WaitGroup
	for _, ch := range w.cfg.Channels {
		loops.Add(1)
		go func() {
			defer loops.Done()
			w.claimLoop(ctx, sendCtx, ch, &inflight)
		}()
	}
	loops.Add(1)
	go func() {
		defer loops.Done()
		w.reaperLoop(ctx)
	}()

	w.log.Info("worker started", "channels", w.cfg.Channels, "concurrency_per_channel", w.cfg.Concurrency)
	<-ctx.Done()
	loops.Wait() // no new claims after this point, so inflight.Add can't race Wait
	w.log.Info("worker draining in-flight sends", "timeout", w.cfg.ShutdownTimeout.String())

	drained := make(chan struct{})
	go func() {
		inflight.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		w.log.Info("worker drained cleanly")
	case <-time.After(w.cfg.ShutdownTimeout):
		w.log.Warn("shutdown timeout reached, cancelling remaining sends")
		cancelSends()
		<-drained
	}
}

// claimLoop runs per channel. Each iteration:
//  0. asks the channel's circuit breaker how many sends may start (none
//     while open: rows stay queued and keep their attempt budget),
//  1. waits for free send slots (local concurrency),
//  2. takes that many rate-limit tokens (global, shared via Redis),
//  3. claims at most that many rows, and refunds tokens it did not use.
//
// Tokens are taken *before* claiming so a claimed row never sits waiting
// for a token while its lease ticks down, and so the claim never takes
// more rows than we are allowed to send right now.
func (w *Worker) claimLoop(ctx, sendCtx context.Context, ch domain.Channel, inflight *sync.WaitGroup) {
	log := w.log.With(observability.KeyChannel, ch)
	slots := make(chan struct{}, w.cfg.Concurrency)
	breaker := w.breakers[ch]

	for {
		permits, wait := breaker.Permits()
		if permits == 0 {
			// Open (wait = rest of the cooldown) or a half-open probe is
			// already in flight. Sleep rather than poll the database.
			sleep(ctx, max(wait, w.cfg.PollInterval))
			if ctx.Err() != nil {
				return
			}
			continue
		}
		limit := w.cfg.ClaimBatchSize
		if permits != breakerNoLimit {
			limit = min(limit, permits)
		}

		n, ok := acquireSlots(ctx, slots, limit)
		if !ok {
			breaker.ReleaseProbe()
			return // shutting down
		}

		tokens, err := w.limiter.Acquire(ctx, ch, n)
		if err != nil {
			breaker.ReleaseProbe()
			releaseSlots(slots, n)
			if ctx.Err() != nil {
				return
			}
			// Fail closed: without Redis we cannot prove we are under the
			// provider's limit, and getting throttled or banned is worse
			// than pausing this channel for a moment.
			log.Error("rate limiter unavailable, pausing channel", "error", err)
			sleep(ctx, w.cfg.PollInterval)
			continue
		}
		releaseSlots(slots, n-tokens)

		jobs, err := w.queue.Claim(ctx, ch, tokens, w.cfg.LeaseDuration)
		unused := tokens - len(jobs)
		releaseSlots(slots, unused)
		w.refund(log, ch, unused)
		if len(jobs) == 0 {
			breaker.ReleaseProbe() // nothing due, so no probe was sent
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("claim failed", "error", err)
			sleep(ctx, w.cfg.PollInterval)
			continue
		}

		for _, job := range jobs {
			inflight.Add(1)
			go func() {
				defer inflight.Done()
				defer releaseSlots(slots, 1)
				w.process(ctx, sendCtx, job)
			}()
		}
		if len(jobs) == 0 {
			sleep(ctx, w.cfg.PollInterval) // idle: nothing due on this channel
		}
	}
}

// process makes one delivery attempt and records the outcome.
func (w *Worker) process(stopCtx, sendCtx context.Context, job domain.Notification) {
	log := w.log.With(
		observability.KeyNotificationID, job.ID,
		observability.KeyCorrelationID, job.CorrelationID,
		observability.KeyChannel, job.Channel,
		observability.KeyAttempt, job.Attempts,
	)
	if job.BatchID != nil {
		log = log.With(observability.KeyBatchID, *job.BatchID)
	}

	// Shutdown began before this job's send started: hand it back untouched
	// (attempt refunded) instead of starting a call we may not finish.
	if stopCtx.Err() != nil {
		w.release(log, job, "released unsent job on shutdown")
		return
	}
	// The breaker opened between claim and send (another send on this
	// channel tripped it): don't add load to a failing provider.
	breaker := w.breakers[job.Channel]
	if breaker.IsOpen() {
		w.release(log, job, "released job: circuit breaker open")
		return
	}

	start := w.now()
	res, sendErr := w.sender.Send(sendCtx, provider.Message{
		NotificationID: job.ID,
		CorrelationID:  job.CorrelationID,
		To:             job.Recipient,
		Channel:        string(job.Channel),
		Content:        job.Content,
	})
	duration := w.now().Sub(start)

	c := Classify(sendErr)
	w.cfg.Observer.AttemptFinished(job.Channel, c.Outcome, duration)
	if sendCtx.Err() == nil { // a send aborted by our own shutdown says nothing about provider health
		breaker.Record(c.Outcome)
	}
	decision := w.cfg.Policy.Decide(job.Attempts, job.MaxAttempts, c, w.now())
	completion := buildCompletion(job, res, c, decision, start, duration)

	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(sendCtx), persistTimeout)
	defer cancel()
	if err := w.queue.Complete(persistCtx, completion); err != nil {
		if errors.Is(err, queue.ErrLeaseLost) {
			log.Warn("lease lost before result was recorded; another worker owns the notification",
				"outcome", c.Outcome)
			return
		}
		// The row stays 'processing' until its lease expires; the reaper
		// will re-queue it (at-least-once).
		log.Error("failed to record attempt", "error", err, "outcome", c.Outcome)
		return
	}

	w.observeDecision(job, decision)
	logDecision(log, decision, c, duration)
}

// observeDecision reports the committed status change to the observer.
func (w *Worker) observeDecision(job domain.Notification, d Decision) {
	switch d.Status {
	case domain.StatusSent:
		// Measured from when the notification became due, so a send
		// scheduled for tomorrow doesn't look like a day of latency.
		due := job.CreatedAt
		if job.ScheduledAt != nil && job.ScheduledAt.After(due) {
			due = *job.ScheduledAt
		}
		w.cfg.Observer.NotificationSent(job.Channel, w.now().Sub(due))
	case domain.StatusFailed:
		w.cfg.Observer.NotificationFailed(job.Channel, d.FailReason)
	}
}

// refund returns unused tokens. Failure only costs a little throughput (the
// tokens refill anyway), so it is logged, not retried.
func (w *Worker) refund(log *slog.Logger, ch domain.Channel, n int) {
	if n <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.limiter.Refund(ctx, ch, n); err != nil {
		log.Warn("rate limit refund failed", "error", err, "tokens", n)
	}
}

func (w *Worker) release(log *slog.Logger, job domain.Notification, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
	defer cancel()
	if _, err := w.queue.Release(ctx, []domain.Notification{job}); err != nil {
		log.Error("failed to release job; reaper will recover it", "error", err)
		return
	}
	log.Info(reason)
}

// reaperLoop periodically re-queues rows whose lease expired.
func (w *Worker) reaperLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.ReaperInterval)
	defer ticker.Stop()
	for {
		res, err := w.queue.ReapExpired(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			w.log.Error("reaper failed", "error", err)
		case res.Requeued > 0 || res.Failed > 0:
			w.log.Warn("reaped expired leases", "requeued", res.Requeued, "failed", res.Failed)
			w.cfg.Observer.LeasesReaped(res.Requeued, res.FailedByChannel)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func buildCompletion(job domain.Notification, res provider.Result, c Classification, d Decision,
	start time.Time, duration time.Duration) queue.Completion {

	attempt := domain.DeliveryAttempt{
		StartedAt:  start,
		DurationMS: int(duration.Milliseconds()),
		HTTPStatus: c.HTTPStatus,
		Outcome:    c.Outcome,
	}
	if c.Outcome == domain.OutcomeSuccess {
		status := res.StatusCode
		attempt.HTTPStatus = &status
	}
	if c.Err != "" {
		msg := c.Err
		attempt.Error = &msg
	}

	completion := queue.Completion{
		ID:            job.ID,
		AttemptNo:     job.Attempts,
		Status:        d.Status,
		NextAttemptAt: d.NextAttemptAt,
		LastError:     d.LastError,
		Attempt:       attempt,
	}
	if res.MessageID != "" {
		id := res.MessageID
		completion.ProviderMessageID = &id
	}
	return completion
}

func logDecision(log *slog.Logger, d Decision, c Classification, duration time.Duration) {
	attrs := []any{"status", d.Status, "outcome", c.Outcome, "duration_ms", duration.Milliseconds()}
	if c.HTTPStatus != nil {
		attrs = append(attrs, "http_status", *c.HTTPStatus)
	}
	switch d.Status {
	case domain.StatusSent:
		log.Info("notification sent", attrs...)
	case domain.StatusRetrying:
		log.Warn("attempt failed, will retry", append(attrs, "error", c.Err, "next_attempt_at", d.NextAttemptAt)...)
	default:
		log.Warn("notification failed", append(attrs, "error", c.Err, "reason", d.FailReason)...)
	}
}

// acquireSlots blocks until at least one slot is free (or ctx ends), then
// grabs up to limit-1 more without blocking. Returns how many it holds.
func acquireSlots(ctx context.Context, slots chan struct{}, limit int) (int, bool) {
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return 0, false
	}
	n := 1
	for n < limit {
		select {
		case slots <- struct{}{}:
			n++
		default:
			return n, true
		}
	}
	return n, true
}

func releaseSlots(slots chan struct{}, n int) {
	for range n {
		<-slots
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"notification-service/internal/domain"
	"notification-service/internal/events"
	"notification-service/internal/observability"
	"notification-service/internal/provider"
	"notification-service/internal/queue"
)

// Queue is the subset of queue.Queue the worker needs (an interface so the
// dispatch loop can be unit tested without Postgres).
type Queue interface {
	Claim(ctx context.Context, channel domain.Channel, quota queue.Quota, lease time.Duration) ([]domain.Notification, error)
	Renew(ctx context.Context, id uuid.UUID, attempt int, lease time.Duration) (bool, error)
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

	// PriorityWeights share each channel's sends between high, normal and
	// low when all are waiting (e.g. 70/20/10), so low is never starved.
	// Zero for normal and low means strict priority. See fairShare.
	PriorityWeights [3]int

	// Circuit breaker per channel (see Breaker).
	BreakerThreshold int
	BreakerWindow    time.Duration // failures further apart than this are not consecutive
	BreakerCooldown  time.Duration

	// Observer receives delivery events for metrics. Optional.
	Observer Observer
	// Events publishes status changes for the live WebSocket stream. Optional.
	Events events.Publisher
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
	// endToEnd is created -> sent (the brief's definition); sinceDue starts
	// at scheduled_at for scheduled sends, so it shows only our delay.
	NotificationSent(ch domain.Channel, endToEnd, sinceDue time.Duration)
	NotificationFailed(ch domain.Channel, reason string)
	// LeasesReaped reports one reaper pass; interruptedByChannel counts the
	// attempts it recorded as interrupted.
	LeasesReaped(requeued int, failedByChannel, interruptedByChannel map[domain.Channel]int)
	// BreakerStateChanged receives int(BreakerState): 0 closed, 1 open, 2 half-open.
	BreakerStateChanged(ch domain.Channel, state int)
}

// NopObserver ignores every event.
type NopObserver struct{}

func (NopObserver) AttemptFinished(domain.Channel, domain.AttemptOutcome, time.Duration) {}
func (NopObserver) NotificationSent(domain.Channel, time.Duration, time.Duration)        {}
func (NopObserver) NotificationFailed(domain.Channel, string)                            {}
func (NopObserver) LeasesReaped(int, map[domain.Channel]int, map[domain.Channel]int)     {}
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
	if cfg.Events == nil {
		cfg.Events = events.Nop{}
	}
	w := &Worker{queue: q, sender: s, limiter: limiter, cfg: cfg, log: log, now: time.Now,
		breakers: make(map[domain.Channel]*Breaker, len(cfg.Channels))}
	for _, ch := range cfg.Channels {
		w.breakers[ch] = NewBreaker(cfg.BreakerThreshold, cfg.BreakerWindow, cfg.BreakerCooldown, func(from, to BreakerState) {
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
	share := newFairShare(w.cfg.PriorityWeights)

	for {
		permits, wait, gen := breaker.Permits()
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

		jobs, err := w.queue.Claim(ctx, ch, share.quota(tokens), w.cfg.LeaseDuration)
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
				w.process(ctx, sendCtx, job, gen)
			}()
		}
		if len(jobs) == 0 {
			sleep(ctx, w.cfg.PollInterval) // idle: nothing due on this channel
		}
	}
}

// process makes one delivery attempt and records the outcome.
func (w *Worker) process(stopCtx, sendCtx context.Context, job domain.Notification, gen uint64) {
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
	// The breaker changed state between claim and send (another send on
	// this channel tripped it): don't add load to a failing provider.
	breaker := w.breakers[job.Channel]
	if !breaker.Current(gen) {
		w.release(log, job, "released job: circuit breaker open")
		return
	}
	if !w.stillOwned(sendCtx, log, job) {
		return
	}

	// One span per attempt, continuing the trace of the request that created
	// the notification (traceparent stored on the row), so a single trace
	// shows API -> DB -> queue wait -> each attempt -> provider. Everything
	// below (DB writes, Redis publish, provider call) is parented to it.
	sendCtx, span := w.startAttemptSpan(sendCtx, job)
	defer span.End()
	if sc := span.SpanContext(); sc.IsValid() {
		log = log.With(observability.KeyTraceID, sc.TraceID().String())
	}

	w.cfg.Events.Publish(sendCtx, events.FromNotification(job, w.now())) // status: processing

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
		breaker.Record(c.Outcome, job.ID.String(), gen)
	}
	decision := w.cfg.Policy.Decide(job.Attempts, job.MaxAttempts, c, w.now())
	completion := buildCompletion(job, res, c, decision, start, duration)
	recordAttemptOnSpan(span, c, decision)

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
	w.publishDecision(sendCtx, job, decision)
	logDecision(log, decision, c, duration)
}

func (w *Worker) startAttemptSpan(ctx context.Context, job domain.Notification) (context.Context, trace.Span) {
	ctx = observability.ContextWithTraceParent(ctx, job.TraceParent)
	attrs := []attribute.KeyValue{
		attribute.String("notification.id", job.ID.String()),
		attribute.String("notification.channel", string(job.Channel)),
		attribute.String("notification.priority", string(job.Priority)),
		attribute.Int("notification.attempt", job.Attempts),
		attribute.String("correlation_id", job.CorrelationID),
	}
	if job.BatchID != nil {
		attrs = append(attrs, attribute.String("notification.batch_id", job.BatchID.String()))
	}
	return observability.Tracer().Start(ctx, "deliver "+string(job.Channel),
		trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attrs...))
}

func recordAttemptOnSpan(span trace.Span, c Classification, d Decision) {
	span.SetAttributes(
		attribute.String("delivery.outcome", string(c.Outcome)),
		attribute.String("notification.status", string(d.Status)),
	)
	if c.HTTPStatus != nil {
		span.SetAttributes(attribute.Int("http.response.status_code", *c.HTTPStatus))
	}
	if c.Outcome != domain.OutcomeSuccess {
		span.SetStatus(codes.Error, c.Err)
	}
}

// publishDecision streams the committed status change to live clients.
func (w *Worker) publishDecision(ctx context.Context, job domain.Notification, d Decision) {
	updated := job
	updated.Status, updated.LastError = d.Status, d.LastError
	w.cfg.Events.Publish(ctx, events.FromNotification(updated, w.now()))
}

// observeDecision reports the committed status change to the observer.
func (w *Worker) observeDecision(job domain.Notification, d Decision) {
	switch d.Status {
	case domain.StatusSent:
		// End-to-end is created -> sent, as the brief defines it. The
		// delay since becoming due is reported too, so a send scheduled
		// for tomorrow doesn't hide a slow queue behind a day of waiting.
		now := w.now()
		due := job.CreatedAt
		if job.ScheduledAt != nil && job.ScheduledAt.After(due) {
			due = *job.ScheduledAt
		}
		w.cfg.Observer.NotificationSent(job.Channel, now.Sub(job.CreatedAt), now.Sub(due))
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

// stillOwned checks, right before the provider call, that the row is still
// ours (processing under our attempt number) and renews the lease for the
// send. A worker that stalled past its lease may find the row re-queued
// and claimed by another worker; sending anyway would deliver twice.
func (w *Worker) stillOwned(ctx context.Context, log *slog.Logger, job domain.Notification) bool {
	ctx, cancel := context.WithTimeout(ctx, persistTimeout)
	defer cancel()
	owned, err := w.queue.Renew(ctx, job.ID, job.Attempts, w.cfg.LeaseDuration)
	switch {
	case err != nil:
		// Ownership unknown: don't send. Hand the job back (attempt
		// refunded); if that fails too, the lease expires and the reaper
		// recovers it.
		log.Error("could not confirm ownership before sending", "error", err)
		w.release(log, job, "released job: ownership check failed")
		return false
	case !owned:
		log.Warn("lease lost before sending; another worker owns the row")
		return false
	}
	return true
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
			w.cfg.Observer.LeasesReaped(res.Requeued, res.FailedByChannel, res.InterruptedByChannel)
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
		DurationMS: new(int(duration.Milliseconds())),
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
		ID:         job.ID,
		AttemptNo:  job.Attempts,
		Status:     d.Status,
		RetryDelay: d.RetryDelay,
		LastError:  d.LastError,
		Attempt:    attempt,
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

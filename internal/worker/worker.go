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
}

// persistTimeout bounds the DB write after a send. It uses a context that is
// not cancelled by shutdown: once the provider has answered, losing the
// result would cause a needless duplicate send after the lease expires.
const persistTimeout = 5 * time.Second

type Worker struct {
	queue  Queue
	sender Sender
	cfg    Config
	log    *slog.Logger
	now    func() time.Time
}

func New(q Queue, s Sender, cfg Config, log *slog.Logger) *Worker {
	return &Worker{queue: q, sender: s, cfg: cfg, log: log, now: time.Now}
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

// claimLoop runs per channel. It only claims as many rows as it has free
// send slots, so a claimed row is never left waiting under a lease while
// other sends finish.
func (w *Worker) claimLoop(ctx, sendCtx context.Context, ch domain.Channel, inflight *sync.WaitGroup) {
	log := w.log.With(observability.KeyChannel, ch)
	slots := make(chan struct{}, w.cfg.Concurrency)

	for {
		n, ok := acquireSlots(ctx, slots, w.cfg.ClaimBatchSize)
		if !ok {
			return // shutting down
		}

		jobs, err := w.queue.Claim(ctx, ch, n, w.cfg.LeaseDuration)
		releaseSlots(slots, n-len(jobs))
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
		w.release(log, job)
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

	logDecision(log, decision, c, duration)
}

func (w *Worker) release(log *slog.Logger, job domain.Notification) {
	ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
	defer cancel()
	if _, err := w.queue.Release(ctx, []domain.Notification{job}); err != nil {
		log.Error("failed to release job on shutdown; reaper will recover it", "error", err)
		return
	}
	log.Info("released unsent job on shutdown")
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

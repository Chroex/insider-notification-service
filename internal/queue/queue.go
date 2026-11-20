// Package queue implements the Postgres-backed work queue: claiming due
// notifications with leases, completing them, releasing them on shutdown and
// reaping leases left behind by crashed workers.
//
// Every write is a conditional UPDATE, so concurrent workers, the reaper and
// API cancellations can never overwrite each other's transitions.
package queue

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"notification-service/internal/domain"
	"notification-service/internal/store"
)

// ErrLeaseLost means the row was no longer ours when we tried to record the
// result: our lease expired, the reaper re-queued it and (possibly) another
// worker claimed it. The attempt is still recorded; the status is not.
var ErrLeaseLost = errors.New("lease lost: notification was re-queued or re-claimed")

type Queue struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Queue {
	return &Queue{pool: pool}
}

// Claim atomically takes up to limit due notifications for one channel and
// leases them to the caller.
//
//   - FOR UPDATE SKIP LOCKED: concurrent workers skip rows another worker is
//     claiming instead of blocking on them, so N workers claim N disjoint sets.
//   - It is a single statement (one implicit transaction) that commits before
//     the caller makes any HTTP call. Row locks are held for milliseconds,
//     never for the duration of a provider request.
//   - attempts is incremented here, so each claim gets a unique attempt
//     number. That number doubles as a fencing token for Complete/Release.
//
// The returned slice is ordered high priority first, then oldest due first.
func (q *Queue) Claim(ctx context.Context, channel domain.Channel, limit int, lease time.Duration) ([]domain.Notification, error) {
	rows, err := q.pool.Query(ctx, `
		WITH due AS (
			SELECT id
			FROM notifications
			WHERE status IN ('scheduled', 'pending', 'retrying')
			  AND channel = $1
			  AND next_attempt_at <= now()
			ORDER BY priority_rank, next_attempt_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE notifications n
		SET status       = 'processing',
		    attempts     = n.attempts + 1,
		    locked_until = now() + $3 * interval '1 millisecond'
		FROM due
		WHERE n.id = due.id
		RETURNING `+qualified("n", store.NotificationColumns),
		channel, limit, lease.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	defer rows.Close()

	var jobs []domain.Notification
	for rows.Next() {
		n, err := store.ScanNotification(rows)
		if err != nil {
			return nil, fmt.Errorf("claim: scan: %w", err)
		}
		jobs = append(jobs, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}

	// UPDATE ... RETURNING does not preserve the CTE's ORDER BY.
	slices.SortStableFunc(jobs, func(a, b domain.Notification) int {
		if r := a.Priority.Rank() - b.Priority.Rank(); r != 0 {
			return r
		}
		return a.NextAttemptAt.Compare(b.NextAttemptAt)
	})
	return jobs, nil
}

// Completion is the result of one attempt, ready to persist.
type Completion struct {
	ID        uuid.UUID
	AttemptNo int // the attempts value our claim produced (fencing token)
	Status    domain.Status
	// RetryDelay, for StatusRetrying, becomes next_attempt_at = now() +
	// delay on the database clock (the clock the claim query compares with).
	RetryDelay        *time.Duration
	LastError         *string
	ProviderMessageID *string
	Attempt           domain.DeliveryAttempt
}

// Complete records the attempt and moves the notification to its next status
// in one transaction.
//
// The UPDATE only matches if the row is still 'processing' under *our*
// attempt number. If the lease expired and the row was re-queued or claimed
// again, it matches nothing and we return ErrLeaseLost instead of clobbering
// the newer state. The attempt row is still committed: the provider call
// really happened, and the history should say so.
func (q *Queue) Complete(ctx context.Context, c Completion) error {
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("complete: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	a := c.Attempt
	if _, err := tx.Exec(ctx, `
		INSERT INTO delivery_attempts
			(notification_id, attempt_no, started_at, duration_ms, http_status, error, outcome)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		c.ID, c.AttemptNo, a.StartedAt, a.DurationMS, a.HTTPStatus, a.Error, a.Outcome); err != nil {
		return fmt.Errorf("complete: insert attempt: %w", err)
	}

	tag, err := tx.Exec(ctx, `
		UPDATE notifications
		SET status              = $3,
		    locked_until        = NULL,
		    next_attempt_at     = COALESCE(now() + $4::bigint * interval '1 millisecond', next_attempt_at),
		    last_error          = $5,
		    provider_message_id = COALESCE($6, provider_message_id),
		    sent_at             = CASE WHEN $3 = 'sent' THEN now() ELSE sent_at END
		WHERE id = $1 AND status = 'processing' AND attempts = $2`,
		c.ID, c.AttemptNo, c.Status, delayMillis(c.RetryDelay), c.LastError, c.ProviderMessageID)
	if err != nil {
		return fmt.Errorf("complete: update: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("complete: commit: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Release returns claimed-but-never-sent jobs to the queue, e.g. on shutdown.
// The attempt counter is rolled back because no provider call was made, so
// a graceful restart never burns a notification's retry budget.
func (q *Queue) Release(ctx context.Context, jobs []domain.Notification) (int64, error) {
	if len(jobs) == 0 {
		return 0, nil
	}
	ids := make([]uuid.UUID, len(jobs))
	attempts := make([]int32, len(jobs))
	for i, j := range jobs {
		ids[i] = j.ID
		attempts[i] = int32(j.Attempts) //nolint:gosec // bounded by max_attempts
	}
	tag, err := q.pool.Exec(ctx, `
		UPDATE notifications n
		SET status = 'pending', attempts = n.attempts - 1, locked_until = NULL
		FROM unnest($1::uuid[], $2::int[]) AS mine(id, attempt)
		WHERE n.id = mine.id AND n.attempts = mine.attempt AND n.status = 'processing'`,
		ids, attempts)
	if err != nil {
		return 0, fmt.Errorf("release: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ReapResult counts what the reaper did.
type ReapResult struct {
	Requeued        int
	Failed          int
	FailedByChannel map[domain.Channel]int
}

// ReapExpired is crash recovery. A row still 'processing' after its lease
// expired belongs to a worker that died or stalled, so it goes back to
// 'pending'. The provider may or may not have received it, so this is
// what makes delivery at-least-once rather than at-most-once.
//
// The attempt is not refunded (we can't know whether the provider got it),
// so a row whose budget is exhausted is failed instead of looping forever.
// Running the reaper on several worker replicas at once is safe: the UPDATE
// is idempotent and row locks serialize it.
func (q *Queue) ReapExpired(ctx context.Context) (ReapResult, error) {
	rows, err := q.pool.Query(ctx, `
		UPDATE notifications
		SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'pending' END,
		    last_error = CASE WHEN attempts >= max_attempts
		                      THEN 'lease expired on final attempt (worker crashed or stalled)'
		                      ELSE 'lease expired, re-queued (worker crashed or stalled)' END,
		    locked_until = NULL,
		    next_attempt_at = now()
		WHERE status = 'processing' AND locked_until < now()
		RETURNING status, channel`)
	if err != nil {
		return ReapResult{}, fmt.Errorf("reap: %w", err)
	}
	defer rows.Close()

	res := ReapResult{FailedByChannel: map[domain.Channel]int{}}
	for rows.Next() {
		var status domain.Status
		var channel domain.Channel
		if err := rows.Scan(&status, &channel); err != nil {
			return ReapResult{}, fmt.Errorf("reap: scan: %w", err)
		}
		if status == domain.StatusFailed {
			res.Failed++
			res.FailedByChannel[channel]++
		} else {
			res.Requeued++
		}
	}
	return res, rows.Err()
}

// qualified prefixes each column with alias ("id, status" -> "n.id, n.status").
// RETURNING needs it here because the CTE "due" also has an id column.
func qualified(alias, columns string) string {
	parts := strings.Split(columns, ",")
	for i, col := range parts {
		parts[i] = alias + "." + strings.TrimSpace(col)
	}
	return strings.Join(parts, ", ")
}

func delayMillis(d *time.Duration) *int64 {
	if d == nil {
		return nil
	}
	ms := d.Milliseconds()
	return &ms
}

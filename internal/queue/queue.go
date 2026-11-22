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

// claimTimeout bounds one claim statement (normally milliseconds).
const claimTimeout = 5 * time.Second

// Quota says how many rows one claim may take and how they are shared
// between priorities. Reserved[r] rows (r = priority rank: 0 high, 1 normal,
// 2 low) are taken from that priority first, oldest due first. Whatever is
// left of Total, including reserved slots a priority could not use, goes to
// the rest of the due backlog in strict priority order. So no slots are
// wasted, and with nothing reserved below high the claim is strict priority.
type Quota struct {
	Total    int
	Reserved [3]int
}

// Strict is a quota of n rows in strict priority order.
func Strict(n int) Quota { return Quota{Total: n} }

// Claim atomically takes up to quota.Total due notifications for one channel
// and leases them to the caller.
//
//   - FOR UPDATE SKIP LOCKED: concurrent workers skip rows another worker is
//     claiming instead of blocking on them, so N workers claim N disjoint sets.
//   - It is a single statement (one implicit transaction) that commits before
//     the caller makes any HTTP call. Row locks are held for milliseconds,
//     never for the duration of a provider request.
//   - attempts is incremented here, so each claim gets a unique attempt
//     number. That number doubles as a fencing token for Complete/Release.
//   - Each reserved part filters one priority_rank and orders by due time,
//     so every part is an ordered scan of the claim index (no sort).
//
// The returned slice is ordered high priority first, then oldest due first.
func (q *Queue) Claim(ctx context.Context, channel domain.Channel, quota Quota, lease time.Duration) ([]domain.Notification, error) {
	// A claim must not be abandoned halfway: the UPDATE commits on the
	// server even if we stop waiting for its rows, and rows we never receive
	// stay 'processing' with no owner until their lease runs out (then the
	// reaper charges them an attempt they never made). So shutdown does not
	// cancel a claim; it runs to completion, bounded by claimTimeout, and the
	// caller hands back what it got (found by the failure-hunter agent).
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimTimeout)
	defer cancel()
	// Reserved slots can never exceed the total (LIMIT must not go negative).
	left := quota.Total
	for r := range quota.Reserved {
		quota.Reserved[r] = max(0, min(quota.Reserved[r], left))
		left -= quota.Reserved[r]
	}
	const due = `
			status IN ('scheduled', 'pending', 'retrying')
			AND channel = $1
			AND next_attempt_at <= now()
			-- Never true for rows our own transitions produce. It keeps one
			-- inconsistent row from failing the whole claim on the
			-- attempts <= max_attempts constraint and blocking the channel.
			AND attempts < max_attempts`
	reserved := func(rank, limitParam int) string {
		return fmt.Sprintf(`SELECT id FROM notifications
			WHERE %s AND priority_rank = %d
			ORDER BY next_attempt_at
			LIMIT $%d
			FOR UPDATE SKIP LOCKED`, due, rank, limitParam)
	}
	rows, err := q.pool.Query(ctx, `
		WITH high AS (`+reserved(0, 3)+`),
		normal AS (`+reserved(1, 4)+`),
		low AS (`+reserved(2, 5)+`),
		picked AS (
			SELECT id FROM high UNION ALL SELECT id FROM normal UNION ALL SELECT id FROM low
		),
		-- Our own transaction already holds the picked rows' locks, so
		-- SKIP LOCKED would not skip them: exclude them explicitly.
		fill AS (
			SELECT id FROM notifications
			WHERE `+due+` AND id NOT IN (SELECT id FROM picked)
			ORDER BY priority_rank, next_attempt_at
			LIMIT $2 - (SELECT count(*) FROM picked)
			FOR UPDATE SKIP LOCKED
		)
		UPDATE notifications n
		SET status       = 'processing',
		    attempts     = n.attempts + 1,
		    locked_until = now() + $6 * interval '1 millisecond'
		WHERE n.id IN (SELECT id FROM picked UNION ALL SELECT id FROM fill)
		RETURNING `+qualified("n", store.NotificationColumns),
		channel, quota.Total, quota.Reserved[0], quota.Reserved[1], quota.Reserved[2], lease.Milliseconds())
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

// Renew confirms the caller still owns a claimed row (still 'processing'
// under its attempt number) and extends the lease for the send. The worker
// calls it right before contacting the provider: a worker that stalled past
// its lease (GC pause, slow database) while the reaper re-queued the row
// and another worker claimed it gets false, and must not send.
func (q *Queue) Renew(ctx context.Context, id uuid.UUID, attempt int, lease time.Duration) (bool, error) {
	tag, err := q.pool.Exec(ctx, `
		UPDATE notifications
		SET locked_until = now() + $3 * interval '1 millisecond'
		WHERE id = $1 AND status = 'processing' AND attempts = $2`,
		id, attempt, lease.Milliseconds())
	if err != nil {
		return false, fmt.Errorf("renew: %w", err)
	}
	return tag.RowsAffected() == 1, nil
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
// really happened, and the history should say so. If the reaper already
// wrote that attempt as 'interrupted', the real result replaces it.
func (q *Queue) Complete(ctx context.Context, c Completion) error {
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("complete: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	a := c.Attempt
	inserted, err := tx.Exec(ctx, `
		INSERT INTO delivery_attempts
			(notification_id, attempt_no, started_at, duration_ms, http_status, error, outcome)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (notification_id, attempt_no) DO UPDATE
		SET started_at = EXCLUDED.started_at, duration_ms = EXCLUDED.duration_ms,
		    http_status = EXCLUDED.http_status, error = EXCLUDED.error, outcome = EXCLUDED.outcome
		WHERE delivery_attempts.outcome = 'interrupted'`,
		c.ID, c.AttemptNo, a.StartedAt, a.DurationMS, a.HTTPStatus, a.Error, a.Outcome)
	if err != nil {
		return fmt.Errorf("complete: insert attempt: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		// Only one claim can produce an attempt number, so a real result
		// already stored for it means a bug, not a race.
		return fmt.Errorf("complete: attempt %d of %s is already recorded", c.AttemptNo, c.ID)
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
	Requeued             int
	Failed               int
	FailedByChannel      map[domain.Channel]int
	InterruptedByChannel map[domain.Channel]int // attempts recorded as interrupted
}

// ReapExpired is crash recovery. A row still 'processing' after its lease
// expired belongs to a worker that died or stalled, so it goes back to
// 'pending'. The provider may or may not have received it, so this is
// what makes delivery at-least-once rather than at-most-once.
//
// The lost call is written to delivery_attempts as 'interrupted' (started
// at the claim, which is the row's updated_at; duration unknown), so the
// history has no gap and shows that this message may have been delivered.
// If the stalled worker reports later, Complete replaces it with the real
// result.
//
// The attempt is not refunded (we can't know whether the provider got it),
// so a row whose budget is exhausted is failed instead of looping forever.
// Running the reaper on several worker replicas at once is safe: SKIP
// LOCKED gives each row to one reaper, and the status check makes a repeat
// a no-op.
func (q *Queue) ReapExpired(ctx context.Context) (ReapResult, error) {
	rows, err := q.pool.Query(ctx, `
		WITH expired AS (
			SELECT id, attempts, updated_at AS claimed_at
			FROM notifications
			WHERE status = 'processing' AND locked_until < now()
			FOR UPDATE SKIP LOCKED
		), reaped AS (
			UPDATE notifications n
			SET status = CASE WHEN n.attempts >= n.max_attempts THEN 'failed' ELSE 'pending' END,
			    last_error = CASE WHEN n.attempts >= n.max_attempts
			                      THEN 'lease expired on final attempt (worker crashed or stalled)'
			                      ELSE 'lease expired, re-queued (worker crashed or stalled)' END,
			    locked_until = NULL,
			    next_attempt_at = now()
			FROM expired e
			WHERE n.id = e.id
			RETURNING n.id, n.status, n.channel, e.attempts, e.claimed_at
		), interrupted AS (
			INSERT INTO delivery_attempts (notification_id, attempt_no, started_at, error, outcome)
			SELECT id, attempts, claimed_at,
			       'worker lease expired before the result was recorded; the provider may have received it',
			       'interrupted'
			FROM reaped
			WHERE attempts > 0
			ON CONFLICT (notification_id, attempt_no) DO NOTHING
			RETURNING notification_id
		)
		SELECT r.status, r.channel, i.notification_id IS NOT NULL
		FROM reaped r LEFT JOIN interrupted i ON i.notification_id = r.id`)
	if err != nil {
		return ReapResult{}, fmt.Errorf("reap: %w", err)
	}
	defer rows.Close()

	res := ReapResult{FailedByChannel: map[domain.Channel]int{}, InterruptedByChannel: map[domain.Channel]int{}}
	for rows.Next() {
		var status domain.Status
		var channel domain.Channel
		var interrupted bool
		if err := rows.Scan(&status, &channel, &interrupted); err != nil {
			return ReapResult{}, fmt.Errorf("reap: scan: %w", err)
		}
		if status == domain.StatusFailed {
			res.Failed++
			res.FailedByChannel[channel]++
		} else {
			res.Requeued++
		}
		if interrupted {
			res.InterruptedByChannel[channel]++
		}
	}
	return res, rows.Err()
}

// qualified prefixes each column with alias ("id, status" -> "n.id, n.status").
// RETURNING needs it here because the claim's CTEs also have an id column.
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

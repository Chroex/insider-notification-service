// Package store contains hand-written pgx repositories. SQL is kept in
// plain strings next to the code that uses it so reviewers can read the
// exact queries, including the conditional UPDATEs that make status
// transitions race-free.
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"notification-service/internal/domain"
)

// Store wraps a pgx pool. It is safe for concurrent use.
type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// NotificationColumns and ScanNotification are exported so the queue
// package reads rows with exactly the same column order.
const NotificationColumns = `id, batch_id, idempotency_key, request_hash, correlation_id,
	channel, priority, recipient, content, status, attempts, max_attempts,
	next_attempt_at, scheduled_at, locked_until, last_error, provider_message_id,
	created_at, updated_at, sent_at`

func ScanNotification(row pgx.Row) (domain.Notification, error) {
	var n domain.Notification
	err := row.Scan(
		&n.ID, &n.BatchID, &n.IdempotencyKey, &n.RequestHash, &n.CorrelationID,
		&n.Channel, &n.Priority, &n.Recipient, &n.Content, &n.Status, &n.Attempts, &n.MaxAttempts,
		&n.NextAttemptAt, &n.ScheduledAt, &n.LockedUntil, &n.LastError, &n.ProviderMessageID,
		&n.CreatedAt, &n.UpdatedAt, &n.SentAt,
	)
	return n, err
}

func collectNotifications(rows pgx.Rows) ([]domain.Notification, error) {
	defer rows.Close()
	var out []domain.Notification
	for rows.Next() {
		n, err := ScanNotification(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// InsertNotification inserts n. If n has an idempotency key that already
// exists, nothing is inserted and the existing row is returned with
// created=false; the caller decides whether that is a replay or a conflict.
//
// ON CONFLICT DO NOTHING (rather than SELECT-then-INSERT) closes the race
// where two concurrent requests with the same key both see "no row" and both
// insert: the unique index arbitrates, and the loser reads the winner's row.
func (s *Store) InsertNotification(ctx context.Context, n domain.Notification) (domain.Notification, bool, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO notifications (id, idempotency_key, request_hash, correlation_id,
			channel, priority, recipient, content, status, max_attempts,
			next_attempt_at, scheduled_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+NotificationColumns,
		n.ID, n.IdempotencyKey, n.RequestHash, n.CorrelationID,
		n.Channel, n.Priority, n.Recipient, n.Content, n.Status, n.MaxAttempts,
		n.NextAttemptAt, n.ScheduledAt,
	)
	created, err := ScanNotification(row)
	if err == nil {
		return created, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) || n.IdempotencyKey == nil {
		return domain.Notification{}, false, fmt.Errorf("insert notification: %w", err)
	}

	existing, err := ScanNotification(s.pool.QueryRow(ctx,
		`SELECT `+NotificationColumns+` FROM notifications WHERE idempotency_key = $1`, *n.IdempotencyKey))
	if err != nil {
		return domain.Notification{}, false, fmt.Errorf("load notification by idempotency key: %w", err)
	}
	return existing, false, nil
}

// GetNotification returns domain.ErrNotFound when id does not exist.
func (s *Store) GetNotification(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	n, err := ScanNotification(s.pool.QueryRow(ctx,
		`SELECT `+NotificationColumns+` FROM notifications WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Notification{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Notification{}, fmt.Errorf("get notification: %w", err)
	}
	return n, nil
}

// ListAttempts returns the delivery history of one notification, oldest first.
func (s *Store) ListAttempts(ctx context.Context, notificationID uuid.UUID) ([]domain.DeliveryAttempt, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, notification_id, attempt_no, started_at, duration_ms, http_status, error, outcome
		FROM delivery_attempts
		WHERE notification_id = $1
		ORDER BY attempt_no`, notificationID)
	if err != nil {
		return nil, fmt.Errorf("list attempts: %w", err)
	}
	defer rows.Close()

	attempts := []domain.DeliveryAttempt{}
	for rows.Next() {
		var a domain.DeliveryAttempt
		if err := rows.Scan(&a.ID, &a.NotificationID, &a.AttemptNo, &a.StartedAt,
			&a.DurationMS, &a.HTTPStatus, &a.Error, &a.Outcome); err != nil {
			return nil, fmt.Errorf("scan attempt: %w", err)
		}
		attempts = append(attempts, a)
	}
	return attempts, rows.Err()
}

// ListFilter selects notifications for GET /notifications. Nil fields are
// not filtered on.
type ListFilter struct {
	Status  *domain.Status
	Channel *domain.Channel
	From    *time.Time // created_at >= From
	To      *time.Time // created_at <  To
	Limit   int
	After   *Cursor
}

// ListNotifications returns up to Limit rows, newest first, and the cursor for
// the next page (nil when there are no more rows).
func (s *Store) ListNotifications(ctx context.Context, f ListFilter) ([]domain.Notification, *Cursor, error) {
	// Only placeholders are interpolated into the SQL text; values always go
	// through args, so this dynamic WHERE clause is injection-safe.
	var where []string
	var args []any
	addCond := func(cond string, arg any) {
		args = append(args, arg)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if f.Status != nil {
		addCond("status = $%d", *f.Status)
	}
	if f.Channel != nil {
		addCond("channel = $%d", *f.Channel)
	}
	if f.From != nil {
		addCond("created_at >= $%d", *f.From)
	}
	if f.To != nil {
		addCond("created_at < $%d", *f.To)
	}
	if f.After != nil {
		args = append(args, f.After.CreatedAt, f.After.ID)
		// Row-value comparison matches the (created_at DESC, id DESC) index order.
		where = append(where, fmt.Sprintf("(created_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}

	query := `SELECT ` + NotificationColumns + ` FROM notifications`
	if len(where) > 0 {
		query += ` WHERE ` + strings.Join(where, " AND ")
	}
	// Fetch one extra row to learn whether another page exists without a COUNT.
	args = append(args, f.Limit+1)
	query += fmt.Sprintf(` ORDER BY created_at DESC, id DESC LIMIT $%d`, len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("list notifications: %w", err)
	}
	items, err := collectNotifications(rows)
	if err != nil {
		return nil, nil, fmt.Errorf("list notifications: %w", err)
	}
	items, next := pageOf(items, f.Limit)
	return items, next, nil
}

// pageOf trims the look-ahead row and builds the next cursor.
func pageOf(items []domain.Notification, limit int) ([]domain.Notification, *Cursor) {
	if items == nil {
		items = []domain.Notification{}
	}
	if len(items) <= limit {
		return items, nil
	}
	items = items[:limit]
	last := items[len(items)-1]
	return items, &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
}

// CancelNotification moves a notification to cancelled only if it is still
// in a cancellable status. Doing the check inside the UPDATE's WHERE clause
// makes it atomic: if a worker claims the row first, this UPDATE matches zero
// rows instead of cancelling a message that is already being sent.
func (s *Store) CancelNotification(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	n, err := ScanNotification(s.pool.QueryRow(ctx, `
		UPDATE notifications
		SET status = 'cancelled', locked_until = NULL
		WHERE id = $1 AND status IN ('scheduled', 'pending', 'retrying')
		RETURNING `+NotificationColumns, id))
	if err == nil {
		return n, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Notification{}, fmt.Errorf("cancel notification: %w", err)
	}

	// Zero rows: either it does not exist or it is not cancellable.
	current, err := s.GetNotification(ctx, id)
	if err != nil {
		return domain.Notification{}, err
	}
	return domain.Notification{}, &domain.InvalidTransitionError{From: current.Status, Action: "cancel"}
}

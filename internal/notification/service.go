// Package notification is the application service behind the HTTP API. It
// validates input, applies defaults, enforces idempotency and turns requests
// into domain.Notification rows. It never talks to the provider: creating a
// notification only enqueues it.
package notification

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"notification-service/internal/domain"
	"notification-service/internal/store"
	"notification-service/internal/validation"
)

// Store is the persistence the service needs. Defined here (consumer side)
// so tests can substitute a fake without touching Postgres.
type Store interface {
	InsertNotification(ctx context.Context, n domain.Notification) (domain.Notification, bool, error)
	GetNotification(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	ListAttempts(ctx context.Context, id uuid.UUID) ([]domain.DeliveryAttempt, error)
	ListNotifications(ctx context.Context, f store.ListFilter) ([]domain.Notification, *store.Cursor, error)
	CancelNotification(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	RetryNotification(ctx context.Context, id uuid.UUID, extraAttempts int) (domain.Notification, error)
	InsertBatch(ctx context.Context, b domain.Batch, items []domain.Notification) (domain.Batch, bool, error)
	GetBatchSummary(ctx context.Context, id uuid.UUID) (domain.BatchSummary, error)
	ListBatchItems(ctx context.Context, batchID uuid.UUID, limit int, after *store.Cursor) ([]domain.Notification, *store.Cursor, error)
	ListBatchItemIDs(ctx context.Context, batchID uuid.UUID) ([]uuid.UUID, error)
}

// Config holds the service's limits.
type Config struct {
	Rules        validation.Rules
	BatchMaxSize int
	MaxAttempts  int
}

type Service struct {
	store Store
	cfg   Config
	now   func() time.Time
	newID func() uuid.UUID
}

func NewService(st Store, cfg Config) *Service {
	return &Service{store: st, cfg: cfg, now: time.Now, newID: newUUIDv7}
}

// newUUIDv7 returns a time-ordered UUID. Compared to random v4 ids, v7 ids
// are inserted at the right edge of the primary-key B-tree (better cache
// locality, fewer page splits) and sort in creation order.
func newUUIDv7() uuid.UUID {
	return uuid.Must(uuid.NewV7())
}

// CreateInput is one notification request, before defaults are applied.
type CreateInput struct {
	Channel     string
	Recipient   string
	Content     string
	Priority    string
	ScheduledAt *time.Time
}

// Create validates and enqueues one notification. created is false when the
// request was an idempotent replay and the original row is returned.
func (s *Service) Create(ctx context.Context, in CreateInput, idempotencyKey, correlationID string) (n domain.Notification, created bool, err error) {
	key, err := normalizeKey(idempotencyKey)
	if err != nil {
		return domain.Notification{}, false, err
	}
	if fieldErrs := s.cfg.Rules.Validate(toValidationRequest(in), s.now()); len(fieldErrs) > 0 {
		return domain.Notification{}, false, &domain.ValidationError{Fields: fieldErrs}
	}

	n = s.build(in, correlationID)
	if key != nil {
		hash := requestHash("notification", canonicalize(in))
		n.IdempotencyKey, n.RequestHash = key, &hash
	}

	stored, created, err := s.store.InsertNotification(ctx, n)
	if err != nil {
		return domain.Notification{}, false, err
	}
	if !created {
		if err := checkReplay(stored.RequestHash, *n.RequestHash); err != nil {
			return domain.Notification{}, false, err
		}
	}
	return stored, created, nil
}

// build turns a validated input into a row ready to insert. A scheduled
// notification becomes claimable at scheduled_at; everything else now.
func (s *Service) build(in CreateInput, correlationID string) domain.Notification {
	now := s.now()
	n := domain.Notification{
		ID:            s.newID(),
		CorrelationID: correlationID,
		Channel:       domain.Channel(in.Channel),
		Priority:      effectivePriority(in.Priority),
		Recipient:     in.Recipient,
		Content:       in.Content,
		Status:        domain.StatusPending,
		MaxAttempts:   s.cfg.MaxAttempts,
		NextAttemptAt: now,
	}
	if in.ScheduledAt != nil {
		at := in.ScheduledAt.UTC()
		n.Status = domain.StatusScheduled
		n.ScheduledAt = &at
		n.NextAttemptAt = at
	}
	return n
}

// BatchResult is the outcome of a batch create. IDs are in request order.
type BatchResult struct {
	Batch domain.Batch
	IDs   []uuid.UUID
}

// CreateBatch validates every item first and rejects the whole batch if any
// item is invalid (all failing indexes are reported). Partial acceptance
// would force clients to diff the response to learn which items to resend;
// all-or-nothing keeps retries simple and matches the single transaction.
func (s *Service) CreateBatch(ctx context.Context, items []CreateInput, idempotencyKey, correlationID string) (BatchResult, bool, error) {
	key, err := normalizeKey(idempotencyKey)
	if err != nil {
		return BatchResult{}, false, err
	}
	if err := s.validateBatch(items); err != nil {
		return BatchResult{}, false, err
	}

	batch := domain.Batch{ID: s.newID(), Total: len(items)}
	rows := make([]domain.Notification, len(items))
	ids := make([]uuid.UUID, len(items))
	for i, in := range items {
		rows[i] = s.build(in, correlationID)
		rows[i].BatchID = &batch.ID
		ids[i] = rows[i].ID
	}
	if key != nil {
		canonical := make([]canonicalItem, len(items))
		for i, in := range items {
			canonical[i] = canonicalize(in)
		}
		hash := requestHash("batch", canonical)
		batch.IdempotencyKey, batch.RequestHash = key, &hash
	}

	stored, created, err := s.store.InsertBatch(ctx, batch, rows)
	if err != nil {
		return BatchResult{}, false, err
	}
	if created {
		return BatchResult{Batch: stored, IDs: ids}, true, nil
	}

	if err := checkReplay(stored.RequestHash, *batch.RequestHash); err != nil {
		return BatchResult{}, false, err
	}
	originalIDs, err := s.store.ListBatchItemIDs(ctx, stored.ID)
	if err != nil {
		return BatchResult{}, false, err
	}
	return BatchResult{Batch: stored, IDs: originalIDs}, false, nil
}

func (s *Service) validateBatch(items []CreateInput) error {
	if len(items) == 0 || len(items) > s.cfg.BatchMaxSize {
		return &domain.ValidationError{Fields: []domain.FieldError{{
			Field:   "notifications",
			Message: fmt.Sprintf("must contain between 1 and %d items, got %d", s.cfg.BatchMaxSize, len(items)),
		}}}
	}
	now := s.now()
	var all []domain.FieldError
	for i, in := range items {
		for _, fe := range s.cfg.Rules.Validate(toValidationRequest(in), now) {
			fe.Index = &i
			all = append(all, fe)
		}
	}
	if len(all) > 0 {
		return &domain.ValidationError{Fields: all}
	}
	return nil
}

// Get returns a notification with its delivery attempts.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (domain.Notification, []domain.DeliveryAttempt, error) {
	n, err := s.store.GetNotification(ctx, id)
	if err != nil {
		return domain.Notification{}, nil, err
	}
	attempts, err := s.store.ListAttempts(ctx, id)
	if err != nil {
		return domain.Notification{}, nil, err
	}
	return n, attempts, nil
}

// List pages through notifications, newest first.
func (s *Service) List(ctx context.Context, f store.ListFilter) ([]domain.Notification, *store.Cursor, error) {
	return s.store.ListNotifications(ctx, f)
}

// Cancel cancels a scheduled, pending or retrying notification.
func (s *Service) Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	return s.store.CancelNotification(ctx, id)
}

// Retry re-queues a failed notification with a fresh attempt budget.
// Failed notifications are the dead-letter set; this is the manual
// "redrive" for them once the cause (bad config, provider outage) is fixed.
func (s *Service) Retry(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	return s.store.RetryNotification(ctx, id, s.cfg.MaxAttempts)
}

// GetBatch returns a batch summary and one page of its items.
func (s *Service) GetBatch(ctx context.Context, id uuid.UUID, limit int, after *store.Cursor) (domain.BatchSummary, []domain.Notification, *store.Cursor, error) {
	summary, err := s.store.GetBatchSummary(ctx, id)
	if err != nil {
		return domain.BatchSummary{}, nil, nil, err
	}
	items, next, err := s.store.ListBatchItems(ctx, id, limit, after)
	if err != nil {
		return domain.BatchSummary{}, nil, nil, err
	}
	return summary, items, next, nil
}

func toValidationRequest(in CreateInput) validation.Request {
	return validation.Request{
		Channel:     in.Channel,
		Recipient:   in.Recipient,
		Content:     in.Content,
		Priority:    in.Priority,
		ScheduledAt: in.ScheduledAt,
	}
}

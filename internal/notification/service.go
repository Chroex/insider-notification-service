// Package notification is the application service behind the HTTP API. It
// validates input, applies defaults, enforces idempotency and turns requests
// into domain.Notification rows. It never talks to the provider: creating a
// notification only enqueues it.
package notification

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"notification-service/internal/domain"
	"notification-service/internal/observability"
	"notification-service/internal/store"
	"notification-service/internal/validation"
)

// Store is the persistence the service needs. Defined here (consumer side)
// so tests can substitute a fake without touching Postgres.
type Store interface {
	InsertNotification(ctx context.Context, n domain.Notification) (domain.Notification, bool, error)
	GetNotificationByIdempotencyKey(ctx context.Context, key string) (domain.Notification, error)
	GetNotification(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	ListAttempts(ctx context.Context, id uuid.UUID) ([]domain.DeliveryAttempt, error)
	ListNotifications(ctx context.Context, f store.ListFilter) ([]domain.Notification, *store.Cursor, error)
	CancelNotification(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	RetryNotification(ctx context.Context, id uuid.UUID, extraAttempts int) (domain.Notification, error)
	InsertBatch(ctx context.Context, b domain.Batch, items []domain.Notification) (domain.Batch, bool, error)
	GetBatchByIdempotencyKey(ctx context.Context, key string) (domain.Batch, error)
	GetBatchSummary(ctx context.Context, id uuid.UUID) (domain.BatchSummary, error)
	ListBatchItems(ctx context.Context, batchID uuid.UUID, limit int, after *store.Cursor) ([]domain.Notification, *store.Cursor, error)
	ListBatchItemIDs(ctx context.Context, batchID uuid.UUID) ([]uuid.UUID, error)
	Summary(ctx context.Context, since time.Time) (store.Summary, error)
	InsertTemplate(ctx context.Context, t domain.Template) (domain.Template, error)
	GetTemplate(ctx context.Context, id uuid.UUID) (domain.Template, error)
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
	// TemplateID + Variables replace Content: the template is rendered at
	// create time and the result stored as content.
	TemplateID *uuid.UUID
	Variables  map[string]any
}

// Create validates and enqueues one notification. created is false when the
// request was an idempotent replay and the original row is returned.
func (s *Service) Create(ctx context.Context, in CreateInput, idempotencyKey, correlationID string) (n domain.Notification, created bool, err error) {
	key, err := normalizeKey(idempotencyKey)
	if err != nil {
		return domain.Notification{}, false, err
	}
	var hash string
	if key != nil {
		// Hash what the client sent, before validation renders templates.
		hash = requestHash("notification", canonicalize(in))
		existing, err := s.store.GetNotificationByIdempotencyKey(ctx, *key)
		switch {
		case err == nil: // replay: answered from what is stored, not re-validated
			if err := checkReplay(existing.RequestHash, hash); err != nil {
				return domain.Notification{}, false, err
			}
			return existing, false, nil
		case !errors.Is(err, domain.ErrNotFound):
			return domain.Notification{}, false, err
		}
	}

	fieldErrs, err := s.validateInput(ctx, &in, templateCache{})
	if err != nil {
		return domain.Notification{}, false, err
	}
	if len(fieldErrs) > 0 {
		return domain.Notification{}, false, &domain.ValidationError{Fields: fieldErrs}
	}

	n = s.build(in, correlationID)
	n.TraceParent = observability.TraceParent(ctx)
	if key != nil {
		n.IdempotencyKey, n.RequestHash = key, &hash
	}

	stored, created, err := s.store.InsertNotification(ctx, n)
	if err != nil {
		return domain.Notification{}, false, err
	}
	if !created { // a concurrent request with the same key won the insert
		if err := checkReplay(stored.RequestHash, hash); err != nil {
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
		Priority:      EffectivePriority(in.Priority),
		Recipient:     in.Recipient,
		Content:       in.Content,
		TemplateID:    in.TemplateID,
		Variables:     in.Variables,
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
	var hash string
	if key != nil {
		// Hash what the client sent, before validation renders templates.
		canonical := make([]canonicalItem, len(items))
		for i, in := range items {
			canonical[i] = canonicalize(in)
		}
		hash = requestHash("batch", canonical)
		existing, err := s.store.GetBatchByIdempotencyKey(ctx, *key)
		switch {
		case err == nil: // replay: answered from what is stored, not re-validated
			return s.replayBatch(ctx, existing, hash)
		case !errors.Is(err, domain.ErrNotFound):
			return BatchResult{}, false, err
		}
	}
	if err := s.validateBatch(ctx, items); err != nil {
		return BatchResult{}, false, err
	}

	batch := domain.Batch{ID: s.newID(), Total: len(items)}
	rows := make([]domain.Notification, len(items))
	ids := make([]uuid.UUID, len(items))
	traceParent := observability.TraceParent(ctx)
	for i, in := range items {
		rows[i] = s.build(in, correlationID)
		rows[i].BatchID = &batch.ID
		rows[i].TraceParent = traceParent
		ids[i] = rows[i].ID
	}
	if key != nil {
		batch.IdempotencyKey, batch.RequestHash = key, &hash
	}

	stored, created, err := s.store.InsertBatch(ctx, batch, rows)
	if err != nil {
		return BatchResult{}, false, err
	}
	if created {
		return BatchResult{Batch: stored, IDs: ids}, true, nil
	}
	return s.replayBatch(ctx, stored, hash) // a concurrent request with the same key won
}

// replayBatch answers a repeated batch request with the original batch and
// its item ids in request order, or a conflict if the payload differs.
func (s *Service) replayBatch(ctx context.Context, stored domain.Batch, hash string) (BatchResult, bool, error) {
	if err := checkReplay(stored.RequestHash, hash); err != nil {
		return BatchResult{}, false, err
	}
	originalIDs, err := s.store.ListBatchItemIDs(ctx, stored.ID)
	if err != nil {
		return BatchResult{}, false, err
	}
	return BatchResult{Batch: stored, IDs: originalIDs}, false, nil
}

// validateBatch renders templates in place (items[i].Content) and reports
// every failing item by index.
func (s *Service) validateBatch(ctx context.Context, items []CreateInput) error {
	if len(items) == 0 || len(items) > s.cfg.BatchMaxSize {
		return &domain.ValidationError{Fields: []domain.FieldError{{
			Field:   "notifications",
			Message: fmt.Sprintf("must contain between 1 and %d items, got %d", s.cfg.BatchMaxSize, len(items)),
		}}}
	}
	cache := templateCache{}
	var all []domain.FieldError
	for i := range items {
		errs, err := s.validateInput(ctx, &items[i], cache)
		if err != nil {
			return err
		}
		for _, fe := range errs {
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

// Summary returns queue depth and delivery statistics for the last window.
func (s *Service) Summary(ctx context.Context, window time.Duration) (store.Summary, error) {
	return s.store.Summary(ctx, s.now().Add(-window))
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

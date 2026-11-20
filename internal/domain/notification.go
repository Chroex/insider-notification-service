package domain

import (
	"time"

	"github.com/google/uuid"
)

// Notification is one message to one recipient on one channel.
type Notification struct {
	ID                uuid.UUID
	BatchID           *uuid.UUID
	IdempotencyKey    *string
	RequestHash       *string
	CorrelationID     string
	TraceParent       *string // W3C traceparent of the creating request (tracing)
	Channel           Channel
	Priority          Priority
	Recipient         string
	Content           string // final text; rendered at create time when a template is used
	TemplateID        *uuid.UUID
	Variables         map[string]any
	Status            Status
	Attempts          int
	MaxAttempts       int
	NextAttemptAt     time.Time
	ScheduledAt       *time.Time
	LockedUntil       *time.Time
	LastError         *string
	ProviderMessageID *string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	SentAt            *time.Time
}

// AttemptOutcome classifies one provider call.
type AttemptOutcome string

const (
	OutcomeSuccess        AttemptOutcome = "success"
	OutcomeRetryableError AttemptOutcome = "retryable_error"
	OutcomePermanentError AttemptOutcome = "permanent_error"
)

// DeliveryAttempt records one provider call for the audit trail.
type DeliveryAttempt struct {
	ID             int64
	NotificationID uuid.UUID
	AttemptNo      int
	StartedAt      time.Time
	DurationMS     int
	HTTPStatus     *int
	Error          *string
	Outcome        AttemptOutcome
}

// Batch groups notifications created by a single batch request.
type Batch struct {
	ID             uuid.UUID
	Total          int
	IdempotencyKey *string
	RequestHash    *string
	CreatedAt      time.Time
}

// BatchSummary is a batch plus live per-status counts of its notifications.
type BatchSummary struct {
	Batch
	StatusCounts map[Status]int
}

// QueueBucket identifies one queue-depth series: how many notifications of a
// channel and priority are currently in a non-terminal status.
type QueueBucket struct {
	Channel  Channel
	Priority Priority
	Status   Status
}

// ActiveStatuses are the non-terminal statuses that make up queue depth.
var ActiveStatuses = []Status{StatusScheduled, StatusPending, StatusProcessing, StatusRetrying}

// Template is a reusable message body (Go text/template syntax). Templates
// are immutable once created, so a notification rendered from one never
// changes meaning later.
type Template struct {
	ID        uuid.UUID
	Name      string
	Channel   Channel
	Body      string
	CreatedAt time.Time
}

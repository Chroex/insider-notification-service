package httpapi

import (
	"time"

	"github.com/google/uuid"

	"notification-service/internal/domain"
	"notification-service/internal/notification"
)

// Request and response bodies. Kept separate from domain types so the API
// contract (api/openapi.yaml) can evolve independently of storage.

type createNotificationRequest struct {
	Channel     string         `json:"channel"`
	Recipient   string         `json:"recipient"`
	Content     string         `json:"content"`
	Priority    string         `json:"priority,omitempty"`
	ScheduledAt *time.Time     `json:"scheduled_at,omitempty"`
	TemplateID  *uuid.UUID     `json:"template_id,omitempty"`
	Variables   map[string]any `json:"variables,omitempty"`
}

func (r createNotificationRequest) toInput() notification.CreateInput {
	return notification.CreateInput{
		Channel:     r.Channel,
		Recipient:   r.Recipient,
		Content:     r.Content,
		Priority:    r.Priority,
		ScheduledAt: r.ScheduledAt,
		TemplateID:  r.TemplateID,
		Variables:   r.Variables,
	}
}

type createBatchRequest struct {
	Notifications []createNotificationRequest `json:"notifications"`
}

type notificationResponse struct {
	ID                uuid.UUID      `json:"id"`
	BatchID           *uuid.UUID     `json:"batch_id,omitempty"`
	Channel           string         `json:"channel"`
	Priority          string         `json:"priority"`
	Recipient         string         `json:"recipient"`
	Content           string         `json:"content"`
	TemplateID        *uuid.UUID     `json:"template_id,omitempty"`
	Variables         map[string]any `json:"variables,omitempty"`
	Status            string         `json:"status"`
	Attempts          int            `json:"attempts"`
	MaxAttempts       int            `json:"max_attempts"`
	ScheduledAt       *time.Time     `json:"scheduled_at,omitempty"`
	NextAttemptAt     *time.Time     `json:"next_attempt_at,omitempty"`
	LastError         *string        `json:"last_error,omitempty"`
	ProviderMessageID *string        `json:"provider_message_id,omitempty"`
	IdempotencyKey    *string        `json:"idempotency_key,omitempty"`
	CorrelationID     string         `json:"correlation_id"`
	CreatedAt         time.Time      `json:"created_at"`
	UpdatedAt         time.Time      `json:"updated_at"`
	SentAt            *time.Time     `json:"sent_at,omitempty"`
}

func toNotificationResponse(n domain.Notification) notificationResponse {
	resp := notificationResponse{
		ID:                n.ID,
		BatchID:           n.BatchID,
		Channel:           string(n.Channel),
		Priority:          string(n.Priority),
		Recipient:         n.Recipient,
		Content:           n.Content,
		TemplateID:        n.TemplateID,
		Variables:         n.Variables,
		Status:            string(n.Status),
		Attempts:          n.Attempts,
		MaxAttempts:       n.MaxAttempts,
		ScheduledAt:       utcPtr(n.ScheduledAt),
		LastError:         n.LastError,
		ProviderMessageID: n.ProviderMessageID,
		IdempotencyKey:    n.IdempotencyKey,
		CorrelationID:     n.CorrelationID,
		CreatedAt:         n.CreatedAt.UTC(),
		UpdatedAt:         n.UpdatedAt.UTC(),
		SentAt:            utcPtr(n.SentAt),
	}
	// next_attempt_at only means something while the notification is queued.
	if n.Status.Cancellable() {
		resp.NextAttemptAt = utcPtr(&n.NextAttemptAt)
	}
	return resp
}

// utcPtr normalizes API timestamps to UTC. pgx returns timestamptz in the
// process's local zone, which would make responses depend on the host's TZ.
func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

func toNotificationResponses(ns []domain.Notification) []notificationResponse {
	out := make([]notificationResponse, len(ns))
	for i, n := range ns {
		out[i] = toNotificationResponse(n)
	}
	return out
}

type attemptResponse struct {
	AttemptNo  int       `json:"attempt_no"`
	StartedAt  time.Time `json:"started_at"`
	DurationMS *int      `json:"duration_ms,omitempty"`
	HTTPStatus *int      `json:"http_status,omitempty"`
	Error      *string   `json:"error,omitempty"`
	Outcome    string    `json:"outcome"`
}

type notificationDetailResponse struct {
	notificationResponse
	DeliveryAttempts []attemptResponse `json:"delivery_attempts"`
}

func toDetailResponse(n domain.Notification, attempts []domain.DeliveryAttempt) notificationDetailResponse {
	out := notificationDetailResponse{
		notificationResponse: toNotificationResponse(n),
		DeliveryAttempts:     make([]attemptResponse, len(attempts)),
	}
	for i, a := range attempts {
		out.DeliveryAttempts[i] = attemptResponse{
			AttemptNo:  a.AttemptNo,
			StartedAt:  a.StartedAt.UTC(),
			DurationMS: a.DurationMS,
			HTTPStatus: a.HTTPStatus,
			Error:      a.Error,
			Outcome:    string(a.Outcome),
		}
	}
	return out
}

// listResponse is the envelope for every paginated list. next_cursor is
// null on the last page.
type listResponse[T any] struct {
	Data       []T     `json:"data"`
	NextCursor *string `json:"next_cursor"`
}

type batchItemRef struct {
	Index int       `json:"index"`
	ID    uuid.UUID `json:"id"`
}

type createBatchResponse struct {
	BatchID   uuid.UUID      `json:"batch_id"`
	Total     int            `json:"total"`
	CreatedAt time.Time      `json:"created_at"`
	Items     []batchItemRef `json:"items"`
}

type batchResponse struct {
	ID           uuid.UUID              `json:"id"`
	Total        int                    `json:"total"`
	CreatedAt    time.Time              `json:"created_at"`
	StatusCounts map[string]int         `json:"status_counts"`
	Items        []notificationResponse `json:"items"`
	NextCursor   *string                `json:"next_cursor"`
}

func toBatchResponse(s domain.BatchSummary, items []domain.Notification, next *string) batchResponse {
	// Always list every status (zero included) so dashboards get a stable shape.
	counts := make(map[string]int, len(domain.Statuses))
	for _, st := range domain.Statuses {
		counts[string(st)] = s.StatusCounts[st]
	}
	return batchResponse{
		ID:           s.ID,
		Total:        s.Total,
		CreatedAt:    s.CreatedAt.UTC(),
		StatusCounts: counts,
		Items:        toNotificationResponses(items),
		NextCursor:   next,
	}
}

type createTemplateRequest struct {
	Name    string `json:"name"`
	Channel string `json:"channel"`
	Body    string `json:"body"`
}

type templateResponse struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Channel   string    `json:"channel"`
	Body      string    `json:"body"`
	Variables []string  `json:"variables"`
	CreatedAt time.Time `json:"created_at"`
}

func toTemplateResponse(t notification.TemplateView) templateResponse {
	vars := t.Variables
	if vars == nil {
		vars = []string{}
	}
	return templateResponse{
		ID: t.ID, Name: t.Name, Channel: string(t.Channel), Body: t.Body,
		Variables: vars, CreatedAt: t.CreatedAt.UTC(),
	}
}

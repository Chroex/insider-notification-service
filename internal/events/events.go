// Package events publishes notification status changes to Redis pub/sub,
// where every API replica picks them up and streams them to WebSocket
// clients (internal/ws).
//
// Pub/sub is fire-and-forget (at-most-once): a subscriber that is down or
// slow misses events. That is fine for a live view; clients that need the
// truth (e.g. after reconnecting) read GET /api/v1/notifications/{id}.
// Publishing therefore never fails the operation that triggered it.
package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"notification-service/internal/domain"
)

// Channel is the Redis pub/sub channel all status events go to.
const Channel = "notifications:status"

// Event is one status change, sent to WebSocket clients as JSON.
type Event struct {
	NotificationID uuid.UUID  `json:"notification_id"`
	BatchID        *uuid.UUID `json:"batch_id,omitempty"`
	Channel        string     `json:"channel"`
	Status         string     `json:"status"`
	Attempt        int        `json:"attempt"`
	Error          string     `json:"error,omitempty"`
	CorrelationID  string     `json:"correlation_id,omitempty"`
	At             time.Time  `json:"at"`
}

// FromNotification builds an event describing n's current status.
func FromNotification(n domain.Notification, at time.Time) Event {
	ev := Event{
		NotificationID: n.ID,
		BatchID:        n.BatchID,
		Channel:        string(n.Channel),
		Status:         string(n.Status),
		Attempt:        n.Attempts,
		CorrelationID:  n.CorrelationID,
		At:             at.UTC(),
	}
	// Only outcomes carry an error; a new "processing" attempt must not
	// repeat the previous attempt's error as if it were current.
	if n.LastError != nil && (n.Status == domain.StatusRetrying || n.Status == domain.StatusFailed) {
		ev.Error = *n.LastError
	}
	return ev
}

// Publisher sends events; safe for concurrent use.
type Publisher interface {
	Publish(ctx context.Context, evs ...Event)
}

// RedisPublisher publishes to Redis. Several events go in one pipeline
// (one round trip), e.g. a 1000-item batch.
type RedisPublisher struct {
	rdb redis.UniversalClient
	log *slog.Logger
}

func NewRedisPublisher(rdb redis.UniversalClient, log *slog.Logger) *RedisPublisher {
	return &RedisPublisher{rdb: rdb, log: log}
}

// publishTimeout keeps a slow Redis from delaying the API response or the
// worker's next send.
const publishTimeout = 500 * time.Millisecond

func (p *RedisPublisher) Publish(ctx context.Context, evs ...Event) {
	if len(evs) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	_, err := p.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, ev := range evs {
			raw, err := json.Marshal(ev)
			if err != nil {
				return err
			}
			pipe.Publish(ctx, Channel, raw)
		}
		return nil
	})
	if err != nil {
		p.log.Warn("publishing status events failed (live stream only; state is in Postgres)",
			"error", err, "events", len(evs))
	}
}

// Nop discards events.
type Nop struct{}

func (Nop) Publish(context.Context, ...Event) {}

// Package ws streams notification status events to WebSocket clients.
//
// One subscription to Redis per API process feeds an in-memory hub, which
// fans each event out to the connected clients whose filter matches.
package ws

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"notification-service/internal/events"
)

// clientBuffer is how many events a client may fall behind before it is
// disconnected. Dropping one slow client is better than letting it block
// delivery to everyone else.
const clientBuffer = 256

// Filter selects which events a client receives; zero value = everything.
type Filter struct {
	NotificationID *uuid.UUID
	BatchID        *uuid.UUID
}

func (f Filter) matches(ev events.Event) bool {
	if f.NotificationID != nil && ev.NotificationID != *f.NotificationID {
		return false
	}
	if f.BatchID != nil && (ev.BatchID == nil || *ev.BatchID != *f.BatchID) {
		return false
	}
	return true
}

type client struct {
	filter Filter
	send   chan []byte
	// slow is closed (once) when the client fell too far behind.
	slow     chan struct{}
	slowOnce sync.Once
}

// Hub fans events out to clients. Safe for concurrent use.
type Hub struct {
	log     *slog.Logger
	mu      sync.RWMutex
	clients map[*client]struct{}
	done    chan struct{}
}

func NewHub(log *slog.Logger) *Hub {
	return &Hub{log: log, clients: map[*client]struct{}{}, done: make(chan struct{})}
}

// Run consumes Redis pub/sub until ctx is cancelled, then signals every
// connected client to close (so shutdown doesn't wait on open sockets).
func (h *Hub) Run(ctx context.Context, rdb redis.UniversalClient) {
	sub := rdb.Subscribe(ctx, events.Channel)
	defer func() { _ = sub.Close() }()
	defer close(h.done)

	msgs := sub.Channel() // go-redis reconnects and resubscribes on its own
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			h.Broadcast([]byte(msg.Payload))
		}
	}
}

// Broadcast delivers one raw event to every matching client without
// blocking: a client whose buffer is full is marked slow and disconnected.
func (h *Hub) Broadcast(raw []byte) {
	var ev events.Event
	if err := json.Unmarshal(raw, &ev); err != nil {
		h.log.Warn("dropping malformed status event", "error", err)
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		if !c.filter.matches(ev) {
			continue
		}
		select {
		case c.send <- raw:
		default:
			c.slowOnce.Do(func() { close(c.slow) })
		}
	}
}

func (h *Hub) register(f Filter) *client {
	c := &client{filter: f, send: make(chan []byte, clientBuffer), slow: make(chan struct{})}
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	return c
}

func (h *Hub) unregister(c *client) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

// Clients reports how many clients are connected (for tests and metrics).
func (h *Hub) Clients() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

package ws

import (
	"context"
	"net/http"
	"slices"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

const (
	pingInterval = 30 * time.Second // keeps idle connections alive through proxies
	writeTimeout = 5 * time.Second
)

// ParseFilter reads ?notification_id= and ?batch_id= (both optional UUIDs).
func ParseFilter(r *http.Request) (Filter, string, bool) {
	var f Filter
	for _, p := range []struct {
		name string
		dst  **uuid.UUID
	}{{"notification_id", &f.NotificationID}, {"batch_id", &f.BatchID}} {
		raw := r.URL.Query().Get(p.name)
		if raw == "" {
			continue
		}
		id, err := uuid.Parse(raw)
		if err != nil {
			return Filter{}, p.name, false
		}
		*p.dst = &id
	}
	return f, "", true
}

// Serve upgrades the request and streams matching events until the client
// disconnects, falls behind, or the hub shuts down. allowedOrigins are
// browser origins allowed to connect ("*" = any); same-origin is always
// allowed.
func (h *Hub) Serve(w http.ResponseWriter, r *http.Request, f Filter, allowedOrigins []string) {
	opts := &websocket.AcceptOptions{OriginPatterns: allowedOrigins}
	if slices.Contains(allowedOrigins, "*") {
		opts = &websocket.AcceptOptions{InsecureSkipVerify: true}
	}
	conn, err := websocket.Accept(w, r, opts)
	if err != nil {
		return // Accept already wrote the HTTP error
	}
	defer func() { _ = conn.CloseNow() }()

	c := h.register(f)
	defer h.unregister(c)

	// We only send; CloseRead handles control frames and tells us when the
	// client goes away.
	ctx := conn.CloseRead(r.Context())
	ping := time.NewTicker(pingInterval)
	defer ping.Stop()

	for {
		select {
		case raw := <-c.send:
			if err := write(ctx, conn, raw); err != nil {
				return
			}
		case <-ping.C:
			pctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := conn.Ping(pctx)
			cancel()
			if err != nil {
				return
			}
		case <-c.slow:
			_ = conn.Close(websocket.StatusPolicyViolation, "client too slow; reconnect and read current state via the API")
			return
		case <-h.done:
			_ = conn.Close(websocket.StatusGoingAway, "server shutting down")
			return
		case <-ctx.Done():
			return
		}
	}
}

func write(ctx context.Context, conn *websocket.Conn, raw []byte) error {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return conn.Write(ctx, websocket.MessageText, raw)
}

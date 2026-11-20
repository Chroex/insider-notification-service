// Package httpapi exposes the notification service over HTTP using the
// standard library router (Go 1.22+ method and path-parameter patterns).
package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	"notification-service/internal/health"
	"notification-service/internal/notification"
)

// NewRouter wires routes and middleware. Middleware order, outermost first:
// correlation ID (so everything below can log it), access log, panic recovery.
func NewRouter(svc *notification.Service, log *slog.Logger, checks ...health.Check) http.Handler {
	h := &handlers{svc: svc}
	mux := http.NewServeMux()

	health.Register(mux, 2*time.Second, checks...)

	mux.HandleFunc("POST /api/v1/notifications", h.createNotification)
	mux.HandleFunc("POST /api/v1/notifications/batch", h.createBatch)
	mux.HandleFunc("GET /api/v1/notifications", h.listNotifications)
	mux.HandleFunc("GET /api/v1/notifications/{id}", h.getNotification)
	mux.HandleFunc("POST /api/v1/notifications/{id}/cancel", h.cancelNotification)
	mux.HandleFunc("POST /api/v1/notifications/{id}/retry", h.retryNotification)
	mux.HandleFunc("GET /api/v1/batches/{id}", h.getBatch)

	// "/" matches anything not matched above: JSON 404 instead of text/plain.
	mux.HandleFunc("/", notFound)

	return chain(mux, correlationID(log), requestLogger, recoverer)
}

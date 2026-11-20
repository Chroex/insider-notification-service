// Package httpapi exposes the notification service over HTTP using the
// standard library router (Go 1.22+ method and path-parameter patterns).
package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"notification-service/internal/health"
	"notification-service/internal/notification"
	"notification-service/internal/observability"
)

// Options are the router's dependencies.
type Options struct {
	Service *notification.Service
	Logger  *slog.Logger
	// Registry is served at GET /metrics. If nil, a fresh one with runtime
	// collectors is created.
	Registry *prometheus.Registry
	Checks   []health.Check
}

// NewRouter wires routes and middleware. Middleware order, outermost first:
// correlation ID (so everything below can log it), access log, panic
// recovery, and HTTP metrics innermost (see httpMetrics for why).
func NewRouter(opts Options) http.Handler {
	reg := opts.Registry
	if reg == nil {
		reg = observability.NewRegistry()
	}
	metrics := observability.NewAPIMetrics(reg)
	h := &handlers{svc: opts.Service, metrics: metrics}
	mux := http.NewServeMux()

	health.Register(mux, 2*time.Second, opts.Checks...)
	mux.Handle("GET /metrics", observability.MetricsHandler(reg))

	mux.HandleFunc("POST /api/v1/notifications", h.createNotification)
	mux.HandleFunc("POST /api/v1/notifications/batch", h.createBatch)
	mux.HandleFunc("GET /api/v1/notifications", h.listNotifications)
	mux.HandleFunc("GET /api/v1/notifications/{id}", h.getNotification)
	mux.HandleFunc("POST /api/v1/notifications/{id}/cancel", h.cancelNotification)
	mux.HandleFunc("POST /api/v1/notifications/{id}/retry", h.retryNotification)
	mux.HandleFunc("GET /api/v1/batches/{id}", h.getBatch)
	mux.HandleFunc("GET /api/v1/metrics/summary", h.metricsSummary)

	// "/" matches anything not matched above: JSON 404 instead of text/plain.
	mux.HandleFunc("/", notFound)

	return chain(mux, correlationID(opts.Logger), requestLogger, recoverer, httpMetrics(metrics))
}

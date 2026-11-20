// Package httpapi exposes the notification service over HTTP using the
// standard library router (Go 1.22+ method and path-parameter patterns).
package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"notification-service/internal/events"
	"notification-service/internal/health"
	"notification-service/internal/notification"
	"notification-service/internal/observability"
	"notification-service/internal/ws"
)

// Options are the router's dependencies.
type Options struct {
	Service *notification.Service
	Logger  *slog.Logger
	// Registry is served at GET /metrics. If nil, a fresh one with runtime
	// collectors is created.
	Registry *prometheus.Registry
	Checks   []health.Check
	// CORSAllowedOrigins are browser origins allowed to call the API
	// (e.g. Swagger UI) and to open the WebSocket. Empty disables CORS.
	CORSAllowedOrigins []string
	// Events publishes status changes (create, cancel, retry); nil = discard.
	Events events.Publisher
	// Hub serves GET /ws. nil disables the endpoint (503).
	Hub *ws.Hub
}

// NewRouter wires routes and middleware. Middleware order, outermost first:
// CORS (so preflights are answered before anything else), correlation ID
// (so everything below can log it), access log, panic recovery, and HTTP
// metrics innermost (see httpMetrics for why).
func NewRouter(opts Options) http.Handler {
	reg := opts.Registry
	if reg == nil {
		reg = observability.NewRegistry()
	}
	metrics := observability.NewAPIMetrics(reg)
	if opts.Events == nil {
		opts.Events = events.Nop{}
	}
	h := &handlers{svc: opts.Service, metrics: metrics, events: opts.Events, hub: opts.Hub, origins: opts.CORSAllowedOrigins}
	mux := http.NewServeMux()

	health.Register(mux, 2*time.Second, opts.Checks...)
	mux.Handle(metricsPattern, observability.MetricsHandler(reg))
	for _, rt := range h.routes() {
		mux.HandleFunc(rt.pattern, rt.handler)
	}
	// "/" matches anything not matched above: JSON 404 instead of text/plain.
	mux.HandleFunc("/", notFound)

	return chain(mux, cors(opts.CORSAllowedOrigins), correlationID(opts.Logger), requestLogger, recoverer, httpMetrics(metrics))
}

const metricsPattern = "GET /metrics"

type route struct {
	pattern string
	handler http.HandlerFunc
}

// routes is the single table of API endpoints. api/openapi.yaml must
// document exactly these (enforced by TestOpenAPISpecMatchesRoutes).
func (h *handlers) routes() []route {
	return []route{
		{"POST /api/v1/notifications", h.createNotification},
		{"POST /api/v1/notifications/batch", h.createBatch},
		{"GET /api/v1/notifications", h.listNotifications},
		{"GET /api/v1/notifications/{id}", h.getNotification},
		{"POST /api/v1/notifications/{id}/cancel", h.cancelNotification},
		{"POST /api/v1/notifications/{id}/retry", h.retryNotification},
		{"GET /api/v1/batches/{id}", h.getBatch},
		{"GET /api/v1/metrics/summary", h.metricsSummary},
		{"POST /api/v1/templates", h.createTemplate},
		{"GET /api/v1/templates/{id}", h.getTemplate},
		{"GET /ws", h.statusStream},
	}
}

// RoutePatterns lists every "METHOD /path" the router serves.
func RoutePatterns() []string {
	patterns := []string{health.LivePattern, health.ReadyPattern, metricsPattern}
	for _, rt := range (&handlers{}).routes() {
		patterns = append(patterns, rt.pattern)
	}
	return patterns
}

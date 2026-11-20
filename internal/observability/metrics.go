package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"notification-service/internal/domain"
)

// NewRegistry returns a registry with Go runtime and process collectors.
// Each binary uses its own registry (not the global default) so it exports
// only the metrics it actually produces, and tests stay isolated.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// MetricsHandler serves a registry in the Prometheus text format.
func MetricsHandler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})
}

// --- API ---

// APIMetrics are produced by cmd/api.
type APIMetrics struct {
	created      *prometheus.CounterVec
	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec
}

func NewAPIMetrics(reg prometheus.Registerer) *APIMetrics {
	m := &APIMetrics{
		created: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_created_total",
			Help: "Notifications accepted by the API (idempotent replays are not counted).",
		}, []string{"channel", "priority"}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP requests by route pattern and status code.",
		}, []string{"method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency by route pattern.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}, []string{"method", "route"}),
	}
	reg.MustRegister(m.created, m.httpRequests, m.httpDuration)
	return m
}

// NotificationCreated counts one newly enqueued notification.
func (m *APIMetrics) NotificationCreated(ch domain.Channel, p domain.Priority) {
	m.created.WithLabelValues(string(ch), string(p)).Inc()
}

// ObserveHTTP records one request. route must be the mux pattern (e.g.
// "GET /api/v1/notifications/{id}"), never the raw path: raw paths contain
// ids and would create one time series per notification.
func (m *APIMetrics) ObserveHTTP(method, route string, status int, d time.Duration) {
	m.httpRequests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
	m.httpDuration.WithLabelValues(method, route).Observe(d.Seconds())
}

// --- worker ---

// WorkerMetrics are produced by cmd/worker. Its methods implement
// worker.Observer.
type WorkerMetrics struct {
	sent            *prometheus.CounterVec
	failed          *prometheus.CounterVec
	attempts        *prometheus.CounterVec
	deliveryLatency *prometheus.HistogramVec
	endToEnd        *prometheus.HistogramVec
	queueDepth      *prometheus.GaugeVec
	breakerState    *prometheus.GaugeVec
	leasesReaped    *prometheus.CounterVec
}

func NewWorkerMetrics(reg prometheus.Registerer) *WorkerMetrics {
	m := &WorkerMetrics{
		sent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_sent_total",
			Help: "Notifications accepted by the provider.",
		}, []string{"channel"}),
		failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_failed_total",
			Help: "Notifications moved to failed, by reason (permanent_error, max_attempts, lease_expired).",
		}, []string{"channel", "reason"}),
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "delivery_attempts_total",
			Help: "Provider calls by outcome (success, retryable_error, permanent_error).",
		}, []string{"channel", "outcome"}),
		deliveryLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "delivery_latency_seconds",
			Help: "Duration of one provider HTTP call.",
			// 5ms .. ~10s, doubling: covers fast mocks up to the 5s timeout.
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
		}, []string{"channel"}),
		endToEnd: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "end_to_end_latency_seconds",
			Help:    "Time from a notification becoming due (created, or scheduled_at) to being sent.",
			Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600},
		}, []string{"channel"}),
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "queue_depth",
			Help: "Notifications not yet in a terminal state, refreshed periodically from Postgres. " +
				"Every worker replica reports the same value: aggregate with max, not sum.",
		}, []string{"channel", "priority", "status"}),
		breakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "circuit_breaker_state",
			Help: "Circuit breaker state per channel: 0 closed, 1 open, 2 half-open.",
		}, []string{"channel"}),
		leasesReaped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "leases_reaped_total",
			Help: "Expired leases recovered by the reaper (requeued or failed).",
		}, []string{"result"}),
	}
	reg.MustRegister(m.sent, m.failed, m.attempts, m.deliveryLatency, m.endToEnd,
		m.queueDepth, m.breakerState, m.leasesReaped)

	// Pre-create series so dashboards and alerts see 0 instead of "no data".
	for _, ch := range domain.Channels {
		m.sent.WithLabelValues(string(ch))
		m.breakerState.WithLabelValues(string(ch)).Set(0)
	}
	return m
}

func (m *WorkerMetrics) AttemptFinished(ch domain.Channel, outcome domain.AttemptOutcome, providerLatency time.Duration) {
	m.attempts.WithLabelValues(string(ch), string(outcome)).Inc()
	m.deliveryLatency.WithLabelValues(string(ch)).Observe(providerLatency.Seconds())
}

func (m *WorkerMetrics) NotificationSent(ch domain.Channel, endToEnd time.Duration) {
	m.sent.WithLabelValues(string(ch)).Inc()
	m.endToEnd.WithLabelValues(string(ch)).Observe(endToEnd.Seconds())
}

func (m *WorkerMetrics) NotificationFailed(ch domain.Channel, reason string) {
	m.failed.WithLabelValues(string(ch), reason).Inc()
}

func (m *WorkerMetrics) LeasesReaped(requeued int, failedByChannel map[domain.Channel]int) {
	m.leasesReaped.WithLabelValues("requeued").Add(float64(requeued))
	for ch, n := range failedByChannel {
		m.leasesReaped.WithLabelValues("failed").Add(float64(n))
		m.failed.WithLabelValues(string(ch), "lease_expired").Add(float64(n))
	}
}

// BreakerStateChanged takes the state as an int (0 closed, 1 open,
// 2 half-open) to avoid importing the worker package here.
func (m *WorkerMetrics) BreakerStateChanged(ch domain.Channel, state int) {
	m.breakerState.WithLabelValues(string(ch)).Set(float64(state))
}

// SetQueueDepth replaces the queue_depth gauge with counts. Every
// channel x priority x active-status combination is set, with 0 where counts
// has no entry, so a series that drains to empty reads 0 rather than keeping
// its last value.
func (m *WorkerMetrics) SetQueueDepth(counts map[domain.QueueBucket]int) {
	for _, ch := range domain.Channels {
		for _, p := range domain.Priorities {
			for _, st := range domain.ActiveStatuses {
				n := counts[domain.QueueBucket{Channel: ch, Priority: p, Status: st}]
				m.queueDepth.WithLabelValues(string(ch), string(p), string(st)).Set(float64(n))
			}
		}
	}
}

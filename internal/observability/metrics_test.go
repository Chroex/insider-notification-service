package observability

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"notification-service/internal/domain"
)

func TestSetQueueDepth_ZeroFillsDrainedSeries(t *testing.T) {
	m := NewWorkerMetrics(prometheus.NewRegistry())
	smsHighPending := domain.QueueBucket{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Status: domain.StatusPending}

	m.SetQueueDepth(map[domain.QueueBucket]int{smsHighPending: 42})
	if got := testutil.ToFloat64(m.queueDepth.WithLabelValues("sms", "high", "pending")); got != 42 {
		t.Fatalf("queue_depth = %v, want 42", got)
	}
	// 3 channels x 3 priorities x 4 active statuses, all present.
	if n := testutil.CollectAndCount(m.queueDepth); n != 36 {
		t.Errorf("series = %d, want 36", n)
	}

	m.SetQueueDepth(map[domain.QueueBucket]int{}) // queue drained
	if got := testutil.ToFloat64(m.queueDepth.WithLabelValues("sms", "high", "pending")); got != 0 {
		t.Errorf("drained queue_depth = %v, want 0 (not the stale 42)", got)
	}
}

func TestWorkerMetrics_Events(t *testing.T) {
	m := NewWorkerMetrics(prometheus.NewRegistry())
	m.AttemptFinished(domain.ChannelSMS, domain.OutcomeSuccess, 30*time.Millisecond)
	m.AttemptFinished(domain.ChannelSMS, domain.OutcomeRetryableError, 10*time.Millisecond)
	m.NotificationSent(domain.ChannelSMS, 3*time.Second, 2*time.Second)
	m.NotificationFailed(domain.ChannelEmail, "permanent_error")
	m.LeasesReaped(2, map[domain.Channel]int{domain.ChannelPush: 1}, map[domain.Channel]int{domain.ChannelPush: 3})
	m.BreakerStateChanged(domain.ChannelEmail, 1)

	checks := []struct {
		name string
		c    prometheus.Collector
		want float64
	}{
		{"sent sms", m.sent.WithLabelValues("sms"), 1},
		{"attempts success", m.attempts.WithLabelValues("sms", "success"), 1},
		{"attempts retryable", m.attempts.WithLabelValues("sms", "retryable_error"), 1},
		{"failed permanent", m.failed.WithLabelValues("email", "permanent_error"), 1},
		{"failed lease_expired", m.failed.WithLabelValues("push", "lease_expired"), 1},
		{"reaped requeued", m.leasesReaped.WithLabelValues("requeued"), 2},
		{"attempts interrupted", m.attempts.WithLabelValues("push", "interrupted"), 3},
		{"breaker email open", m.breakerState.WithLabelValues("email"), 1},
		{"breaker sms closed", m.breakerState.WithLabelValues("sms"), 0},
	}
	for _, c := range checks {
		if got := testutil.ToFloat64(c.c); got != c.want {
			t.Errorf("%s = %v, want %v", c.name, got, c.want)
		}
	}
	if n := testutil.CollectAndCount(m.deliveryLatency); n != 1 {
		t.Errorf("delivery latency series = %d, want 1 (sms)", n)
	}
}

func TestAPIMetrics_HTTPLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewAPIMetrics(reg)
	m.ObserveHTTP("GET", "GET /api/v1/notifications/{id}", 200, 5*time.Millisecond)
	m.NotificationCreated(domain.ChannelPush, domain.PriorityLow)

	err := testutil.CollectAndCompare(m.httpRequests, strings.NewReader(`
# HELP http_requests_total HTTP requests by route pattern and status code.
# TYPE http_requests_total counter
http_requests_total{method="GET",route="GET /api/v1/notifications/{id}",status="200"} 1
`))
	if err != nil {
		t.Error(err)
	}
	if got := testutil.ToFloat64(m.created.WithLabelValues("push", "low")); got != 1 {
		t.Errorf("created = %v", got)
	}
}

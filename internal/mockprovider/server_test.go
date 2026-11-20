package mockprovider

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"notification-service/internal/config"
)

// newTestServer returns a server whose random source yields the given
// values in order (then repeats the last) and which never sleeps.
func newTestServer(cfg config.MockProvider, randomValues ...float64) *Server {
	s := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.sleep = func(context.Context, time.Duration) {}
	i := 0
	s.random = func() float64 {
		v := randomValues[min(i, len(randomValues)-1)]
		i++
		return v
	}
	return s
}

func post(s *Server, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(body)))
	return rec
}

const validBody = `{"to":"+905551112233","channel":"sms","content":"hello"}`

func TestServer_Accepts(t *testing.T) {
	s := newTestServer(config.MockProvider{FailureRate: 0.1, FailureCodes: []int{500}}, 0.5)
	rec := post(s, validBody)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	var resp AcceptedResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.MessageID == "" || resp.Status != "accepted" || resp.Timestamp.IsZero() {
		t.Errorf("unexpected body: %+v", resp)
	}
}

func TestServer_RandomFailurePicksConfiguredCode(t *testing.T) {
	cfg := config.MockProvider{FailureRate: 0.5, FailureCodes: []int{500, 503, 429}}
	// Random draws in call order: latency (0), failure roll (0.1 < 0.5, so
	// fail), code pick (0.7 -> index 2).
	s := newTestServer(cfg, 0, 0.1, 0.7)
	if rec := post(s, validBody); rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", rec.Code)
	}
}

func TestServer_ForcedStatusOverridesRandom(t *testing.T) {
	s := newTestServer(config.MockProvider{FailureRate: 0, FailureCodes: []int{500}}, 0.9)
	if rec := post(s, `{"to":"x","channel":"sms","content":"hi [mock:400]"}`); rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestServer_RetryAfterHeader(t *testing.T) {
	cfg := config.MockProvider{FailureCodes: []int{500}, RetryAfter: 3 * time.Second}
	s := newTestServer(cfg, 0.9)
	rec := post(s, `{"to":"x","channel":"sms","content":"[mock:429]"}`)
	if got := rec.Header().Get("Retry-After"); got != "3" {
		t.Errorf("Retry-After = %q, want 3", got)
	}
	rec = post(s, `{"to":"x","channel":"sms","content":"[mock:500]"}`)
	if got := rec.Header().Get("Retry-After"); got != "" {
		t.Errorf("Retry-After on 500 = %q, want none", got)
	}
}

func TestServer_RejectsBadRequests(t *testing.T) {
	s := newTestServer(config.MockProvider{FailureCodes: []int{500}}, 0.9)
	tests := []struct{ name, body string }{
		{"malformed json", `{"to":`},
		{"missing content", `{"to":"x","channel":"sms"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if rec := post(s, tt.body); rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", rec.Code)
			}
		})
	}
}

func TestServer_LatencyWithinBounds(t *testing.T) {
	cfg := config.MockProvider{LatencyMin: 10 * time.Millisecond, LatencyMax: 30 * time.Millisecond}
	for _, r := range []float64{0, 0.5, 0.999} {
		s := newTestServer(cfg, r)
		got := s.latency()
		if got < cfg.LatencyMin || got > cfg.LatencyMax {
			t.Errorf("latency(%v) = %v, want within [%v, %v]", r, got, cfg.LatencyMin, cfg.LatencyMax)
		}
	}
}

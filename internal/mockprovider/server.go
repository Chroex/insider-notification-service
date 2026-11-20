// Package mockprovider mimics webhook.site configured to accept messages: it
// answers 202 with {messageId, status, timestamp}. Latency and failure rates
// are configurable so retries, backoff and the circuit breaker can be
// demonstrated locally without external dependencies or free-tier limits.
//
// For deterministic demos, content containing "[mock:<code>]" (for example
// "[mock:400]") always gets that status code.
package mockprovider

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/google/uuid"

	"notification-service/internal/config"
)

// Request is the provider contract: the same shape the worker sends.
type Request struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// AcceptedResponse is returned with 202 Accepted.
type AcceptedResponse struct {
	MessageID string    `json:"messageId"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

var forcedStatus = regexp.MustCompile(`\[mock:(\d{3})\]`)

// Server is an http.Handler. random and sleep are injectable for tests.
type Server struct {
	cfg    config.MockProvider
	log    *slog.Logger
	random func() float64 // uniform in [0, 1)
	sleep  func(ctx context.Context, d time.Duration)
}

func New(cfg config.MockProvider, log *slog.Logger) *Server {
	return &Server{cfg: cfg, log: log, random: rand.Float64, sleep: sleepCtx}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "use POST")
		return
	}

	var req Request
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.To == "" || req.Channel == "" || req.Content == "" {
		writeError(w, http.StatusBadRequest, "to, channel and content are required")
		return
	}

	s.sleep(r.Context(), s.latency())

	status := s.chooseStatus(req.Content)
	s.log.Info("mock provider request",
		"notification_id", r.Header.Get("X-Notification-ID"),
		"correlation_id", r.Header.Get("X-Correlation-ID"),
		"channel", req.Channel, "status", status)

	if status != http.StatusAccepted {
		if (status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable) && s.cfg.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(int(s.cfg.RetryAfter.Seconds())))
		}
		writeError(w, status, http.StatusText(status))
		return
	}

	writeJSON(w, http.StatusAccepted, AcceptedResponse{
		MessageID: uuid.NewString(),
		Status:    "accepted",
		Timestamp: time.Now().UTC(),
	})
}

// chooseStatus applies a forced status from the content first, then the
// random failure rate.
func (s *Server) chooseStatus(content string) int {
	if m := forcedStatus.FindStringSubmatch(content); m != nil {
		code, _ := strconv.Atoi(m[1]) // regexp guarantees three digits
		return code
	}
	if s.random() < s.cfg.FailureRate {
		idx := int(s.random() * float64(len(s.cfg.FailureCodes)))
		return s.cfg.FailureCodes[min(idx, len(s.cfg.FailureCodes)-1)]
	}
	return http.StatusAccepted
}

func (s *Server) latency() time.Duration {
	spread := s.cfg.LatencyMax - s.cfg.LatencyMin
	return s.cfg.LatencyMin + time.Duration(s.random()*float64(spread))
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

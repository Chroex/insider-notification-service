package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func okCheck(name string) Check {
	return Check{Name: name, Ping: func(context.Context) error { return nil }}
}

func failingCheck(name string) Check {
	return Check{Name: name, Ping: func(context.Context) error { return errors.New("connection refused") }}
}

func serve(t *testing.T, path string, checks ...Check) (*httptest.ResponseRecorder, response) {
	t.Helper()
	mux := http.NewServeMux()
	Register(mux, time.Second, checks...)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	var body response
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	return rec, body
}

func TestLive_IgnoresDependencies(t *testing.T) {
	rec, body := serve(t, "/health/live", failingCheck("postgres"))
	if rec.Code != http.StatusOK || body.Status != "ok" {
		t.Errorf("live = %d %+v, want 200 ok even when dependencies fail", rec.Code, body)
	}
}

func TestReady(t *testing.T) {
	tests := []struct {
		name       string
		checks     []Check
		wantStatus int
		wantChecks map[string]string
	}{
		{
			name:       "all dependencies up",
			checks:     []Check{okCheck("postgres"), okCheck("redis")},
			wantStatus: http.StatusOK,
			wantChecks: map[string]string{"postgres": "ok", "redis": "ok"},
		},
		{
			name:       "one dependency down",
			checks:     []Check{okCheck("postgres"), failingCheck("redis")},
			wantStatus: http.StatusServiceUnavailable,
			wantChecks: map[string]string{"postgres": "ok", "redis": "error: connection refused"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec, body := serve(t, "/health/ready", tt.checks...)
			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			for name, want := range tt.wantChecks {
				if got := body.Checks[name]; got != want {
					t.Errorf("check %s = %q, want %q", name, got, want)
				}
			}
		})
	}
}

func TestReady_RespectsTimeout(t *testing.T) {
	slow := Check{Name: "slow", Ping: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	mux := http.NewServeMux()
	Register(mux, 20*time.Millisecond, slow)
	rec := httptest.NewRecorder()

	start := time.Now()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("readiness took %v; timeout not applied", elapsed)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
}

// An optional dependency (Redis for the API: only the live status stream
// needs it) is reported but does not take the instance out of rotation:
// accepting notifications needs Postgres alone.
func TestReady_OptionalDependencyDownIsDegradedNotUnavailable(t *testing.T) {
	redis := failingCheck("redis")
	redis.Optional = true
	rec, body := serve(t, "/health/ready", okCheck("postgres"), redis)
	if rec.Code != http.StatusOK || body.Status != "degraded" || body.Checks["redis"] != "error: connection refused" {
		t.Errorf("ready = %d %+v, want 200 degraded with the redis error", rec.Code, body)
	}
	rec, _ = serve(t, "/health/ready", failingCheck("postgres"), redis)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("required dependency down = %d, want 503", rec.Code)
	}
}

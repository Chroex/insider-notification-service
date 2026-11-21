package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Requests no route handles get the API's JSON error shape: 404 for an
// unknown path, 405 with an Allow header for a known path and wrong method.
func TestRouter_UnmatchedRequestsGetJSONErrors(t *testing.T) {
	router := NewRouter(Options{Logger: slog.New(slog.DiscardHandler)})
	tests := []struct {
		method, path string
		wantStatus   int
		wantCode     string
		wantAllow    []string
	}{
		{"GET", "/api/v1/nope", http.StatusNotFound, "not_found", nil},
		{"PUT", "/api/v1/notifications", http.StatusMethodNotAllowed, "method_not_allowed", []string{"GET", "POST"}},
		{"DELETE", "/api/v1/notifications/0190a8c4-0000-7000-8000-000000000000", http.StatusMethodNotAllowed,
			"method_not_allowed", []string{"GET"}},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.path, nil))

			var body struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("body is not JSON: %q", rec.Body.String())
			}
			if rec.Code != tt.wantStatus || body.Error.Code != tt.wantCode {
				t.Errorf("got %d %q, want %d %q", rec.Code, body.Error.Code, tt.wantStatus, tt.wantCode)
			}
			allow := rec.Header().Get("Allow")
			for _, m := range tt.wantAllow {
				if !strings.Contains(allow, m) {
					t.Errorf("Allow = %q, want it to list %s", allow, m)
				}
			}
		})
	}
}

package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCORS(t *testing.T) {
	var reached bool
	h := cors([]string{"http://localhost:8081"})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))
	do := func(method, origin string, preflight bool) *httptest.ResponseRecorder {
		reached = false
		req := httptest.NewRequest(method, "/api/v1/notifications", nil)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		if preflight {
			req.Header.Set("Access-Control-Request-Method", "POST")
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	rec := do(http.MethodOptions, "http://localhost:8081", true)
	if rec.Code != http.StatusNoContent || reached ||
		rec.Header().Get("Access-Control-Allow-Origin") != "http://localhost:8081" ||
		rec.Header().Get("Access-Control-Allow-Headers") == "" {
		t.Errorf("preflight from allowed origin: code %d reached %v headers %v", rec.Code, reached, rec.Header())
	}

	rec = do(http.MethodGet, "http://localhost:8081", false)
	if !reached || rec.Header().Get("Access-Control-Expose-Headers") == "" {
		t.Errorf("simple request from allowed origin should pass through with CORS headers: %v", rec.Header())
	}

	rec = do(http.MethodGet, "https://evil.example", false)
	if !reached || rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("disallowed origin must get no CORS headers: %v", rec.Header())
	}

	rec = do(http.MethodGet, "", false)
	if !reached || rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("non-browser requests are unaffected")
	}
}

func TestCORS_Wildcard(t *testing.T) {
	h := cors([]string{"*"})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "http://anything.test")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Header().Get("Access-Control-Allow-Origin") != "http://anything.test" {
		t.Errorf("wildcard should allow any origin: %v", rec.Header())
	}
}

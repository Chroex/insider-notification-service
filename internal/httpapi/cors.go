package httpapi

import (
	"net/http"
	"slices"
)

// cors lets browser clients on allowed origins call the API, chiefly
// Swagger UI's "Try it out" (served from :8081, a different origin than the
// API on :8080). Origins are an explicit allow-list from CORS_ALLOWED_ORIGINS;
// "*" allows any origin. No cookies are used, so credentials are not allowed.
func cors(allowed []string) func(http.Handler) http.Handler {
	anyOrigin := slices.Contains(allowed, "*")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin == "" || (!anyOrigin && !slices.Contains(allowed, origin)) {
				next.ServeHTTP(w, r)
				return
			}
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", origin)
			h.Add("Vary", "Origin") // caches must not reuse this answer for other origins
			h.Set("Access-Control-Expose-Headers", "X-Correlation-ID, Location, Idempotent-Replayed")

			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				// Preflight: answer here; it must not reach the API handlers.
				h.Set("Access-Control-Allow-Methods", "GET, POST")
				h.Set("Access-Control-Allow-Headers", "Content-Type, Idempotency-Key, X-Correlation-ID")
				h.Set("Access-Control-Max-Age", "600")
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

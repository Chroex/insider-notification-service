package httpapi

import (
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"strings"
	"time"

	"github.com/google/uuid"

	"notification-service/internal/observability"
)

// HeaderCorrelationID is read from requests, echoed on responses, stored on
// every notification and forwarded to the provider.
const HeaderCorrelationID = "X-Correlation-ID"

// A client-supplied correlation ID ends up in logs and the database, so it
// is restricted to a safe charset and length (no log injection, no bloat).
var validCorrelationID = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// correlationID reuses a valid incoming X-Correlation-ID or generates one,
// then puts it in the context, the response header and a request logger.
func correlationID(base *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get(HeaderCorrelationID)
			if !validCorrelationID.MatchString(id) {
				id = uuid.NewString()
			}
			w.Header().Set(HeaderCorrelationID, id)

			ctx := observability.WithCorrelationID(r.Context(), id)
			ctx = observability.WithLogger(ctx, base.With(observability.KeyCorrelationID, id))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// statusRecorder captures the status code for access logs.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// requestLogger writes one structured access-log line per request.
func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		level := slog.LevelInfo
		switch {
		case rec.status >= 500:
			level = slog.LevelError
		case strings.HasPrefix(r.URL.Path, "/health/"):
			level = slog.LevelDebug // probes run every few seconds; keep them out of info logs
		}
		observability.Logger(r.Context()).Log(r.Context(), level, "http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

// recoverer turns a panic in a handler into a logged 500 with the standard
// error body, instead of a dropped connection.
func recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler { //nolint:errorlint // sentinel compared by identity, as net/http does
					panic(v) // the client went away; let net/http handle it
				}
				observability.Logger(r.Context()).Error("panic in handler",
					"panic", v, "stack", string(debug.Stack()))
				writeError(w, r, http.StatusInternalServerError, codeInternal, "internal server error", nil)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// chain applies middleware so the first one listed is the outermost.
func chain(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

package httpapi

import (
	"bufio"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"runtime/debug"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

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
			log := base.With(observability.KeyCorrelationID, id)
			if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
				// trace_id in every log line lets you jump from a log to its trace.
				log = log.With(observability.KeyTraceID, sc.TraceID().String())
				trace.SpanFromContext(ctx).SetAttributes(attribute.String("correlation_id", id))
			}
			ctx = observability.WithLogger(ctx, log)
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

// Hijack lets WebSocket upgrades pass through the middleware chain: the
// upgrade needs the raw connection, which a plain wrapper would hide.
func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("underlying ResponseWriter does not support hijacking")
	}
	r.status = http.StatusSwitchingProtocols
	return h.Hijack()
}

// Unwrap exposes the wrapped writer to http.ResponseController.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

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

// tracing starts a server span per request (continuing an incoming
// traceparent). Probes, metrics scrapes and long-lived WebSockets are not
// traced: they would be noise, or a span that lasts hours.
func tracing(next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, "http.request",
		otelhttp.WithFilter(func(r *http.Request) bool {
			return !strings.HasPrefix(r.URL.Path, "/health/") && r.URL.Path != "/metrics" && r.URL.Path != "/ws"
		}))
}

// httpMetrics records request count and latency per route pattern.
//
// It must be the innermost middleware: ServeMux stores the matched pattern
// in r.Pattern on the *same* *http.Request it was given, and outer
// middleware hand it a copy (r.WithContext). Labelling by pattern, never by
// raw path, keeps cardinality bounded (paths contain notification ids).
func httpMetrics(m *observability.APIMetrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			m.ObserveHTTP(r.Method, route, rec.status, time.Since(start))
			// Name the span after the route pattern now that the mux has
			// matched it ("POST /api/v1/notifications", not a raw path).
			trace.SpanFromContext(r.Context()).SetName(route)
		})
	}
}

// chain applies middleware so the first one listed is the outermost.
func chain(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

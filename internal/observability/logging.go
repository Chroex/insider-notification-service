// Package observability holds logging and (later) metrics helpers shared by
// all binaries.
package observability

import (
	"io"
	"log/slog"
)

// Standard log attribute keys. Using constants keeps keys identical across
// the API, worker and provider client so logs can be joined in a log store.
const (
	KeyCorrelationID  = "correlation_id"
	KeyNotificationID = "notification_id"
	KeyBatchID        = "batch_id"
	KeyChannel        = "channel"
	KeyAttempt        = "attempt"
	KeyService        = "service"
	KeyTraceID        = "trace_id"
)

// NewLogger returns a JSON slog logger tagged with the service name.
func NewLogger(w io.Writer, service string, level slog.Level) *slog.Logger {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(handler).With(KeyService, service)
}

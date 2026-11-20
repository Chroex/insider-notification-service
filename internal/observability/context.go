package observability

import (
	"context"
	"log/slog"
)

type ctxKey int

const (
	correlationIDKey ctxKey = iota
	loggerKey
)

// WithCorrelationID stores the request's correlation ID in ctx.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey, id)
}

// CorrelationID returns the ID stored by WithCorrelationID, or "".
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationIDKey).(string)
	return id
}

// WithLogger stores a request-scoped logger (already tagged with the
// correlation ID) so every layer logs with the same keys.
func WithLogger(ctx context.Context, log *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, log)
}

// Logger returns the request-scoped logger, or slog.Default().
func Logger(ctx context.Context) *slog.Logger {
	if log, ok := ctx.Value(loggerKey).(*slog.Logger); ok {
		return log
	}
	return slog.Default()
}

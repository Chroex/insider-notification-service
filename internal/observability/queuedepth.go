package observability

import (
	"context"
	"log/slog"
	"time"

	"notification-service/internal/domain"
)

// QueueDepthFunc fetches current counts (store.Store.QueueDepth).
type QueueDepthFunc func(ctx context.Context) (map[domain.QueueBucket]int, error)

// RefreshQueueDepth updates the queue_depth gauge from the database every
// interval until ctx is cancelled. The value comes from Postgres, the
// source of truth, so it is correct no matter which replica did the work.
// One GROUP BY over the partial claim/lease indexes per interval is cheap.
func RefreshQueueDepth(ctx context.Context, interval time.Duration, fetch QueueDepthFunc, m *WorkerMetrics, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		counts, err := fetch(ctx)
		switch {
		case err == nil:
			m.SetQueueDepth(counts)
		case ctx.Err() == nil:
			// Keep the last values; a stale gauge is better than a false zero.
			log.Warn("queue depth refresh failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

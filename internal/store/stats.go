package store

import (
	"context"
	"fmt"
	"time"

	"notification-service/internal/domain"
)

// ChannelStats aggregates delivery results over a time window. Latencies
// are in milliseconds; nil when there was no data in the window.
type ChannelStats struct {
	Sent                     int
	Failed                   int
	AttemptsByOutcome        map[domain.AttemptOutcome]int
	ProviderP50, ProviderP95 *float64
	EndToEndP50, EndToEndP95 *float64
}

// Summary is the data behind GET /api/v1/metrics/summary.
type Summary struct {
	Since      time.Time
	QueueDepth map[domain.Channel]map[domain.Status]int // non-terminal statuses only
	Overall    ChannelStats
	ByChannel  map[domain.Channel]*ChannelStats
}

// Summary computes queue depth (now) and delivery stats since `since`.
//
// It is computed from Postgres rather than from in-process Prometheus
// histograms on purpose: the database sees the work of every worker
// replica, while each process's histograms only see its own.
// percentile_cont over the window is exact (Prometheus quantiles are
// bucket-interpolated estimates). The time-column indexes from migration
// 000006 keep each query proportional to the window, not the full history.
func (s *Store) Summary(ctx context.Context, since time.Time) (Summary, error) {
	sum := Summary{
		Since:      since,
		QueueDepth: map[domain.Channel]map[domain.Status]int{},
		Overall:    ChannelStats{AttemptsByOutcome: map[domain.AttemptOutcome]int{}},
		ByChannel:  map[domain.Channel]*ChannelStats{},
	}
	for _, ch := range domain.Channels {
		sum.ByChannel[ch] = &ChannelStats{AttemptsByOutcome: map[domain.AttemptOutcome]int{}}
		sum.QueueDepth[ch] = map[domain.Status]int{}
	}
	// pick returns the per-channel stats, or Overall for ROLLUP's NULL row.
	pick := func(ch *string) *ChannelStats {
		if ch == nil {
			return &sum.Overall
		}
		if st, ok := sum.ByChannel[domain.Channel(*ch)]; ok {
			return st
		}
		return &ChannelStats{AttemptsByOutcome: map[domain.AttemptOutcome]int{}} // unknown channel: ignore
	}

	steps := []struct {
		name string
		run  func() error
	}{
		{"queue depth", func() error { return s.queueDepth(ctx, sum.QueueDepth) }},
		{"attempts", func() error {
			rows, err := s.pool.Query(ctx, `
				SELECT n.channel, a.outcome, count(*)
				FROM delivery_attempts a JOIN notifications n ON n.id = a.notification_id
				WHERE a.started_at >= $1
				GROUP BY n.channel, a.outcome`, since)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var ch string
				var outcome domain.AttemptOutcome
				var n int
				if err := rows.Scan(&ch, &outcome, &n); err != nil {
					return err
				}
				pick(&ch).AttemptsByOutcome[outcome] += n
				sum.Overall.AttemptsByOutcome[outcome] += n
			}
			return rows.Err()
		}},
		{"provider latency", func() error {
			rows, err := s.pool.Query(ctx, `
				SELECT n.channel,
				       percentile_cont(0.50) WITHIN GROUP (ORDER BY a.duration_ms),
				       percentile_cont(0.95) WITHIN GROUP (ORDER BY a.duration_ms)
				FROM delivery_attempts a JOIN notifications n ON n.id = a.notification_id
				WHERE a.started_at >= $1
				GROUP BY ROLLUP (n.channel)`, since)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var ch *string
				var p50, p95 *float64
				if err := rows.Scan(&ch, &p50, &p95); err != nil {
					return err
				}
				st := pick(ch)
				st.ProviderP50, st.ProviderP95 = p50, p95
			}
			return rows.Err()
		}},
		{"sent", func() error {
			// End-to-end runs from creation to sent, as the brief defines
			// it (scheduled waits included); the worker's
			// due_to_sent_seconds metric shows the delay after becoming due.
			rows, err := s.pool.Query(ctx, `
				SELECT channel, count(*),
				       percentile_cont(0.50) WITHIN GROUP (ORDER BY e2e_ms),
				       percentile_cont(0.95) WITHIN GROUP (ORDER BY e2e_ms)
				FROM (
					SELECT channel,
					       extract(epoch FROM sent_at - created_at) * 1000 AS e2e_ms
					FROM notifications
					WHERE status = 'sent' AND sent_at >= $1
				) s
				GROUP BY ROLLUP (channel)`, since)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var ch *string
				var n int
				var p50, p95 *float64
				if err := rows.Scan(&ch, &n, &p50, &p95); err != nil {
					return err
				}
				st := pick(ch)
				st.Sent, st.EndToEndP50, st.EndToEndP95 = n, p50, p95
			}
			return rows.Err()
		}},
		{"failed", func() error {
			rows, err := s.pool.Query(ctx, `
				SELECT channel, count(*) FROM notifications
				WHERE status = 'failed' AND updated_at >= $1
				GROUP BY ROLLUP (channel)`, since)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var ch *string
				var n int
				if err := rows.Scan(&ch, &n); err != nil {
					return err
				}
				pick(ch).Failed = n
			}
			return rows.Err()
		}},
	}
	for _, step := range steps {
		if err := step.run(); err != nil {
			return Summary{}, fmt.Errorf("summary %s: %w", step.name, err)
		}
	}
	return sum, nil
}

// QueueDepth counts non-terminal notifications by channel, priority and
// status. Terminal rows (sent/failed/cancelled) grow forever and are not
// queue depth, so they are excluded; the remaining statuses are covered by
// the partial claim and lease indexes.
func (s *Store) QueueDepth(ctx context.Context) (map[domain.QueueBucket]int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT channel, priority, status, count(*)
		FROM notifications
		WHERE status IN ('scheduled', 'pending', 'processing', 'retrying')
		GROUP BY channel, priority, status`)
	if err != nil {
		return nil, fmt.Errorf("queue depth: %w", err)
	}
	defer rows.Close()
	out := map[domain.QueueBucket]int{}
	for rows.Next() {
		var k domain.QueueBucket
		var n int
		if err := rows.Scan(&k.Channel, &k.Priority, &k.Status, &n); err != nil {
			return nil, fmt.Errorf("queue depth: scan: %w", err)
		}
		out[k] = n
	}
	return out, rows.Err()
}

func (s *Store) queueDepth(ctx context.Context, into map[domain.Channel]map[domain.Status]int) error {
	depth, err := s.QueueDepth(ctx)
	if err != nil {
		return err
	}
	for k, n := range depth {
		if m, ok := into[k.Channel]; ok {
			m[k.Status] += n
		}
	}
	return nil
}

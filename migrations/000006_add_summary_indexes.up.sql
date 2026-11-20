-- Indexes for GET /api/v1/metrics/summary, which aggregates over a recent
-- time window. Without them each summary would scan the full history.
CREATE INDEX delivery_attempts_started_idx ON delivery_attempts (started_at);

-- Partial: only sent / failed rows, keyed by when they reached that state.
CREATE INDEX notifications_sent_idx   ON notifications (sent_at)    WHERE status = 'sent';
CREATE INDEX notifications_failed_idx ON notifications (updated_at) WHERE status = 'failed';

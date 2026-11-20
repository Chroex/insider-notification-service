-- One row per provider call. This is the audit trail behind
-- GET /notifications/{id} and the per-attempt metrics.
CREATE TABLE delivery_attempts (
    id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    notification_id uuid        NOT NULL REFERENCES notifications (id) ON DELETE CASCADE,
    attempt_no      integer     NOT NULL CHECK (attempt_no > 0),
    started_at      timestamptz NOT NULL,
    duration_ms     integer     NOT NULL CHECK (duration_ms >= 0),
    http_status     integer,            -- NULL for network errors / timeouts
    error           text,
    outcome         text        NOT NULL CHECK (outcome IN (
                                    'success', 'retryable_error', 'permanent_error')),
    -- attempt_no is assigned when a worker claims the row, so two workers can
    -- never record the same attempt number. Also serves lookups by notification.
    UNIQUE (notification_id, attempt_no)
);

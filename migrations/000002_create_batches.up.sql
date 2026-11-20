-- A batch groups notifications created by one POST /notifications/batch call.
-- Per-status counts are computed from notifications, not stored here, so they
-- can never drift out of sync.
CREATE TABLE batches (
    id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    total      integer     NOT NULL CHECK (total > 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

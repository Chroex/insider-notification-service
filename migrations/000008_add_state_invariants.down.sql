ALTER TABLE batches DROP CONSTRAINT IF EXISTS batches_idempotency_key_has_hash;

ALTER TABLE notifications
    DROP CONSTRAINT IF EXISTS notifications_variables_need_template,
    DROP CONSTRAINT IF EXISTS notifications_idempotency_key_has_hash,
    DROP CONSTRAINT IF EXISTS notifications_attempts_within_budget,
    DROP CONSTRAINT IF EXISTS notifications_scheduled_has_time,
    DROP CONSTRAINT IF EXISTS notifications_sent_at_iff_sent,
    DROP CONSTRAINT IF EXISTS notifications_lease_iff_processing;

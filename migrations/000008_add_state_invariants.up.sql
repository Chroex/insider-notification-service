-- Invariants every code path already maintains, now enforced by the database.
-- A future bug in any UPDATE fails loudly instead of leaving a row that is
-- quietly wrong: e.g. 'processing' without a lease is never reaped, so it
-- would be stuck forever; 'sent' without sent_at breaks the latency metrics.
ALTER TABLE notifications
    ADD CONSTRAINT notifications_lease_iff_processing
        CHECK ((status = 'processing') = (locked_until IS NOT NULL)),
    ADD CONSTRAINT notifications_sent_at_iff_sent
        CHECK ((status = 'sent') = (sent_at IS NOT NULL)),
    ADD CONSTRAINT notifications_scheduled_has_time
        CHECK (status <> 'scheduled' OR scheduled_at IS NOT NULL),
    ADD CONSTRAINT notifications_attempts_within_budget
        CHECK (attempts <= max_attempts),
    ADD CONSTRAINT notifications_idempotency_key_has_hash
        CHECK ((idempotency_key IS NULL) = (request_hash IS NULL)),
    ADD CONSTRAINT notifications_variables_need_template
        CHECK (variables IS NULL OR template_id IS NOT NULL);

ALTER TABLE batches
    ADD CONSTRAINT batches_idempotency_key_has_hash
        CHECK ((idempotency_key IS NULL) = (request_hash IS NULL));

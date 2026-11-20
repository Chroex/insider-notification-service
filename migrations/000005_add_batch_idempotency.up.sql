-- A client that times out on POST /notifications/batch and retries must not
-- create up to 1000 duplicate notifications, so batches get their own
-- Idempotency-Key (same semantics as single notifications).
ALTER TABLE batches
    ADD COLUMN idempotency_key text UNIQUE,
    ADD COLUMN request_hash    text;

ALTER TABLE batches
    DROP COLUMN IF EXISTS idempotency_key,
    DROP COLUMN IF EXISTS request_hash;

-- A worker that crashes (or stalls past its lease) in the middle of a
-- provider call never records that attempt. The reaper now writes it as
-- 'interrupted': the call may or may not have reached the provider, and the
-- history should say so instead of skipping an attempt number. Its duration
-- is unknown, so duration_ms becomes nullable for that outcome only.
ALTER TABLE delivery_attempts
    DROP CONSTRAINT delivery_attempts_outcome_check,
    ADD CONSTRAINT delivery_attempts_outcome_check CHECK (outcome IN (
        'success', 'retryable_error', 'permanent_error', 'interrupted')),
    ALTER COLUMN duration_ms DROP NOT NULL,
    ADD CONSTRAINT delivery_attempts_duration_known
        CHECK (duration_ms IS NOT NULL OR outcome = 'interrupted');

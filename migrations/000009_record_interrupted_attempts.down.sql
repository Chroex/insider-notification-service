DELETE FROM delivery_attempts WHERE outcome = 'interrupted';

ALTER TABLE delivery_attempts
    DROP CONSTRAINT IF EXISTS delivery_attempts_duration_known,
    ALTER COLUMN duration_ms SET NOT NULL,
    DROP CONSTRAINT delivery_attempts_outcome_check,
    ADD CONSTRAINT delivery_attempts_outcome_check CHECK (outcome IN (
        'success', 'retryable_error', 'permanent_error'));

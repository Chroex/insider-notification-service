-- notifications is both the source of truth and the work queue
-- (claimed with SELECT ... FOR UPDATE SKIP LOCKED). See docs/DECISIONS.md.
CREATE TABLE notifications (
    id                  uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id            uuid        REFERENCES batches (id),

    -- Idempotency: the key is unique (NULLs allowed many times); request_hash
    -- lets us tell "same request replayed" (200) from "key reused with a
    -- different payload" (409).
    idempotency_key     text        UNIQUE,
    request_hash        text,

    correlation_id      text        NOT NULL,
    channel             text        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    priority            text        NOT NULL DEFAULT 'normal'
                                    CHECK (priority IN ('high', 'normal', 'low')),
    -- Numeric rank so the claim query can ORDER BY and use an index; text
    -- ordering would put 'high' < 'low' < 'normal'.
    priority_rank       smallint    GENERATED ALWAYS AS (
                                        CASE priority WHEN 'high' THEN 0
                                                      WHEN 'normal' THEN 1
                                                      ELSE 2 END
                                    ) STORED,
    recipient           text        NOT NULL,
    -- Always the final, rendered content (templates are rendered at create
    -- time), so workers never need template logic.
    content             text        NOT NULL,
    template_id         uuid        REFERENCES templates (id),
    variables           jsonb,

    status              text        NOT NULL CHECK (status IN (
                                        'scheduled', 'pending', 'processing', 'retrying',
                                        'sent', 'failed', 'cancelled')),
    attempts            integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts        integer     NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    -- When the row next becomes claimable: now() for new rows, scheduled_at
    -- for scheduled rows, now()+backoff for retries.
    next_attempt_at     timestamptz NOT NULL DEFAULT now(),
    scheduled_at        timestamptz,
    -- Worker lease: a 'processing' row whose lease has expired is returned
    -- to the queue by the reaper (crash recovery).
    locked_until        timestamptz,
    last_error          text,
    provider_message_id text,

    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    sent_at             timestamptz
);

-- Claim query: WHERE status IN (claimable) AND channel = $1
-- AND next_attempt_at <= now() ORDER BY priority_rank, next_attempt_at.
-- Partial index: only claimable rows are indexed, so it stays small even
-- when millions of sent rows accumulate.
CREATE INDEX notifications_claim_idx
    ON notifications (channel, priority_rank, next_attempt_at)
    WHERE status IN ('scheduled', 'pending', 'retrying');

-- Reaper: find expired leases quickly.
CREATE INDEX notifications_lease_idx
    ON notifications (locked_until)
    WHERE status = 'processing';

-- List endpoint: filter by status/channel, keyset-paginate on (created_at, id).
CREATE INDEX notifications_list_idx
    ON notifications (status, channel, created_at DESC, id DESC);
CREATE INDEX notifications_created_idx
    ON notifications (created_at DESC, id DESC);

-- Batch detail endpoint.
CREATE INDEX notifications_batch_idx
    ON notifications (batch_id, created_at, id)
    WHERE batch_id IS NOT NULL;

-- Keep updated_at honest regardless of which code path issues the UPDATE.
CREATE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notifications_set_updated_at
    BEFORE UPDATE ON notifications
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

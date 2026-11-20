# Architecture Decision Records

Short ADR-style entries: **context → decision → alternatives → consequences**.
Entries are added as the system is built; later entries may refine earlier ones.

---

## ADR-001: PostgreSQL as both source of truth and work queue

**Context.** Requests must be accepted fast and processed asynchronously, with
priorities, scheduled sends, retry delays, cancellation and status queries. The
scope is a ~12 hour build that reviewers must be able to run with
`docker compose up`.

**Decision.** Store notifications in Postgres and use the same table as the
queue. Workers claim work with
`SELECT ... FOR UPDATE SKIP LOCKED` inside a short transaction, mark the rows
`processing` with a lease (`locked_until`), commit, and only then call the
provider.

**Why it fits this scope.**
- **No dual-write problem.** Creating a notification *is* enqueuing it: one
  `INSERT` in one transaction. With a separate broker we would need to write the
  row and publish a message atomically (outbox pattern) or risk rows with no
  message, or messages with no row.
- **Priority, scheduling and retry delays are just columns.** `ORDER BY
  priority_rank, next_attempt_at` and `WHERE next_attempt_at <= now()` give us
  priority queues, delayed delivery and backoff without extra infrastructure.
  Kafka has no per-message delay or priority; RabbitMQ needs plugins or
  per-delay queues.
- **Cancellation is a conditional `UPDATE`.** With a broker, a cancelled
  message is still in the queue and every consumer must check a side table.
- **`SKIP LOCKED`** lets many workers claim disjoint rows without blocking each
  other.
- **One fewer moving part** for reviewers to run, monitor and reason about.

**Throughput limits (honest numbers).** A single well-tuned Postgres instance
typically sustains a few thousand claim+complete cycles per second with batched
claims. Limits come from write amplification (each notification is updated
several times: claim, complete, attempt row), WAL volume, index maintenance on
a hot table, and vacuum pressure from dead tuples. Mitigations in this design:
batched claims, a *partial* claim index that only covers claimable rows, and a
short claim transaction. Insider One's "millions per day" averages to tens of
messages per second but bursts are much higher; that is where this design
starts to strain.

**How it would evolve at Insider One scale.**
1. **Transactional outbox → Kafka.** The API still writes the notification and
   an outbox row in one transaction; a relay (or Debezium CDC) publishes to
   Kafka. Postgres stays the source of truth for status queries.
2. **Topic per channel, partitioned by recipient/tenant.** Per-channel topics
   isolate a slow SMS provider from email. Partitioning by recipient preserves
   per-recipient ordering; consumer groups scale workers horizontally.
3. **Priority via separate topics** (`sms.high`, `sms.normal`, `sms.low`) with
   weighted consumption, since Kafka has no native priority.
4. **Delays/retries via retry topics** (`sms.retry.30s`, `sms.retry.5m`) or a
   delay scheduler, plus a DLQ topic for exhausted messages.
5. **Partition the notifications table** by `created_at` (monthly) and archive
   old partitions, so the hot set stays small.

**Consequences.** Simple, correct and transactional for this scope; the queue
logic lives in a few SQL statements that are easy to test. Throughput ceiling is
lower than a dedicated broker, and the table needs vacuum attention under
sustained load.

---

## ADR-002: Scheduling and retries are folded into `next_attempt_at`

**Context.** Three kinds of rows become claimable at a point in time: new
(`pending`, due now), scheduled (`scheduled`, due at `scheduled_at`) and
retrying (`retrying`, due after backoff).

**Decision.** Every claimable row has a `next_attempt_at`. The claim query
selects `status IN ('scheduled','pending','retrying') AND next_attempt_at <=
now()`. There is no separate scheduler loop that flips `scheduled → pending` or
`retrying → pending`.

**Alternatives.** A periodic scheduler loop promoting rows to `pending` (the
spec allows either). It adds a moving part, a polling interval that delays
sends, and another write per row.

**Consequences.** One index, one query, no promotion lag. The statuses
`scheduled` and `retrying` remain visible to API clients, so they can still see
*why* a notification has not been sent yet. In the state diagram,
`scheduled → processing` and `retrying → processing` happen directly when due;
semantically they pass through "pending" for zero time.

---

## ADR-003: Text columns with CHECK constraints, plus a generated `priority_rank`

**Context.** Channel, priority, status and outcome are small closed sets.

**Decision.** Store them as `text` with `CHECK (... IN (...))` constraints.
Priority additionally has a `GENERATED ALWAYS AS ... STORED` `priority_rank`
smallint (high=0, normal=1, low=2).

**Alternatives.** Postgres `ENUM` types: adding or reordering values needs
`ALTER TYPE` and cannot run inside a transaction in older versions, which makes
migrations awkward. Storing priority as an integer only: less readable in SQL
and API payloads.

**Consequences.** Values are readable in `psql` and in logs; the database still
rejects bad values; the claim index sorts on a compact integer. Text sorting
would have put `high < low < normal`, which is why the rank exists.

---

## ADR-004: Partial indexes tailored to each query

**Decision.**
- `notifications_claim_idx (channel, priority_rank, next_attempt_at) WHERE status
  IN ('scheduled','pending','retrying')`: exactly matches the claim query.
  Because it is partial, it only contains the (small) backlog, not the millions
  of `sent` rows, so it stays in memory and is cheap to maintain.
- `notifications_lease_idx (locked_until) WHERE status = 'processing'`: lets the
  reaper find expired leases without scanning.
- `(status, channel, created_at DESC, id DESC)` and `(created_at DESC, id DESC)`
  for the list endpoint's filters and keyset pagination.
- `(batch_id, created_at, id) WHERE batch_id IS NOT NULL` for batch details.

**Alternatives.** The spec's suggested `(status, channel, priority,
next_attempt_at)` full index works, but indexes every historical row and sorts
priority as text.

**Consequences.** More indexes slow writes slightly; each one is justified by a
specific query.

---

## ADR-005: Migrations run as a one-shot compose service

**Context.** Schema must exist before `api` and `worker` start.

**Decision.** Versioned SQL files in `/migrations` (golang-migrate format). In
compose, a `migrate` service runs `migrate up` and exits; `api` and `worker`
depend on it with `condition: service_completed_successfully`.

**Alternatives.** Running migrations inside the API on startup: couples schema
changes to a specific binary and races when several replicas start (golang-migrate's
advisory lock makes it safe, but it is still surprising). In Kubernetes the
equivalent of our approach is an init container or a pre-deploy Job.

**Consequences.** App binaries stay simple and never need DDL privileges. The
same SQL files are applied by the integration tests.

---

## ADR-006: Minimal dependencies, standard library first

**Decision.** `net/http` with Go 1.22 routing patterns (`GET /notifications/{id}`)
and hand-written middleware; `log/slog` for JSON logs; environment parsing by
hand. External modules only where they clearly earn their place: `pgx/v5`
(fastest, most complete Postgres driver), `go-redis/v9`, `google/uuid`,
`prometheus/client_golang`, `golang-migrate`, `testcontainers-go`.

**Alternatives.** `chi`/`gin` for routing, `viper`/`envconfig` for config. Since
Go 1.22 the standard mux handles methods and path parameters, so a router
dependency adds little.

**Consequences.** Less to audit and upgrade; a reviewer can read every line of
middleware.

---

## ADR-007: Configuration from environment variables only

**Decision.** All settings come from env vars with working defaults
(`.env.example` documents them). `config.Load` takes a lookup function instead
of reading `os.Getenv` directly, and collects *all* parse errors before failing.

**Consequences.** 12-factor friendly (same image in every environment);
unit-testable without touching the process environment; a misconfigured deploy
reports every problem in one log line rather than one per restart.

---

## ADR-008: Separate liveness and readiness probes

**Decision.** `/health/live` returns 200 whenever the process can serve HTTP.
`/health/ready` pings Postgres and Redis concurrently with a 2s timeout and
returns 503 if any fails. The worker serves the same endpoints on an admin port
(`:8082`).

**Why.** If liveness checked the database, a Postgres outage would make the
orchestrator restart every pod in a loop: restarts cannot fix the database and
only add load when it recovers. Readiness is the right signal to stop routing
traffic.

---

## ADR-009: A local mock provider with failure injection

**Context.** The real target is webhook.site, whose free tier limits request
volume, and which can't easily produce 503/429 on demand.

**Decision.** `cmd/mockprovider` mimics the contract (202 + `{messageId, status,
timestamp}`) with configurable latency range, random failure rate, failure
status codes and an optional `Retry-After`. Content containing `[mock:<code>]`
(e.g. `[mock:400]`) deterministically returns that status, for demos.

**Consequences.** Reviewers can demo retries, permanent failures and the circuit
breaker offline. Integration tests use an in-process `httptest` server instead,
so they are fully deterministic.

---

## ADR-010: Small static binaries on a distroless base image

**Decision.** One multi-stage `Dockerfile` builds all three binaries with
`CGO_ENABLED=0 -trimpath -ldflags="-s -w"` and copies them to
`gcr.io/distroless/static-debian12:nonroot`. Each compose service selects its
binary with `command`.

**Consequences.** Images are ~10-15 MB, run as non-root, and contain no shell or
package manager (smaller attack surface). Trade-off: no `curl` inside the
container for healthchecks; health is checked from outside instead.

---

## ADR-011: Idempotency via a unique key plus a normalized request hash

**Context.** Clients retry on timeouts. A retried create must not send a
second SMS, and a client bug that reuses a key for a different message must
not be silently swallowed.

**Decision.**
- `Idempotency-Key` header (optional, at most 255 chars) is stored in a `UNIQUE`
  column alongside `request_hash`, a SHA-256 of the *normalized* payload
  (defaults applied, `scheduled_at` converted to UTC). So omitting `priority`
  and sending `"normal"` count as the same request.
- Insert with `ON CONFLICT (idempotency_key) DO NOTHING RETURNING ...`. If no
  row comes back, load the existing row and compare hashes:
  same hash → **200** with the original resource and `Idempotent-Replayed:
  true`; different hash → **409 `idempotency_conflict`**.
- New resources return **201** with a `Location` header.

**Alternatives.** SELECT-then-INSERT has a race: two concurrent retries both
see "no row" and both insert. A Redis `SETNX` lock adds a second system that
can disagree with the database. Storing the whole request instead of a hash
duplicates message content.

**Consequences.** Correct under concurrency (an integration test fires 20
parallel requests with one key and gets exactly one row). Keys are global (no
tenants in this scope) and never expire. In production we would scope them
per API client and purge keys after ~24h, as Stripe does.

---

## ADR-012: Batches are all-or-nothing, written with COPY in one transaction

**Decision.**
- Validate every item first. If any are invalid, return **422** whose
  `details` lists every failing `index` + `field`, and write nothing.
- Insert the `batches` row and all notifications in **one transaction**, using
  `COPY` for the notifications (one round trip for up to 1000 rows). IDs are
  generated in the application, so `COPY`'s lack of `RETURNING` doesn't matter.
- The batch accepts its own `Idempotency-Key` (migration 000005). A retried
  batch returns the original `batch_id` and item ids. Per-item keys inside a
  batch are not supported.
- Size limit 1..`BATCH_MAX_SIZE` (default 1000), request body limit 16 MiB.

**Alternatives.** Partial acceptance (insert valid items, report invalid
ones): clients must then diff the response to decide what to resend, and a
retry risks duplicating the accepted half. Multi-row `INSERT`: works, but it is one huge
statement with 11,000 bind parameters to parse and plan, and noticeably slower
than streaming rows with `COPY`.

**Consequences.** Simple client semantics: either the batch got in or nothing
did. A batch with one typo is rejected entirely, which is the safer default for
marketing sends.

**Note on migrations.** Batch idempotency arrived after 000002 was committed,
so it is a new migration (000005) rather than an edit. Applied migrations are
immutable; editing them would leave existing databases out of sync.

---

## ADR-013: Keyset (cursor) pagination on `(created_at, id)`

**Decision.** `GET /notifications` and `GET /batches/{id}` return
`{ "data": [...], "next_cursor": "..." | null }`. The cursor is an opaque
base64url token holding the last row's `(created_at, id)`. The next page uses
`WHERE (created_at, id) < ($t, $id) ORDER BY created_at DESC, id DESC`. We
fetch `limit+1` rows to know if another page exists, without a `COUNT(*)`.

**Why not OFFSET.**
1. *Performance:* `OFFSET 100000` makes Postgres read and discard 100k rows,
   so deep pages get linearly slower. A keyset seeks directly through the
   `(created_at DESC, id DESC)` index.
2. *Correctness:* this table receives inserts constantly. With OFFSET, a row
   inserted while a client pages shifts every later page, so the client sees
   duplicates or misses rows. Keyset positions are stable (tested).

`id` is the tie-breaker because every row of a batch shares one `created_at`
(the same transaction). Cost: no "jump to page 37", which an API consumer
rarely needs.

---

## ADR-014: UUIDv7 primary keys generated by the application

**Decision.** IDs are UUIDv7 (time-ordered), generated in Go before insert.

**Why.** Random UUIDv4 keys land all over the primary-key B-tree, causing page
splits and poor cache locality on a write-heavy table. v7 keys are appended
near the right edge, like a sequence, but are still globally unique and safe to
expose (unlike a sequential bigint, they don't reveal volume). Generating them
in the app lets batch inserts use `COPY` and lets us return ids without
`RETURNING`. `google/uuid` makes v7 ids monotonic within a process, so a
batch's id order equals its request order (unit-tested).

---

## ADR-015: Cancellation is a conditional UPDATE; `processing` is not cancellable

**Decision.**
```sql
UPDATE notifications SET status = 'cancelled'
WHERE id = $1 AND status IN ('scheduled','pending','retrying')
RETURNING ...
```
Zero rows means not found (404) or not cancellable (409 `invalid_state`, and
the message names the current status).

**Why.** The status check and the write are one atomic statement, so a cancel
can never race a worker's claim (which is also a conditional UPDATE). An
integration test runs the two concurrently 30 times and asserts exactly one
wins each time. `processing` is excluded because the request may already be at
the provider; claiming we cancelled it would be a lie.

---

## ADR-016: HTTP layer conventions

- **One error shape** everywhere: `{ "error": { "code", "message", "details" },
  "correlation_id" }`, produced by one mapper (`httpapi/errors.go`). Unknown
  errors are logged with the correlation ID and returned as a generic 500, so
  SQL or hostnames never leak.
- **400 vs 422:** 400 when the request can't be parsed (bad JSON, unknown field,
  bad query parameter); 422 when it parses but breaks a business rule.
- **Strict JSON:** unknown fields are rejected (`DisallowUnknownFields`), so
  `"recipent"` fails loudly instead of being silently ignored. Bodies are
  size-limited (1 MiB single, 16 MiB batch).
- **Correlation ID:** reuse the incoming `X-Correlation-ID` only if it matches
  `[A-Za-z0-9._:-]{1,128}`, otherwise generate a UUID. The value is written to
  logs and the database, so untrusted input is restricted (no log injection).
- **Timestamps are always UTC** in responses (pgx returns the server's local
  zone otherwise).
- **Thin handlers, application service in the middle:** handlers decode and
  encode; `internal/notification.Service` validates, applies defaults and
  enforces idempotency; `internal/store` owns SQL. The service depends on a
  `Store` interface defined on the consumer side, so its decision logic is unit
  tested with a fake.

---

## ADR-017: Claiming work: one short statement, bounded by free send slots

**Decision.** Each channel has one claim loop and a semaphore of
`WORKER_CONCURRENCY` send slots. The loop waits for at least one free slot,
grabs up to `CLAIM_BATCH_SIZE` free slots, and claims exactly that many rows:

```sql
WITH due AS (
  SELECT id FROM notifications
  WHERE status IN ('scheduled','pending','retrying') AND channel = $1
    AND next_attempt_at <= now()
  ORDER BY priority_rank, next_attempt_at
  LIMIT $2
  FOR UPDATE SKIP LOCKED)
UPDATE notifications n
SET status = 'processing', attempts = n.attempts + 1,
    locked_until = now() + <lease>
FROM due WHERE n.id = due.id
RETURNING ...
```

Each claimed row gets its own goroutine; the slot is freed when the send is
recorded. An empty claim sleeps `POLL_INTERVAL` (500ms) before trying again.

**Why.**
- **Never hold a transaction across HTTP.** The statement commits in
  milliseconds; the provider call happens afterwards. Holding row locks for a
  5-second provider call would pin connections and block vacuum.
- **SKIP LOCKED** lets N workers (goroutines or replicas) claim disjoint rows
  without waiting on each other. Tested: two workers, 300 notifications, each
  delivered exactly once.
- **Claiming only as many rows as there are free slots** means a claimed row
  never sits waiting for a slot while its lease ticks down.
- **Per-channel loops** isolate channels: a slow SMS provider cannot starve
  email sends.

**Alternatives.** One goroutine per slot each claiming one row (simpler, but
one query per message and N idle pollers per channel). `LISTEN/NOTIFY` to wake
idle workers instantly instead of polling: worth adding if 500ms latency
matters; polling is simpler and self-healing.

---

## ADR-018: Leases, fencing and the reaper: delivery is at-least-once

**Context.** A worker can crash (OOM, `kill -9`, node loss) after claiming a
row, possibly after the provider received the message but before we recorded
it.

**Decision.**
- A claim sets `locked_until = now() + LEASE_DURATION` (30s). A reaper loop
  in every worker (every `REAPER_INTERVAL`, 10s) moves `processing` rows with an
  expired lease back to `pending`, or to `failed` if the attempt budget is
  spent. The reaper is idempotent, so running it on every replica is safe.
- **Fencing token:** the claim increments `attempts`, and every completion
  UPDATE requires `status = 'processing' AND attempts = <my attempt>`. A worker
  that stalls past its lease cannot overwrite the result of the worker that
  re-claimed the row; it gets `ErrLeaseLost`. Its attempt is still written to
  `delivery_attempts`, because the call really happened. (Tested.)
- `LEASE_DURATION >= 2 x PROVIDER_TIMEOUT` is enforced at startup, so a healthy
  send can never outlive its lease.

**Consequence: at-least-once delivery.** If a worker dies after the provider
accepted a message but before the result was committed, the reaper re-queues
the row and it is sent again. Verified manually: `kill -9` with 8 sends in
flight → all 8 re-queued after the lease and delivered again (twice in total).
At-most-once (mark sent *before* calling) would silently lose messages
instead, which is worse for OTPs and order updates.

**How duplicates are minimised.**
1. API idempotency keys stop clients from creating duplicates (ADR-011).
2. Conditional, fenced state transitions stop *our own* components from
   double-processing a row (claim, cancel, complete, reaper).
3. Every provider request carries `X-Notification-ID: <our uuid>`, a stable
   idempotency key a real provider (Twilio, SES, FCM) can use to drop the
   duplicate on its side. This is the only way to get effectively-once
   delivery end to end.
4. The window is small: only crashes between the provider's answer and our
   commit (milliseconds) produce duplicates.

---

## ADR-019: Error classification and attempt accounting

**Decision.**
- *Retryable:* no HTTP response (connection errors, DNS, per-attempt timeout,
  cancellation), 5xx, 429, 408.
- *Permanent:* any other 4xx, plus unexpected 1xx/3xx. These are failed
  immediately: an identical request cannot succeed, and retrying wastes quota
  and can get us rate-limited or flagged by the provider.
- *Success:* any 2xx. The brief's provider returns 202; webhook.site defaults to
  200, so we accept both and store `messageId` when present.
- An attempt is counted when it is *claimed* (that number is the fencing
  token). A job released on graceful shutdown before its send started has the
  attempt refunded. A job reaped after a crash keeps it, because we can't know
  whether the provider got it.
- `Retry-After` (seconds or HTTP-date) is honoured when it asks for longer than
  our own delay.

Phase 3 uses a fixed `RETRY_BASE_DELAY`; exponential backoff with jitter
replaces it later behind the same `RetryPolicy.Decide` call.

---

## ADR-020: Graceful shutdown

**Decision.** On SIGTERM the worker:
1. stops claiming (claim loops exit),
2. lets in-flight provider calls finish, for up to `SHUTDOWN_TIMEOUT` (15s),
3. then cancels the rest. A cancelled call is recorded as a retryable attempt
   (it may have reached the provider), not silently dropped,
4. releases any job whose send had not started (attempt refunded),
5. keeps the admin/health server up until the drain finishes.

Provider calls run on a context derived with `context.WithoutCancel`, so the
signal itself doesn't abort a call that is about to succeed. Result writes use
a fresh 5s context, so a result is recorded even during a hard stop.
`SHUTDOWN_TIMEOUT > PROVIDER_TIMEOUT` is enforced so a normal call always
fits, and compose's `stop_grace_period` (20s) exceeds `SHUTDOWN_TIMEOUT`.
Verified manually: SIGTERM with 8 sends in flight → all 8 recorded `sent`
before exit.

---

## ADR-021: Distributed rate limiting with a Redis token bucket (Lua)

**Context.** Each channel's provider allows ~100 msg/s. The limit must hold
across *all* worker replicas, not per process, and waiting workers must not
spin.

**Decision.** One token bucket per channel in Redis (`ratelimit:<channel>`,
a hash of `tokens` and `ts`), updated by a Lua script that Redis runs
atomically:
1. Read the bucket, refill `elapsed x rate` tokens (capped at `burst`).
2. Grant `min(requested, floor(tokens))`. A **partial grant** lets a worker
   with 10 free slots still send 3 if only 3 tokens exist.
3. If nothing was granted, return `wait_ms` = time until the next whole
   token. The caller sleeps exactly that long, so a waiting worker costs
   one Redis call per token rather than a busy loop. (Tested: 20 concurrent
   waiters made 230 calls for 210 tokens.)
4. `PEXPIRE` the key once an idle bucket would be full anyway.

The script reads **Redis's clock** (`TIME`), not the caller's, so clock skew
between worker hosts cannot inflate the rate.

**Where it sits in the worker.** Tokens are acquired *before* claiming rows,
and the claim asks for at most as many rows as tokens granted. Unused tokens
(fewer rows were due) are refunded. If tokens were taken *after* claiming, a
claimed row would wait for a token while its lease ticks down, and a backlog
would pile up as `processing` rows that are not actually being processed.

**Defaults.** 100 tokens/s per channel (`RATE_LIMIT_<CHANNEL>_PER_SECOND`),
burst 10 (`RATE_LIMIT_BURST`). The burst is deliberately small. With
burst = 100, a provider that counts fixed one-second windows could see up to
~200 requests in one window right after an idle period and start returning
429s. Burst 10 keeps any one-second window under ~110.

**Measured.** 4 limiter instances x 5 goroutines over 2s: exactly 210 grants
(100/s x 2s + burst 10). 2 full worker replicas against 500 queued SMS: 209
provider calls in 2s.

**Failure mode: fail closed.** If Redis is unreachable, the channel pauses
(logged, retried every poll interval) instead of sending unthrottled. Getting
throttled or banned by the provider is worse than a short pause, and the
worker's `/health/ready` turns red so the outage is visible.

**Alternatives.**
- *Fixed window counter* (`INCR` + `EXPIRE`): simplest, but allows 2x the rate
  across a window boundary.
- *Sliding log* (sorted set of timestamps): exact, but memory grows with the
  rate.
- *GCRA / `redis-cell`*: elegant, but needs a Redis module.
- *Per-process limiter (`x/time/rate`)*: needs `rate / replicas` per process
  and breaks whenever replicas scale.

Token bucket in Lua is exact enough, O(1) memory, and runs on plain Redis 7.

**Trade-offs.** Each claim cycle costs 1-2 extra Redis round trips (acquire,
maybe refund), negligible next to the provider call. An idle worker briefly
holds tokens between acquire and refund; with burst 10 that can delay another
replica by a few milliseconds.

---

## ADR-022: Strict priority ordering (and how to avoid starvation)

**Decision.** The claim query orders by `priority_rank, next_attempt_at`:
every due `high` notification on a channel goes before any `normal`, every
`normal` before any `low`. Within a priority it is FIFO by due time. Rate
limiting makes this matter: during a flash-sale burst the channel is capped at
100/s, and strict priority decides *which* 100 go first (OTPs and
transactional messages before marketing).

**Evidence that it is cheap.** `EXPLAIN ANALYZE` on 205k rows (200k sent,
5k pending): an index scan on the partial `notifications_claim_idx` with no
Sort node (the index supplies the order), 0.05 ms. The index is 240 kB, against
18 MB for the full list index, because it only holds the backlog (ADR-004).

**Trade-off: starvation.** If `high` traffic alone saturates a channel's rate
limit for a long time, `low` notifications are never claimed. For Insider
One's mix (bursts, not sustained saturation) that is acceptable and is the
behaviour product teams usually expect: marketing waits, OTPs don't.

**Alternatives if starvation becomes real.**
1. *Weighted fair claiming:* each claim cycle splits its tokens across
   priorities by weight (e.g. 70/20/10) with one `priority_rank = $x` claim
   per priority, giving unused share to the others. Keeps index-ordered
   claims and guarantees `low` a minimum share.
2. *Aging:* treat a row as one priority higher for every N minutes it has
   waited. Easy to express in SQL, but the computed sort key can no longer be
   served by the index, so every claim sorts the whole backlog.
3. *Separate queues/topics per priority* in a Kafka design (ADR-001), with
   consumer quotas per topic.

Option 1 is the natural next step here: a small change in the claim loop and
no schema change.

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

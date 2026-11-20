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

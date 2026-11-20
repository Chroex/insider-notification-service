# Notification Service

An event-driven notification service in Go. It accepts SMS, Email and Push
requests over HTTP, stores them in PostgreSQL (which doubles as the work
queue), and delivers them asynchronously through workers that respect
per-channel rate limits, prioritise urgent messages, retry failures with
backoff, and stop calling a failing provider.

- **Fast, safe intake:** validation, idempotency keys, batches of up to 1000
  written in one transaction, and reusable message templates.
- **Asynchronous delivery:** `SELECT ... FOR UPDATE SKIP LOCKED` work queue
  with leases, crash recovery and graceful shutdown.
- **Provider protection:** a Redis token bucket (100 msg/s per channel, shared
  across replicas) and a per-channel circuit breaker.
- **Smart retries:** error classification, exponential backoff with full
  jitter, `Retry-After`, and a dead-letter set with manual redrive.
- **Observability:** Prometheus metrics, a human-readable summary endpoint,
  structured JSON logs, and correlation IDs from request to provider.

Every significant choice is explained in [docs/DECISIONS.md](docs/DECISIONS.md).

---

## Contents

1. [Quick start](#quick-start)
2. [Architecture](#architecture)
3. [Request lifecycle](#request-lifecycle)
4. [Retry policy](#retry-policy)
5. [Rate limiting and priority](#rate-limiting-and-priority)
6. [Observability](#observability)
7. [API and curl examples](#api-and-curl-examples)
8. [Configuration](#configuration)
9. [Running tests](#running-tests)
10. [Using webhook.site as the provider](#using-webhooksite-as-the-provider)
11. [Project layout](#project-layout)
12. [Assumptions](#assumptions)
13. [Trade-offs and future work](#trade-offs-and-future-work)

---

## Quick start

Requirements: Docker with Compose v2. Go 1.22+ only if you want to run tests
or binaries outside Docker.

```bash
docker compose up --build        # or: make up  (detached)
make seed                        # send a demo batch: every channel and priority, plus 2 forced failures
```

| Service       | URL                                       | Notes |
|---------------|-------------------------------------------|-------|
| API           | http://localhost:8080                     | `/api/v1/...`, `/metrics`, `/health/*` |
| Swagger UI    | http://localhost:8081                     | Interactive docs for [api/openapi.yaml](api/openapi.yaml) |
| Worker admin  | http://localhost:8082                     | `/metrics`, `/health/live`, `/health/ready` |
| Mock provider | http://localhost:8090/send                | Stand-in for webhook.site, with failure injection |
| Postgres      | `localhost:5432`                          | user / password / db: `notifications` |
| Redis         | `localhost:6379`                          | Rate-limit buckets |

`docker compose up` starts postgres and redis, runs the `migrate` one-shot
container, and starts `api`, `worker`, `mockprovider` and `swagger-ui` once the
schema is in place. `make down` stops everything and deletes the volumes.

---

## Architecture

```mermaid
flowchart LR
    client([API client])
    subgraph api_box [cmd/api]
        api[HTTP API<br/>validate · idempotency · enqueue]
    end
    subgraph worker_box [cmd/worker x N replicas]
        claim[Claim loop per channel<br/>breaker → rate limit → claim]
        send[Send slots<br/>provider call + record]
        reaper[Lease reaper]
    end
    pg[(PostgreSQL<br/>notifications = queue<br/>batches · attempts)]
    redis[(Redis<br/>token bucket per channel)]
    provider[[Provider<br/>webhook.site / mockprovider]]
    prom[(Prometheus)]

    client -- "POST /api/v1/notifications" --> api
    api -- "INSERT (status = pending)" --> pg
    claim -- "SELECT … FOR UPDATE SKIP LOCKED" --> pg
    claim -- "acquire tokens (Lua)" --> redis
    claim --> send
    send -- "POST {to, channel, content}<br/>X-Correlation-ID, X-Notification-ID" --> provider
    send -- "record attempt + status" --> pg
    reaper -- "re-queue expired leases" --> pg
    prom -. scrape /metrics .-> api
    prom -. scrape /metrics .-> worker_box
```

Two binaries from one Go module, plus a mock:

- **`cmd/api`** validates, applies defaults, enforces idempotency and inserts
  rows with status `pending`. That insert *is* the enqueue, so there is no
  dual write to a broker. It never calls the provider.
- **`cmd/worker`** runs one claim loop per channel. Each iteration asks the
  circuit breaker for permission, waits for free send slots, takes tokens from
  the Redis rate limiter, then claims at most that many due rows (high priority
  first) in one short statement that commits **before** any HTTP call. Each row
  is sent in its own goroutine; the result and the attempt are written in one
  transaction.
- **`cmd/mockprovider`** mimics webhook.site (202 + `{messageId, status,
  timestamp}`) with configurable latency and failure rate.

Why PostgreSQL rather than Kafka or RabbitMQ for the queue, and how this would
evolve at Insider One scale: [ADR-001](docs/DECISIONS.md#adr-001-postgresql-as-both-source-of-truth-and-work-queue).

---

## Request lifecycle

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as API
    participant P as PostgreSQL
    participant W as Worker
    participant R as Redis
    participant X as Provider

    C->>A: POST /api/v1/notifications (Idempotency-Key, X-Correlation-ID)
    A->>A: validate (per-channel rules), apply defaults, hash payload
    A->>P: INSERT … ON CONFLICT (idempotency_key) DO NOTHING
    A-->>C: 201 Created (or 200 replay / 409 conflict / 422 invalid)

    loop every free slot, per channel
        W->>W: circuit breaker allows sends?
        W->>R: take up to N tokens (atomic Lua)
        W->>P: claim ≤ tokens due rows, FOR UPDATE SKIP LOCKED<br/>status=processing, attempts+1, locked_until=now()+lease
    end
    W->>X: POST {to, channel, content} (5s timeout)
    X-->>W: 202 {messageId} / 4xx / 5xx / timeout
    W->>W: classify → sent | retrying (backoff) | failed
    W->>P: INSERT delivery_attempt + conditional UPDATE (fenced by attempt number)
    C->>A: GET /api/v1/notifications/{id}
    A-->>C: status + full attempt history
```

**Statuses**

```mermaid
stateDiagram-v2
    [*] --> scheduled: scheduled_at in the future
    [*] --> pending
    scheduled --> processing: due, claimed
    pending --> processing: claimed
    retrying --> processing: backoff elapsed, claimed
    processing --> sent: 2xx
    processing --> retrying: retryable error, attempts left
    processing --> failed: permanent error / attempts exhausted
    processing --> pending: lease expired (reaper) / released on shutdown
    scheduled --> cancelled: POST …/cancel
    pending --> cancelled: POST …/cancel
    retrying --> cancelled: POST …/cancel
    failed --> pending: POST …/retry
    sent --> [*]
    cancelled --> [*]
```

`scheduled` and `retrying` rows are claimed directly once `next_attempt_at` is
due, so no separate scheduler loop is needed ([ADR-002](docs/DECISIONS.md#adr-002-scheduling-and-retries-are-folded-into-next_attempt_at)).
Every transition is a conditional `UPDATE … WHERE status IN (…)`, so the API,
workers and the reaper can never overwrite each other.

**Delivery is at-least-once.** If a worker dies after the provider accepted a
message but before the result is committed, the reaper re-queues the row when
its lease expires and it is sent again. Duplicates are minimised by idempotency
keys, fenced state transitions, and the `X-Notification-ID` header, which a
real provider can use to deduplicate ([ADR-018](docs/DECISIONS.md#adr-018-leases-fencing-and-the-reaper-delivery-is-at-least-once)).

---

## Retry policy

| Outcome of a provider call | Classified as | What happens |
|---|---|---|
| 2xx | success | `sent`, `provider_message_id` stored |
| 4xx except 408/429 | permanent | `failed` immediately, never retried |
| 5xx, 408, 429, timeout, network error | retryable | `retrying` with backoff, until `MAX_ATTEMPTS` (5) |

- **Backoff:** exponential with full jitter. After the k-th failure the delay is
  uniform in `[0, min(5m, 2s x 2^(k-1)))`, so ceilings run 2s, 4s, 8s, 16s.
  Random spread stops a provider outage from turning into synchronized retry
  storms. A `Retry-After` header wins when it asks for longer (capped at 1h).
- **Per-attempt timeout:** 5s (`PROVIDER_TIMEOUT`), applied via context.
- **Exhaustion:** `failed` with `last_error`. Failed notifications are the
  dead-letter set (`GET /api/v1/notifications?status=failed`).
  `POST /api/v1/notifications/{id}/retry` re-queues one with a fresh budget.
- **Circuit breaker (per channel):** 5 consecutive retryable failures open it.
  For 30s nothing is claimed on that channel, so queued notifications keep
  their attempts; then a single probe decides whether to close it again. A 4xx
  doesn't count against the provider: it answered.
- **Every attempt** is recorded in `delivery_attempts` (HTTP status, error,
  duration, outcome) and shown by `GET /api/v1/notifications/{id}`.

Backoff handles brief blips; the breaker handles outages. Together they keep a
30-minute provider outage from dead-lettering the whole backlog
([ADR-019](docs/DECISIONS.md#adr-019-error-classification-and-attempt-accounting),
[ADR-023](docs/DECISIONS.md#adr-023-exponential-backoff-with-full-jitter-honouring-retry-after),
[ADR-024](docs/DECISIONS.md#adr-024-circuit-breaker-per-channel),
[ADR-025](docs/DECISIONS.md#adr-025-failed-notifications-are-the-dead-letter-set-manual-retry)).

---

## Rate limiting and priority

- **Rate limit:** a token bucket per channel in Redis, updated by an atomic Lua
  script using Redis's own clock, so the limit holds across any number of
  worker replicas. Defaults: 100 msg/s per channel, burst 10. Workers take
  tokens *before* claiming rows and sleep exactly until the next token is due
  (no spinning). Measured: 100.0 sends/s per channel end to end. If Redis is
  down, sending pauses rather than running unthrottled
  ([ADR-021](docs/DECISIONS.md#adr-021-distributed-rate-limiting-with-a-redis-token-bucket-lua)).
- **Priority:** strict. Every due `high` goes before any `normal`, and every
  `normal` before any `low`; FIFO within a priority. Served by a partial index
  with no sort step (0.05 ms on 205k rows). Sustained `high` saturation can
  starve `low`; the weighted-fair alternative is described in
  [ADR-022](docs/DECISIONS.md#adr-022-strict-priority-ordering-and-how-to-avoid-starvation).

---

## Observability

- **Prometheus** at `GET /metrics` on the API (`:8080`) and the worker admin port
  (`:8082`): `notifications_created_total`, `notifications_sent_total`,
  `notifications_failed_total{reason}`, `delivery_attempts_total{outcome}`,
  `delivery_latency_seconds`, `end_to_end_latency_seconds`,
  `queue_depth{channel,priority,status}`, `circuit_breaker_state`, plus HTTP and
  Go runtime metrics.
- **Summary for humans:** `GET /api/v1/metrics/summary?window=15m` returns queue
  depth, success rates and p50/p95 latencies, overall and per channel. It is
  computed from Postgres, so it covers every worker replica.
- **Logs:** JSON (`log/slog`) with consistent keys: `correlation_id`,
  `notification_id`, `batch_id`, `channel`, `attempt`.
- **Correlation:** send `X-Correlation-ID` (or one is generated). It is echoed
  in the response, written to every API and worker log line, stored on the
  notification, and sent to the provider.
- **Health:** `/health/live` (process up, never checks dependencies) and
  `/health/ready` (Postgres and Redis reachable) on both API and worker.

Details: [ADR-026](docs/DECISIONS.md#adr-026-observability-metrics-summary-and-correlation).

---

## API and curl examples

Full reference: **Swagger UI at http://localhost:8081** (spec:
[api/openapi.yaml](api/openapi.yaml)). Errors always look like:

```json
{ "error": { "code": "validation_failed", "message": "request validation failed",
             "details": [{ "field": "recipient", "message": "must be a phone number in E.164 format, e.g. +905551234567" }] },
  "correlation_id": "4f6c1f0e-0f43-4b52-9a5e-3d1c2b7f9a10" }
```

**Create a notification** (`201`; same key + same body → `200` with
`Idempotent-Replayed: true`; same key + different body → `409`):

```bash
curl -i -X POST localhost:8080/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-42-shipped' \
  -H 'X-Correlation-ID: checkout-7f3a' \
  -d '{"channel":"sms","recipient":"+905551234567","content":"Your order has shipped","priority":"high"}'
```

**Schedule one for later:**

```bash
curl -X POST localhost:8080/api/v1/notifications -H 'Content-Type: application/json' \
  -d '{"channel":"email","recipient":"user@example.com","content":"Weekly digest","scheduled_at":"2030-01-01T09:00:00Z"}'
```

**Create a batch** (1-1000 items, all-or-nothing; invalid items are reported by `index`):

```bash
curl -X POST localhost:8080/api/v1/notifications/batch -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: campaign-2026-11-flash' \
  -d '{"notifications":[
        {"channel":"sms","recipient":"+905551234567","content":"Flash sale starts now!","priority":"high"},
        {"channel":"email","recipient":"ayse@example.com","content":"Weekly digest","priority":"low"},
        {"channel":"push","recipient":"device-token-abc","content":"You left items in your cart"}]}'
```

**Create a template and send from it** (`{{.var}}` placeholders, Go
`text/template` syntax; missing variables and over-long rendered output are 422s):

```bash
curl -X POST localhost:8080/api/v1/templates -H 'Content-Type: application/json' \
  -d '{"name":"order-shipped-sms","channel":"sms","body":"Hi {{.first_name}}, order {{.order_id}} has shipped."}'
# -> {"id":"<template_id>", "variables":["first_name","order_id"], ...}

curl -X POST localhost:8080/api/v1/notifications -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","template_id":"<template_id>",
       "variables":{"first_name":"Ayşe","order_id":"TR-1042"}}'
# -> "content": "Hi Ayşe, order TR-1042 has shipped."

curl localhost:8080/api/v1/templates/<template_id>
```

**Get a notification with its attempt history:**

```bash
curl localhost:8080/api/v1/notifications/<id>
```

**List with filters and cursor pagination** (newest first; pass `next_cursor` back as `cursor`):

```bash
curl 'localhost:8080/api/v1/notifications?status=failed&channel=sms&limit=20'
curl 'localhost:8080/api/v1/notifications?from=2026-11-20T00:00:00Z&to=2026-11-21T00:00:00Z&cursor=<next_cursor>'
```

**Get a batch** (live status counts + paginated items):

```bash
curl 'localhost:8080/api/v1/batches/<batch_id>?limit=50'
```

**Cancel** (only from `scheduled`, `pending`, `retrying`; otherwise `409`):

```bash
curl -X POST localhost:8080/api/v1/notifications/<id>/cancel
```

**Retry a failed notification** (only from `failed`):

```bash
curl -X POST localhost:8080/api/v1/notifications/<id>/retry
```

**Metrics and health:**

```bash
curl 'localhost:8080/api/v1/metrics/summary?window=15m'
curl localhost:8080/metrics
curl localhost:8082/metrics          # worker: delivery metrics, queue_depth, circuit_breaker_state
curl localhost:8080/health/ready
curl localhost:8082/health/ready
```

**Demo failures with the mock provider:** content containing `[mock:<code>]`
always gets that status, e.g. `"content":"test [mock:400]"` fails permanently
after one attempt, and `[mock:503]` retries until it gives up.

---

## Configuration

Environment variables only, all optional; see [.env.example](.env.example).
Invalid values (or unsafe combinations) stop the binary at startup, with every
problem listed in one error.

| Variable | Default | Used by | Description |
|---|---|---|---|
| `LOG_LEVEL` | `info` | all | `debug`, `info`, `warn`, `error` |
| `DATABASE_URL` | `postgres://notifications:notifications@localhost:5432/notifications?sslmode=disable` | api, worker | Postgres connection |
| `DB_MAX_CONNS` | `10` | api, worker | Pool size per process |
| `REDIS_URL` | `redis://localhost:6379/0` | api, worker | Redis connection |
| `API_ADDR` | `:8080` | api | Listen address |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:8081` | api | Comma-separated browser origins (`*` = any) |
| `WORKER_ADMIN_ADDR` | `:8082` | worker | Health and metrics listen address |
| `SHUTDOWN_TIMEOUT` | `15s` | api, worker | Max wait for in-flight work on SIGTERM (must exceed `PROVIDER_TIMEOUT`) |
| `PROVIDER_URL` | `http://localhost:8090/send` | worker | Provider endpoint (compose: the mock) |
| `PROVIDER_TIMEOUT` | `5s` | worker | Per-attempt HTTP timeout |
| `SMS_MAX_CHARS` | `160` | api | SMS content limit (characters) |
| `EMAIL_MAX_CHARS` | `10000` | api | Email content limit |
| `PUSH_MAX_CHARS` | `256` | api | Push content limit |
| `PUSH_TOKEN_MAX_CHARS` | `512` | api | Push device token limit |
| `BATCH_MAX_SIZE` | `1000` | api | Items per batch request |
| `MAX_ATTEMPTS` | `5` | api | Delivery attempts per notification (also the budget added by manual retry) |
| `WORKER_CONCURRENCY` | `10` | worker | Parallel provider calls per channel |
| `CLAIM_BATCH_SIZE` | `10` | worker | Max rows per claim query |
| `POLL_INTERVAL` | `500ms` | worker | Idle wait when nothing is due |
| `LEASE_DURATION` | `30s` | worker | Claim lease (must be ≥ 2 x `PROVIDER_TIMEOUT`) |
| `REAPER_INTERVAL` | `10s` | worker | How often expired leases are recovered |
| `RETRY_BASE_DELAY` | `2s` | worker | Backoff ceiling after the first failure |
| `RETRY_BACKOFF_FACTOR` | `2` | worker | Ceiling multiplier per failed attempt |
| `RETRY_MAX_DELAY` | `5m` | worker | Backoff ceiling cap |
| `BREAKER_FAILURE_THRESHOLD` | `5` | worker | Consecutive retryable failures that open a channel's breaker |
| `BREAKER_COOLDOWN` | `30s` | worker | How long an open breaker pauses the channel |
| `RATE_LIMIT_SMS_PER_SECOND` | `100` | worker | SMS rate across all replicas |
| `RATE_LIMIT_EMAIL_PER_SECOND` | `100` | worker | Email rate across all replicas |
| `RATE_LIMIT_PUSH_PER_SECOND` | `100` | worker | Push rate across all replicas |
| `RATE_LIMIT_BURST` | `10` | worker | Token bucket capacity |
| `QUEUE_DEPTH_INTERVAL` | `15s` | worker | Refresh period of the `queue_depth` gauge |
| `MOCK_ADDR` | `:8090` | mockprovider | Listen address |
| `MOCK_LATENCY_MIN` / `MOCK_LATENCY_MAX` | `20ms` / `120ms` | mockprovider | Simulated latency range |
| `MOCK_FAILURE_RATE` | `0.1` | mockprovider | Probability (0-1) of an injected failure |
| `MOCK_FAILURE_CODES` | `500,503,429` | mockprovider | Status codes picked for injected failures |
| `MOCK_RETRY_AFTER` | `0s` | mockprovider | `Retry-After` sent with 429/503 (0 = none) |

Compose passes `LOG_LEVEL`, `PROVIDER_URL` and the `MOCK_*` values through from
your shell, e.g. `MOCK_FAILURE_RATE=0.5 docker compose up`.

---

## Running tests

```bash
make test-unit   # unit tests with -race; no Docker needed (~5s)
make test        # unit + integration tests with -race; needs Docker (~30s)
make lint        # go vet + golangci-lint (including integration-tagged files)
```

Integration tests (build tag `integration`) start disposable PostgreSQL 16 and
Redis 7 containers with **testcontainers-go** and use an in-process `httptest`
fake provider, so they are deterministic. They cover:

- create → worker sends → `sent`, with provider headers and body checked
- 503, 503, 202 → retried and sent, with 3 attempts recorded
- 400 → `failed` after one attempt, never retried; exhaustion after `max_attempts`
- cancel prevents sending; cancelling `sent`/`processing`/`failed` → 409; cancel vs. claim race
- the same Idempotency-Key never creates a duplicate (including 20 concurrent requests)
- high priority claimed and sent before older low priority
- the rate limiter caps two worker replicas near 100/s per channel
- two concurrent workers never send the same notification twice (300 notifications)
- crash recovery (reaper), stale workers can't overwrite newer claims, graceful shutdown
- `Retry-After` honoured; the circuit breaker opens, stops traffic, and recovers
- **every API response is validated against `api/openapi.yaml`**, and a unit
  test fails if the router and the spec list different endpoints

If Docker Desktop cannot pull testcontainers' helper image (Ryuk), run
`TESTCONTAINERS_RYUK_DISABLED=true make test`; the tests remove their own
containers.

---

## Using webhook.site as the provider

1. Open https://webhook.site and copy your unique URL
   (`https://webhook.site/<uuid>`).
2. In *Edit*, set the default response to status **202**, content type
   `application/json`, and a body such as
   `{"messageId": "demo-1", "status": "accepted", "timestamp": "2026-01-01T00:00:00Z"}`.
   Any 2xx is accepted, and `messageId` is stored when present (a static value
   is fine for testing).
3. Point the worker at it:

   ```bash
   PROVIDER_URL=https://webhook.site/<uuid> docker compose up -d worker
   make seed
   ```

   Requests arrive with the body `{"to", "channel", "content"}` and the headers
   `X-Correlation-ID` and `X-Notification-ID`.

webhook.site's free tier limits how many requests a URL accepts, which is why
load tests and the default setup use the local mock provider instead.

---

## Project layout

```
cmd/
  api/            HTTP API binary
  worker/         delivery engine binary (+ admin server)
  mockprovider/   local provider stand-in
internal/
  config/         env parsing and validation
  domain/         notification, statuses, channels, priorities, errors
  validation/     per-channel content and recipient rules
  notification/   application service: validation, defaults, idempotency
  store/          Postgres repositories (pgx), summary queries
  queue/          claim / complete / release / reap
  worker/         dispatcher, retry policy, backoff, circuit breaker
  provider/       provider HTTP client
  ratelimit/      Redis token bucket (Lua scripts)
  templating/     template compilation, variable discovery, rendering
  httpapi/        handlers, middleware, error mapping, CORS
  observability/  logging keys, Prometheus metrics, queue depth refresher
  health/         liveness and readiness
  platform/       connection bootstrapping, HTTP server lifecycle
  testutil/       testcontainers helpers (integration tag only)
migrations/       versioned SQL (golang-migrate)
api/openapi.yaml  OpenAPI 3 spec (contract-tested)
docs/DECISIONS.md architecture decision records
```

---

## Assumptions

Validation limits are configurable through environment variables:

| Channel | Recipient | Content limit |
|---------|-----------|---------------|
| `sms`   | E.164 phone number (`+905551234567`) | 160 characters (one GSM-7 segment) |
| `email` | Bare address (`user@example.com`, no display name, dotted domain) | 10,000 characters |
| `push`  | Device token: non-empty, no whitespace, at most 512 chars | 256 characters |

- Lengths count Unicode characters, not bytes. Real SMS encoding (GSM-7 160 vs
  UCS-2 70 characters per segment, multipart SMS) is future work.
- `priority` defaults to `normal`. `scheduled_at` must be in the future.
- A batch is all-or-nothing: one invalid item rejects the whole batch with a 422
  listing every failing index.
- Idempotency keys are global (no multi-tenancy) and do not expire. Batches
  take a batch-level key; individual items in a batch do not.
- A notification in `processing` cannot be cancelled: it may already be at the
  provider.
- Any 2xx from the provider counts as accepted (the brief says 202;
  webhook.site defaults to 200). `messageId` is stored when present.
- Rate limits are per channel and global across worker replicas. If Redis is
  down, sending pauses rather than running unthrottled.
- Priority is strict (high, then normal, then low).
- Templates are immutable (no update endpoint) and are rendered when the
  notification is created; the stored `content` is the final text.
- Delivery is **at-least-once**. Every request carries `X-Notification-ID` so
  a real provider can deduplicate.
- There is no authentication; in production the API would sit behind an API
  gateway with per-client keys, which would also scope idempotency keys.

---

## Trade-offs and future work

**Trade-offs made deliberately**

- *Postgres as the queue:* transactional enqueue and simple operations, at the
  cost of a throughput ceiling of a few thousand messages/s per database.
  The path to Kafka (outbox + topics per channel) is in ADR-001.
- *Strict priority* can starve `low` under sustained `high` load.
- *Per-process circuit breakers:* each replica must see its own failures
  before it stops (bounded at `threshold + concurrency` calls each).
- *Polling* (500ms when idle) instead of `LISTEN/NOTIFY` adds up to 0.5s of
  latency on an idle channel.

**Future work**

- Weighted-fair priority (e.g. 70/20/10) to bound starvation.
- Multipart SMS with GSM-7/UCS-2 segment counting.
- Per-tenant API keys, rate limits and idempotency scopes; expiring idempotency keys.
- Bulk dead-letter redrive (`retry` by batch or error type).
- Template versioning (new name or version per change) and per-locale variants.
- Partitioning `notifications` by month and archiving old partitions.
- `LISTEN/NOTIFY` to wake idle workers immediately.

# Notification Service

An event-driven notification service in Go: accepts SMS, Email and Push
requests over HTTP, persists them in PostgreSQL (which doubles as the work
queue), and delivers them asynchronously through rate-limited workers with
retries.

> Work in progress. The full README (architecture diagram, request lifecycle,
> retry policy, curl examples) is written in a later phase. Design decisions
> are recorded as they are made in [docs/DECISIONS.md](docs/DECISIONS.md).

## Quick start

```bash
docker compose up --build
```

| Service       | URL                                      |
|---------------|------------------------------------------|
| API           | http://localhost:8080                    |
| Swagger UI    | http://localhost:8081                    |
| Worker admin  | http://localhost:8082/health/ready       |
| Mock provider | http://localhost:8090/send               |
| Postgres      | `localhost:5432` (notifications/notifications) |
| Redis         | `localhost:6379`                         |

## Development

```bash
make test-unit   # fast unit tests, no Docker
make test        # unit + integration tests (needs Docker)
make lint        # go vet + golangci-lint
```

Configuration is environment-only; see [.env.example](.env.example).

## Retry policy

| Outcome of a provider call | Classified as | What happens |
|---|---|---|
| 2xx | success | `sent`, `provider_message_id` stored |
| 4xx except 408/429 | permanent | `failed` immediately, never retried |
| 5xx, 408, 429, timeout, network error | retryable | `retrying` with backoff, until `MAX_ATTEMPTS` (5) |

- **Backoff:** exponential with full jitter. After the k-th failure the delay is
  uniform in `[0, min(5m, 2s x 2^(k-1)))`, so ceilings run 2s, 4s, 8s, 16s.
  A `Retry-After` header wins when it asks for longer (capped at 1h).
- **Per-attempt timeout:** 5s (`PROVIDER_TIMEOUT`).
- **Exhaustion:** `failed` with `last_error`. Failed notifications are the
  dead-letter set; `POST /api/v1/notifications/{id}/retry` re-queues one with a
  fresh budget.
- **Circuit breaker (per channel):** 5 consecutive retryable failures open it.
  For 30s nothing is claimed on that channel (no attempts are burned), then
  one probe decides whether to close it again.
- **Every attempt** is recorded in `delivery_attempts` (status code, error,
  duration, outcome) and shown by `GET /api/v1/notifications/{id}`.

See [docs/DECISIONS.md](docs/DECISIONS.md) ADR-019, ADR-023 to ADR-025 for the reasoning.

## Assumptions (so far)

Validation limits are configurable through environment variables:

| Channel | Recipient | Content limit |
|---------|-----------|---------------|
| `sms`   | E.164 phone number (`+905551234567`) | 160 characters (one GSM-7 segment; multipart SMS is future work) |
| `email` | Bare address (`user@example.com`, no display name, dotted domain) | 10,000 characters |
| `push`  | Device token: non-empty, no whitespace, at most 512 chars | 256 characters |

- Lengths count Unicode characters, not bytes. Real SMS encoding (GSM-7 160 vs
  UCS-2 70 characters per segment) is future work.
- `priority` defaults to `normal`. `scheduled_at` must be in the future.
- A batch is all-or-nothing: one invalid item rejects the whole batch with a 422
  listing every failing index.
- Idempotency keys are global (no multi-tenancy) and do not expire. Batches
  take a batch-level key; individual items in a batch do not.
- A notification in `processing` cannot be cancelled: it may already be at the
  provider.
- Any 2xx from the provider counts as accepted (the brief says 202;
  webhook.site defaults to 200). `messageId` is stored when present.
- Rate limits are per channel and global across worker replicas (Redis token
  bucket, 100/s by default, burst 10). If Redis is down, sending pauses
  rather than running unthrottled.
- Priority is strict (high, then normal, then low). Sustained high-priority
  saturation can starve low priority; see ADR-022 for the weighted alternative.
- Delivery is **at-least-once**: a worker crash between the provider's answer
  and our database commit can resend a message. Every request carries
  `X-Notification-ID` so a real provider can deduplicate it.

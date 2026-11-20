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

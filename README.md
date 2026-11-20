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

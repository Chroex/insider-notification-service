# Walkthrough

A guided tour for someone who has to explain this system: one notification
followed through the code, the six ideas worth knowing well, the bugs found
along the way, and a five-minute demo. The README is the reference; the
reasoning behind every choice is in [DECISIONS.md](DECISIONS.md).

## The system in one paragraph

The API validates a request, writes the notification to Postgres with status
`pending` and answers immediately. The notifications table *is* the queue:
worker goroutines (per channel) take a rate-limit token from Redis, claim due
rows with `FOR UPDATE SKIP LOCKED`, call the provider over HTTP and record the
result. Failures are classified as retryable or permanent; retryable ones come
back later with exponential backoff. Every step is visible through logs with a
correlation ID, Prometheus metrics, a JSON summary, a WebSocket status stream
and OpenTelemetry traces.

## Follow one notification

`POST /api/v1/notifications {"channel":"sms","recipient":"+905551234567","content":"Hi"}`

1. **Middleware** ([httpapi/middleware.go](../internal/httpapi/middleware.go)):
   reuse or generate the `X-Correlation-ID`, start a trace span, log the
   request, recover panics, record HTTP metrics.
2. **Handler** `createNotification`
   ([httpapi/handlers.go](../internal/httpapi/handlers.go)): decode strict JSON
   (unknown fields are a 400) and call the service. Handlers only translate
   HTTP to and from the service.
3. **Service** `Create` ([notification/service.go](../internal/notification/service.go)):
   if an `Idempotency-Key` is already stored, return the original (200) or a
   409. Otherwise render the template if there is one
   ([templating](../internal/templating/templating.go)), apply the channel
   rules ([validation](../internal/validation/validation.go)) and build the row:
   status `pending` and `next_attempt_at` = now (or `scheduled` and its
   `scheduled_at`).
4. **Store** `InsertNotification` ([store/notifications.go](../internal/store/notifications.go)):
   one `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING`. Once it commits,
   the notification is durable *and* queued; there is no second system to
   keep in sync. The API answers **201**.
5. **Worker loop** `claimLoop` ([worker/worker.go](../internal/worker/worker.go)),
   one per channel: ask the circuit breaker how much it may send, reserve free
   send slots, take that many tokens from Redis
   ([ratelimit/limiter.go](../internal/ratelimit/limiter.go), a Lua script),
   then claim up to that many due rows.
6. **Claim** ([queue/queue.go](../internal/queue/queue.go)): one statement
   selects due rows ordered by priority with `FOR UPDATE SKIP LOCKED` and sets
   `status = 'processing'`, `attempts + 1`, `locked_until = now() + 30s`. It
   commits before any HTTP call.
7. **Send** `process` → [provider/client.go](../internal/provider/client.go):
   `POST {to, channel, content}` with `X-Notification-ID` and
   `X-Correlation-ID`, 5 s timeout, redirects never followed.
8. **Decide** ([worker/classify.go](../internal/worker/classify.go),
   [worker/policy.go](../internal/worker/policy.go)): 2xx → `sent`; 5xx, 429,
   408 or a network error → `retrying` with a jittered delay (or `failed` when
   the 5 attempts are used up); any other 4xx → `failed` at once.
9. **Complete** (`Complete` in queue.go): one transaction writes the
   `delivery_attempts` row and moves the notification on, but only if it is
   still `processing` under *this* attempt number (the fencing token).
10. **Tell the world**: the worker publishes the status change to Redis
    ([events](../internal/events/events.go)); the API's hub
    ([ws/hub.go](../internal/ws/hub.go)) pushes it to WebSocket clients. Metrics
    and the trace span are updated. `GET /api/v1/notifications/{id}` now shows
    `sent` with the attempt history.

## Six ideas to know well

**1. Postgres is the queue** (ADR-001, ADR-017). Enqueueing is the same
`INSERT` that stores the notification, so a request is never stored but
not queued (the dual-write problem). Priority, scheduling and retry delays are
just columns and an `ORDER BY`. `SKIP LOCKED` lets many workers claim
different rows without blocking each other. It is enough for thousands of
messages per second; at Insider scale an outbox would feed Kafka topics per
channel, with Postgres staying the source of truth for status.

**2. Leases make delivery at-least-once** (ADR-018). A claim is a 30 s lease. If
a worker dies mid-send, the reaper re-queues the row when the lease expires
and records the lost call as an `interrupted` attempt. The provider may have
received it, so the message can be sent twice; losing it would be worse.
Duplicates are kept rare by fencing (a stale worker cannot overwrite a newer
claim) and `X-Notification-ID`, which lets a provider drop repeats.

**3. Idempotency is a unique index** (ADR-011). The key is `UNIQUE`; the request
hash tells a retry (200, same id) from a reused key (409). A replay is answered
before validation, so a retried scheduled request still succeeds after its
time has passed. `ON CONFLICT` settles races; 20 parallel requests make one row.

**4. Rate limiting happens before claiming** (ADR-021). Tokens come from one
Redis bucket per channel shared by every worker replica (100/s, burst 10).
The worker takes tokens *first* and claims only as many rows as it got, so a
throttled worker never holds rows it cannot send, and unused tokens are
refunded. If Redis is down the channel pauses (fails closed) rather than risk
the provider banning us.

**5. Retries are deliberate** (ADR-019, ADR-023, ADR-024). Retry only what can
succeed later (timeouts, 5xx, 429, 408). Backoff is exponential with full
jitter (base 2 s, cap 5 min) so a provider outage does not turn into
synchronized retry waves, and a longer `Retry-After` wins. A per-channel
circuit breaker opens after a streak of retryable failures from 5 *different*
notifications (a success or a permanent 4xx resets the streak, and so does a
gap of more than a minute between failures), stops claiming for 30 s, then
lets one probe through. While it is open nothing
is claimed, so no attempts are burned.

**6. You can see everything** (ADR-026, ADR-030). One correlation ID in every
log line, the database row and the provider request. Prometheus metrics per
channel (sent, failed by reason, attempts by outcome, latency histograms, queue
depth, breaker state). `GET /api/v1/metrics/summary` for humans. A Jaeger trace
that crosses the queue: the row stores the creating request's `traceparent`,
so API → DB → worker → provider is one trace.

## Bugs found, and how

These make good interview stories: each was found by a test, a probe or a demo,
and each fix has a test that failed first.

| Bug | How it was found | Fix |
|---|---|---|
| Fresh notifications sometimes not due yet | A priority test failed once in a while | Due times use only the database clock (ADR-002) |
| One poison message could open the circuit breaker | End-to-end demo | Count *distinct* failing notifications in a window (ADR-024) |
| A provider redirect marked a message `sent` that was never delivered | Comparing with another implementation | Never follow redirects (ADR-019) |
| `\u0000` in content caused a 500 | Probing the API | Reject NUL characters with a 422 |
| A retried scheduled request got a 422 after its time passed | Comparing with another implementation | Answer replays before validation (ADR-011) |
| `{{printf "%099999999d" 1}}` in a template could allocate ~100 MB | Probing the API | Templates allow only variables and `if` (ADR-028) |
| A 25-digit `Retry-After` was ignored | Reading the parser | Saturate instead of overflow |
| "Flaky" link errors | Investigating test failures | The disk was full: an environment problem, not code |

## A five-minute demo

```bash
make up              # whole stack in Docker
make smoke           # ~40 end-to-end checks, one line each, ~15 s
make seed            # a batch with every channel, a 400 and a 503
```

1. **Swagger** at http://localhost:8081: create a notification, read it back
   with its attempts.
2. **Retries**: `GET /api/v1/notifications/{id}` for the `[mock:503]` item from
   the seed batch shows five attempts, spaced by a random delay whose upper
   bound doubles each time (full jitter), then `failed`;
   `POST .../retry` re-queues it.
3. **Crash recovery**: slow the mock (`MOCK_LATENCY_MIN=3s MOCK_LATENCY_MAX=4s
   docker compose up -d mockprovider`), send a few notifications,
   `docker compose kill -s SIGKILL worker`, start it again. About 40 s later
   each one shows attempt 1 `interrupted`, then a successful attempt (set
   `MOCK_FAILURE_RATE=0` too if you want exactly two attempts each).
4. **Observability**: `curl -s 'localhost:8080/api/v1/metrics/summary?window=15m' | jq .overall`,
   then one trace in Jaeger at http://localhost:16686 (service `api`).

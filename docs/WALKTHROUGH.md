# Walkthrough

A guided tour for someone who has to explain this system: one notification
followed through the code, the six ideas worth knowing well, the bugs found
along the way, and a five-minute demo. The README is the reference; the
reasoning behind every choice is in [DECISIONS.md](DECISIONS.md).

## The system in one paragraph

The API validates a request, writes the notification to Postgres with status
`pending` and answers immediately. The notifications table *is* the queue:
worker goroutines (per channel) claim due rows with `FOR UPDATE SKIP LOCKED`,
take a rate-limit slot from Redis right before each call, call the provider
over HTTP and record the result. Failures are classified as retryable or permanent; retryable ones come
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
   send slots, then claim up to that many due rows.
6. **Claim** ([queue/queue.go](../internal/queue/queue.go)): one statement
   takes due rows by priority share (70/20/10, so low is never starved) with
   `FOR UPDATE SKIP LOCKED` and sets
   `status = 'processing'`, `attempts + 1`, `locked_until = now() + 30s`. It
   commits before any HTTP call.
7. **Send** `process`: confirm the row is still ours (`Renew`), take a
   rate-limit slot from Redis
   ([ratelimit/limiter.go](../internal/ratelimit/limiter.go), a Lua script),
   then call [provider/client.go](../internal/provider/client.go):
   `POST {to, channel, content}` with `X-Notification-ID` and
   `X-Correlation-ID`, 5 s timeout, redirects never followed.
8. **Decide** ([worker/classify.go](../internal/worker/classify.go),
   [worker/policy.go](../internal/worker/policy.go)): 202 → `sent` (any other 2xx is a permanent failure); 5xx, 429,
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

**4. Rate limiting counts sends, as the provider sees them** (ADR-021).
Redis keeps, per channel, one entry per send that still counts, shared by
every worker replica. A send takes a slot right before its HTTP call and
holds it while the request is in flight and for one second after it
returns. The provider receives each request somewhere in between, so it
never sees more than 100 in any second, however late the network delivers
them. The price: throughput is about `100 / (1 + provider latency)`. If Redis
is down the channel pauses (fails closed) rather than risk the provider
banning us.

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
| A 1000-item template batch allocated ~374 MB before being rejected | Code review | Rendering stops at the channel's limit (ADR-028) |
| A repeat that added `variables` to a plain request replayed it (200, not 409) | Debate with the Codex version | The request hash always includes `variables` (ADR-011) |
| With a short cooldown, an old send's success closed the breaker before the probe answered | Debate with the Codex version | Results belong to the breaker period they started in (ADR-024) |
| A worker stalled past its lease could still send a row another worker had claimed | Debate with the Codex version | Re-check ownership right before the send (ADR-018) |
| End-to-end latency left out the scheduled wait, unlike the brief's definition | Debate with the Codex version | Created → sent, plus a separate due → sent metric (ADR-026) |
| Any 2xx counted as `sent`, so a misconfigured URL answering 200 would lose messages silently | Re-reading the brief: its provider contract is 202 | Only 202 is success; another 2xx fails loudly without a retry (ADR-019) |
| Strict priority let a flood of `high` starve `low` forever; the brief only asks for priority support | Re-reading the brief against our own assumptions | Sends are shared 70/20/10 while several priorities wait (ADR-022) |
| A token bucket with burst 10 let one second carry 110 sends; burst 1 fixed that but dropped throughput to ~77/s | A test that counts the busiest 1-second window | Sliding window in Redis (ADR-021) |
| The sliding window counted grants taken before the claim: a refund could remove another replica's entries, and the provider saw up to 160 sends in a second | The failure-hunter agent, measuring arrivals at a fake provider | Take the slot right before each call and hold it until one second after the call returns (ADR-021) |
| A misconfigured provider answering 200 would drain the whole backlog to `failed`, because permanent errors reset the breaker | The interviewer agent | A 2xx other than 202 counts against the provider in the breaker (ADR-019) |
| Second round against the new limiter: a job waiting for a slot sent after the breaker opened; a wait longer than the lease let two workers send one row (up to 5 times); timed-out requests freed their slots early (a stalled provider saw 228 in a second); a replica with a shorter timeout shortened the key's TTL | The failure-hunter agent, round 2 | Re-check breaker and ownership after the wait, bound the wait by half the lease, keep an unanswered request's slot for the max send time, never shorten a TTL (ADR-017, ADR-021) |
| A half-open probe dropped before its send (a failed ownership check) wedged the channel until restart | The failure-hunter agent | Every early exit hands the probe back, tied to its breaker period (ADR-024) |
| Seven stale lines in README, DECISIONS and code comments (strict priority, old claim SQL, a "future work" item already done) | The pdf-auditor and interviewer agents | Corrected; the 70/20/10 docs now say spare capacity goes by priority (ADR-022) |
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

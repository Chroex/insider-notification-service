#!/usr/bin/env bash
# End-to-end smoke test against a running stack (make up / docker compose up).
#
# Drives the real API, worker, mock provider, Postgres and Redis through the
# main flows and checks the outcome of each. Prints one line per check and
# exits 1 if any check failed. Needs curl and jq.
#
# Runs in about half a minute. Random mock failures (MOCK_FAILURE_RATE) only
# make some sends retry; the waits below allow for that.
set -uo pipefail

API="${API_URL:-http://localhost:8080}"
WORKER="${WORKER_URL:-http://localhost:8082}"
RUN="smoke-$(date +%s)-$$" # unique per run: idempotency keys, template names
failures=0

pass() { printf '  \033[32mok\033[0m   %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; failures=$((failures + 1)); }
section() { printf '\n%s\n' "$1"; }

# expect NAME GOT WANT
expect() {
  if [[ "$2" == "$3" ]]; then pass "$1"; else fail "$1 (got '$2', want '$3')"; fi
}

# request METHOD PATH [JSON] [HEADER...]: sets $status and $body.
request() {
  local method=$1 path=$2 data=${3:-}
  shift $(($# < 3 ? $# : 3))
  local args=(-sS -X "$method" -H 'Content-Type: application/json' -w '\n%{http_code}')
  [[ -n "$data" ]] && args+=(-d "$data")
  for header in "$@"; do args+=(-H "$header"); done
  local out
  out=$(curl "${args[@]}" "$API$path")
  status=${out##*$'\n'}
  body=${out%$'\n'*}
}

# wait_status ID WANT [SECONDS]: polls until the notification reaches WANT,
# then prints the last status seen.
wait_status() {
  local id=$1 want=$2 seconds=${3:-45} current=""
  for ((i = 0; i < seconds * 2; i++)); do
    current=$(curl -sS "$API/api/v1/notifications/$id" | jq -r .status)
    [[ "$current" == "$want" ]] && break
    sleep 0.5
  done
  echo "$current"
}

create() { # create JSON [HEADER...]: POST one notification, sets $id
  request POST /api/v1/notifications "$@"
  id=$(jq -r '.id // empty' <<<"$body")
}

section "Health"
expect "api ready" "$(curl -s -o /dev/null -w '%{http_code}' "$API/health/ready")" 200
expect "worker ready" "$(curl -s -o /dev/null -w '%{http_code}' "$WORKER/health/ready")" 200

section "Delivery on every channel"
for recipient in sms:+905551234567 email:smoke@example.com push:device-token-smoke; do
  channel=${recipient%%:*}
  create "{\"channel\":\"$channel\",\"recipient\":\"${recipient#*:}\",\"content\":\"smoke $channel\"}"
  expect "$channel accepted" "$status" 201
  expect "$channel sent" "$(wait_status "$id" sent)" sent
  message_id=$(curl -sS "$API/api/v1/notifications/$id" | jq -r '.provider_message_id // empty')
  [[ -n "$message_id" ]] && pass "$channel has provider_message_id" || fail "$channel has no provider_message_id"
done

section "Failures and retries"
create '{"channel":"sms","recipient":"+905551234567","content":"smoke [mock:400]"}'
permanent=$id
expect "400 from provider fails permanently" "$(wait_status "$permanent" failed)" failed
expect "permanent failure is not retried" \
  "$(curl -sS "$API/api/v1/notifications/$permanent" | jq -c '[.delivery_attempts[].outcome]')" '["permanent_error"]'

create '{"channel":"email","recipient":"smoke@example.com","content":"smoke [mock:503]"}'
retrying=$id
expect "503 from provider is retried" "$(wait_status "$retrying" retrying 20)" retrying
request POST "/api/v1/notifications/$retrying/cancel"
expect "a retrying notification can be cancelled" "$(jq -r .status <<<"$body")" cancelled

request POST "/api/v1/notifications/$permanent/retry"
expect "manual retry re-queues a failed one" "$status" 200
expect "manual retry makes a new attempt" "$(wait_status "$permanent" failed)" failed
expect "attempt history keeps both attempts" \
  "$(curl -sS "$API/api/v1/notifications/$permanent" | jq '.delivery_attempts | length')" 2

section "Idempotency"
payload='{"channel":"push","recipient":"device-token-smoke","content":"smoke idempotent"}'
create "$payload" "Idempotency-Key: $RUN"
original=$id
expect "first request creates" "$status" 201
create "$payload" "Idempotency-Key: $RUN"
expect "replay returns 200" "$status" 200
expect "replay returns the original id" "$id" "$original"
create '{"channel":"push","recipient":"device-token-smoke","content":"something else"}' "Idempotency-Key: $RUN"
expect "same key, different payload is 409" "$status" 409

section "Scheduling and cancellation"
soon=$(jq -nr 'now + 3 | todate')
create "{\"channel\":\"sms\",\"recipient\":\"+905551234567\",\"content\":\"smoke scheduled\",\"scheduled_at\":\"$soon\"}"
scheduled=$id
expect "scheduled notification waits" "$(jq -r .status <<<"$body")" scheduled
expect "scheduled notification is sent when due" "$(wait_status "$scheduled" sent)" sent

later=$(jq -nr 'now + 3600 | todate')
create "{\"channel\":\"sms\",\"recipient\":\"+905551234567\",\"content\":\"smoke cancel\",\"scheduled_at\":\"$later\"}"
request POST "/api/v1/notifications/$id/cancel"
expect "cancel a scheduled notification" "$(jq -r .status <<<"$body")" cancelled
request POST "/api/v1/notifications/$id/cancel"
expect "cancelling twice is 409" "$status" 409

section "Batch"
request POST /api/v1/notifications/batch '{"notifications":[
  {"channel":"sms","recipient":"+905551234567","content":"smoke batch 1"},
  {"channel":"email","recipient":"smoke@example.com","content":"smoke batch 2"},
  {"channel":"push","recipient":"device-token-smoke","content":"smoke batch 3"}]}'
expect "batch accepted" "$status" 201
batch=$(jq -r .batch_id <<<"$body")
sent=0
for ((i = 0; i < 90 && sent != 3; i++)); do
  sent=$(curl -sS "$API/api/v1/batches/$batch" | jq '.status_counts.sent')
  sleep 0.5
done
expect "all batch items sent" "$sent" 3

section "Templates"
request POST /api/v1/templates "{\"name\":\"$RUN\",\"channel\":\"sms\",\"body\":\"Hi {{.name}}{{if .vip}}, VIP{{end}}\"}"
expect "template created" "$status" 201
expect "required variables discovered" "$(jq -c .variables <<<"$body")" '["name","vip"]'
template=$(jq -r .id <<<"$body")
create "{\"channel\":\"sms\",\"recipient\":\"+905551234567\",\"template_id\":\"$template\",\"variables\":{\"name\":\"Ayşe\",\"vip\":true}}"
expect "template rendered into content" "$(jq -r .content <<<"$body")" "Hi Ayşe, VIP"
request POST /api/v1/templates "{\"name\":\"$RUN-loop\",\"channel\":\"sms\",\"body\":\"{{range .items}}x{{end}}\"}"
expect "loops in templates are rejected" "$status" 422

section "Request validation"
create '{"channel":"sms","recipient":"12345","content":""}'
expect "invalid fields are a 422" "$status" 422
expect "every invalid field is listed" "$(jq '.error.details | length' <<<"$body")" 2
request PUT /api/v1/notifications
expect "wrong method is a 405" "$status" 405

section "Observability"
request GET "/api/v1/metrics/summary?window=15m"
expect "summary endpoint" "$status" 200
[[ $(jq '.overall.sent' <<<"$body") -ge 1 ]] && pass "summary counts sent notifications" || fail "summary shows no sent notifications"
curl -sS "$WORKER/metrics" | grep -q '^notifications_sent_total' &&
  pass "worker exports Prometheus metrics" || fail "worker metrics missing notifications_sent_total"

echo
if ((failures > 0)); then
  printf '\033[31m%d check(s) failed\033[0m\n' "$failures"
  exit 1
fi
printf '\033[32mall checks passed\033[0m\n'

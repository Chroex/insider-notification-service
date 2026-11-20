#!/usr/bin/env bash
# Sends a demo batch covering every channel and priority, plus two items that
# make the mock provider fail ([mock:400] permanent, [mock:503] retryable),
# so retries and failures are visible in the demo.
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"

curl -sS -X POST "$API_URL/api/v1/notifications/batch" \
  -H 'Content-Type: application/json' \
  -H "X-Correlation-ID: seed-$(date +%s)" \
  --data-binary @- <<'JSON'
{
  "notifications": [
    {"channel": "sms",   "recipient": "+905551234567",     "content": "Flash sale starts now!",           "priority": "high"},
    {"channel": "sms",   "recipient": "+905559876543",     "content": "Your order has shipped.",          "priority": "normal"},
    {"channel": "email", "recipient": "ayse@example.com",  "content": "Weekly digest: 5 new products.",   "priority": "low"},
    {"channel": "email", "recipient": "mehmet@example.com","content": "Password changed successfully.",   "priority": "high"},
    {"channel": "push",  "recipient": "device-token-abc",  "content": "Breaking: big news just in.",      "priority": "high"},
    {"channel": "push",  "recipient": "device-token-def",  "content": "You left items in your cart.",     "priority": "low"},
    {"channel": "sms",   "recipient": "+905550000001",     "content": "Demo permanent failure [mock:400]"},
    {"channel": "email", "recipient": "retry@example.com", "content": "Demo retryable failure [mock:503]"}
  ]
}
JSON
echo

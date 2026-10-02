#!/usr/bin/env bash
# End-to-end check against a running `docker compose up`.
set -euo pipefail
BASE="${BASE:-http://localhost:8080}"
KEY="${API_KEY:-dev-key-change-me}"
fail() { echo "FAIL: $*" >&2; exit 1; }
ok()   { echo "ok:   $*"; }

curl -fsS "$BASE/readyz" >/dev/null || fail "readyz"
ok "ready"

ALIAS="smoke-$RANDOM$RANDOM"
resp=$(curl -fsS -X POST "$BASE/v1/links" -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/landing"}')
slug=$(echo "$resp" | sed -n 's/.*"slug":"\([^"]*\)".*/\1/p')
[ -n "$slug" ] || fail "no slug in $resp"
ok "created generated slug $slug"

code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/v1/links" -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' -d "{\"url\":\"https://example.org\",\"alias\":\"$ALIAS\"}")
[ "$code" = 201 ] || fail "custom alias create = $code"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/v1/links" -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' -d "{\"url\":\"https://example.org\",\"alias\":\"$ALIAS\"}")
[ "$code" = 409 ] || fail "duplicate alias = $code (want 409)"
ok "custom alias + conflict"

code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/v1/links" -d '{"url":"https://a.io"}')
[ "$code" = 401 ] || fail "unauthenticated create = $code"
ok "auth enforced"

for i in 1 2 3 4 5; do
  out=$(curl -s -o /dev/null -w '%{http_code} %{redirect_url}' -H 'Referer: https://news.example' "$BASE/$slug")
  [ "$out" = "302 https://example.com/landing" ] || fail "redirect = $out"
done
ok "5 redirects return 302 to the destination"

[ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/does-not-exist")" = 404 ] || fail "404"
ok "unknown slug 404"

echo "waiting for clicks to flow Kafka -> consumer -> Postgres ..."
for i in $(seq 1 30); do
  total=$(curl -fsS "$BASE/v1/links/$slug/stats" -H "X-API-Key: $KEY" | sed -n 's/.*"total_clicks":\([0-9]*\).*/\1/p')
  [ "${total:-0}" -ge 5 ] && break
  sleep 1
done
[ "${total:-0}" = 5 ] || fail "stats total_clicks = ${total:-0}, want 5"
ok "stats show 5 clicks"
curl -fsS "$BASE/v1/links/$slug/stats" -H "X-API-Key: $KEY"; echo

# Rate limit: burst 10 / 60 per minute per key.
limited=0
for i in $(seq 1 25); do
  c=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/v1/links" -H "X-API-Key: $KEY" \
    -H 'Content-Type: application/json' -d '{"url":"https://example.com/rl"}')
  [ "$c" = 429 ] && limited=1
done
[ "$limited" = 1 ] || fail "rate limit never triggered"
ok "rate limiting returns 429"
echo "ALL SMOKE CHECKS PASSED"

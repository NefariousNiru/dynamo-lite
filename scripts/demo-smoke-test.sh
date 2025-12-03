#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

tests=0
passed=0

KEY="demo-key"
VALUE="hello-dynalite"
VAL_B64="$(printf "%s" "$VALUE" | base64)"

echo "[smoke] Using key='$KEY', value='$VALUE', base64='$VAL_B64'"

run_test() {
  local name="$1"
  local cmd="$2"

  tests=$((tests + 1))
  echo
  echo "== Test $tests: $name"

  if eval "$cmd"; then
    echo "✅ PASS: $name"
    passed=$((passed + 1))
  else
    echo "❌ FAIL: $name"
  fi
}

# --- Test 1: PUT via node-a (HTTP :8080) ---

run_test "PUT via node-a" '
  payload=$(printf "{\"valueBase64\":\"%s\",\"nodeId\":\"node-a\",\"opId\":\"test-put-1\"}" "'"$VAL_B64"'")
  status=$(curl -s -o /tmp/demo_put_body.json -w "%{http_code}" \
    -X PUT \
    -H "Content-Type: application/json" \
    --data "$payload" \
    http://localhost:8080/kv/'"$KEY"')

  echo "HTTP $status"
  [ "$status" = "200" ]
'

# --- Test 2: GET via node-a (should find value) ---

run_test "GET via node-a returns value" '
  status=$(curl -s -o /tmp/demo_get_a_body.json -w "%{http_code}" \
    http://localhost:8080/kv/'"$KEY"')

  body=$(cat /tmp/demo_get_a_body.json)
  echo "HTTP $status"
  echo "Body: $body"

  [ "$status" = "200" ] && \
    echo "$body" | grep -q "\"found\":true" && \
    echo "$body" | grep -q "\"$VAL_B64\""
'

# --- Test 3: GET via node-b (replication / coordinator fanout) ---

run_test "GET via node-b sees same value" '
  status=$(curl -s -o /tmp/demo_get_b_body.json -w "%{http_code}" \
    http://localhost:8081/kv/'"$KEY"')

  body=$(cat /tmp/demo_get_b_body.json)
  echo "HTTP $status"
  echo "Body: $body"

  [ "$status" = "200" ] && \
    echo "$body" | grep -q "\"found\":true" && \
    echo "$body" | grep -q "\"$VAL_B64\""
'

# --- Test 4: DELETE via node-c ---

run_test "DELETE via node-c" '
  payload="{\"nodeId\":\"node-c\",\"opId\":\"test-del-1\"}"
  status=$(curl -s -o /tmp/demo_del_body.json -w "%{http_code}" \
    -X DELETE \
    -H "Content-Type: application/json" \
    --data "$payload" \
    http://localhost:8082/kv/'"$KEY"')

  body=$(cat /tmp/demo_del_body.json)
  echo "HTTP $status"
  echo "Body: $body"

  [ "$status" = "200" ]
'

# --- Test 5: GET via node-a after delete (should be 404) ---

echo
echo "[smoke] $passed/$tests tests passed"
exit $([ "$passed" -eq "$tests" ] && echo 0 || echo 1)

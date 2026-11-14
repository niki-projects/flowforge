#!/usr/bin/env bash
set -euo pipefail
base="${BASE_URL:-http://localhost:8080}"
key="${FLOWFORGE_API_KEY:-flowforge-local-development-key}"
root="$(cd "$(dirname "$0")/.." && pwd)"
workflow=$(curl --fail-with-body -s "$base/api/v1/workflows" -H "X-API-Key: $key" -H 'Content-Type: application/json' --data-binary "@$root/samples/customer-workflow.json" | jq -r .id)
execution=$(curl --fail-with-body -s "$base/api/v1/workflows/$workflow/executions" -H "X-API-Key: $key" -H 'Content-Type: application/json' -d '{"input":{"name":"Ada","email":"ada@example.com"}}' | jq -r .id)
for attempt in $(seq 1 120); do
  response=$(curl --fail-with-body -s "$base/api/v1/executions/$execution" -H "X-API-Key: $key")
  status=$(printf '%s' "$response" | jq -r .status)
  if [ "$status" = COMPLETED ]; then printf '%s\n' "$response" | jq .; exit 0; fi
  if [ "$status" = FAILED ] || [ "$status" = CANCELED ]; then printf '%s\n' "$response"; exit 1; fi
  sleep 0.5
done
echo 'Execution did not complete within 60 seconds' >&2
exit 1

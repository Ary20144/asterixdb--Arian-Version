#!/usr/bin/env bash
# Post a .sqlpp file (arg 1) to the query API and print status + any errors.
set -euo pipefail
source "$(dirname "$0")/../config.sh"

STMT="$(cat "$1")"
jq -n --arg stmt "$STMT" '{statement: $stmt}' \
  | curl -s -X POST "$API" -H 'Content-Type: application/json' -d @- \
  | jq '{status: .status, errors: (.errors // "none"), metrics: .metrics}'

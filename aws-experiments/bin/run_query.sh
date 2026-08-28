#!/usr/bin/env bash
# One timed run of the experiment query at a given memory budget.
#   Usage: run_query.sh <joinmemory> <tag>      e.g.  run_query.sh 6496KB p10-run1
# Saves the full API response (profile timings + annotated plan) to
# results/<tag>_<mem>.json and prints the one-line outcome.
set -euo pipefail
source "$(dirname "$0")/../config.sh"

MEM="$1"
TAG="$2"
mkdir -p "$RESULTS_DIR"
OUT="$RESULTS_DIR/${TAG}_${MEM}.json"

STMT="USE tpcds; SET \`compiler.joinmemory\` \"$MEM\"; SELECT * FROM customer c, store_returns sr WHERE sr.sr_customer_sk = c.c_customer_sk LIMIT 1;"

jq -n --arg stmt "$STMT" \
  '{statement: $stmt, profile: "timings", "optimized-logical-plan": true, "plan-format": "json"}' \
  | curl -s -X POST "$API" -H 'Content-Type: application/json' -d @- > "$OUT"

STATUS=$(jq -r '.status // "no-status"' "$OUT")
EXEC=$(jq -r '.metrics.executionTime // "n/a"' "$OUT")
ELAP=$(jq -r '.metrics.elapsedTime // "n/a"' "$OUT")
echo "$TAG mem=$MEM status=$STATUS executionTime=$EXEC elapsedTime=$ELAP -> $OUT"

#!/usr/bin/env bash
# Post-load verification. Expected values (TPC-DS SF1, this experiment's data):
#   customer count      = 100000
#   join result (oracle) = 277498   <-- every timed run's SUMMARY matches= must equal this
set -euo pipefail
source "$(dirname "$0")/../config.sh"

run() {
  jq -n --arg stmt "$1" '{statement: $stmt}' \
    | curl -s -X POST "$API" -H 'Content-Type: application/json' -d @- \
    | jq -c '{status: .status, results: .results, executionTime: .metrics.executionTime}'
}

echo "== customer count (expect 100000) =="
run "USE tpcds; SELECT VALUE COUNT(*) FROM customer;"

echo "== store_returns count (record it) =="
run "USE tpcds; SELECT VALUE COUNT(*) FROM store_returns;"

echo "== join oracle: result cardinality (expect 277498; takes a full join, be patient) =="
run "USE tpcds; SET \`compiler.joinmemory\` \"64224KB\"; SELECT VALUE COUNT(*) FROM customer c, store_returns sr WHERE sr.sr_customer_sk = c.c_customer_sk;"

#!/usr/bin/env bash
# Append the newest NLJ-STATIC lines from the NC log to results/summaries.log,
# tagged with arg 1. Call right after each run_query.sh.
set -euo pipefail
source "$(dirname "$0")/../config.sh"

TAG="${1:-untagged}"
mkdir -p "$RESULTS_DIR"
LEDGER="$RESULTS_DIR/summaries.log"

if [[ ! -f "$NC_LOG" ]]; then
  echo "WARN: NC log not found at $NC_LOG (fix NC_LOG in config.sh)" | tee -a "$LEDGER"
  exit 0
fi

{
  echo "---- $TAG $(date '+%Y-%m-%dT%H:%M:%S') ----"
  grep "NLJ-STATIC" "$NC_LOG" | tail -20
} >> "$LEDGER"
echo "SUMMARY captured for $TAG:"
grep "NLJ-STATIC SUMMARY" "$NC_LOG" | tail -1

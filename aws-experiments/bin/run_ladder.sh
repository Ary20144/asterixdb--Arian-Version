#!/usr/bin/env bash
# The full static-memory ladder: 5 rungs x 4 runs each.
# Budgets = (ceil(p% x 2005 build frames) + 2 reserved) x 32KB.
# Run 1 of each rung is the warm-up: DISCARD it in analysis, average runs 2-4.
set -euo pipefail
cd "$(dirname "$0")"

RUNGS=(6496KB 12896KB 25728KB 51392KB 64224KB)
NAMES=(p10   p20     p40     p80     p100)
# Expected per rung, for on-the-spot sanity checks:
#   p10 : spills=9 passesOverInner=10 sFrameReads=59250
#   p20 : spills=4 passesOverInner=5  sFrameReads=29625
#   p40 : spills=2 passesOverInner=3  sFrameReads=17775
#   p80 : spills=1 passesOverInner=2  sFrameReads=11850
#   p100: spills=0 passesOverInner=1  sFrameReads=5925
# Every run: matches=277498, outerFramesIn=2005, innerRunFileBytes=194150400.

for i in "${!RUNGS[@]}"; do
  for r in 1 2 3 4; do
    TAG="${NAMES[$i]}-run$r"
    echo "===== $TAG (${RUNGS[$i]}) $(date '+%H:%M:%S') ====="
    ./run_query.sh "${RUNGS[$i]}" "$TAG"
    ./grab_summary.sh "$TAG" || true
  done
done

echo "Ladder complete. Raw JSONs + summaries.log are in ../results/"

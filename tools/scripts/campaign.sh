#!/usr/bin/env bash
# Phase 3 campaign: runs the chaos signature scenario over many seeds in parallel processes.
# Usage: tools/scripts/campaign.sh <total_seeds> <processes> <out.md>
set -euo pipefail
TOTAL="${1:-20000}"; PROCS="${2:-8}"; OUT="${3:-bench-results/phase3-campaign.md}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; cd "$ROOT"
cargo test --release -p cairn-query --test chaos campaign --no-run 2>/dev/null
BIN="$(cargo test --release -p cairn-query --test chaos campaign --no-run 2>&1 | grep -o 'target/release/deps/chaos-[0-9a-f]*' | head -1)"
PER=$(( TOTAL / PROCS ))
START="$(date +%s)"
mkdir -p data/campaign
pids=()
for i in $(seq 0 $((PROCS-1))); do
  a=$(( i * PER )); b=$(( (i+1) * PER ))
  CAIRN_SEEDS="$a..$b" "$BIN" campaign --ignored --nocapture > "data/campaign/part$i.log" 2>&1 &
  pids+=($!)
done
fail=0
for p in "${pids[@]}"; do wait "$p" || fail=1; done
END="$(date +%s)"
{
  echo "# Phase 3 simulation campaign"
  echo
  echo "- Date: $(date +%F). Commit: $(git rev-parse --short HEAD)."
  echo "- Scenario: crates/cairn-query/tests/chaos.rs (3 nodes, 3 clients, 12 keys, 40 fault rounds: partitions, 3% drops, crashes and restarts, flushes every ~3 KB, snapshots)."
  echo "- Seeds: 0..$TOTAL in $PROCS processes; wall time $((END-START)) s."
  echo "- Checks per run: every read against the per-key model with real-time bounds, read-your-takedown on Linearizable and ReadYourWrites reads, replica convergence (applied index and documents), determinism digest."
  echo
  if [ "$fail" = 0 ]; then
    echo "**Result: zero violations.**"
  else
    echo "**Result: FAILURES.** See data/campaign/part*.log for the failing seeds."
  fi
  echo
  echo '```'
  grep -h "CAMPAIGN" data/campaign/part*.log
  echo '```'
} > "$OUT"
cat "$OUT"
exit $fail

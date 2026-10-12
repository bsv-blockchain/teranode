#!/bin/bash
# Verifies --shard/--total produce an exhaustive, disjoint partition of the full test list.
set -euo pipefail
cd "$(dirname "$0")/../.."
TOTAL=4  # arbitrary partition count for the self-test; need not match the CI matrix
SCRIPT=test/scripts/run_tests_sequentially.sh

full=$("$SCRIPT" --list-only | sort)
acc=$(for i in $(seq 0 $((TOTAL-1))); do "$SCRIPT" --shard "$i" --total "$TOTAL" --list-only; done | sort)

if [ "$full" != "$acc" ]; then
  echo "FAIL: union of shards != full list (or a test appears in >1 shard)"
  echo "--- only in full ---"; comm -23 <(echo "$full") <(echo "$acc")
  echo "--- duplicated/extra in shards ---"; echo "$acc" | uniq -d
  exit 1
fi
echo "PASS: $(echo "$full" | grep -c . ) tests, $TOTAL shards, exhaustive + disjoint"

#!/usr/bin/env bash
# sweep.sh — run the validator-loadtest across a config matrix.
# Appends one row per case to a TSV for analysis.
#
# Usage: ./sweep.sh [output.tsv]
# Override the binary path via LOADTEST_BIN env var.

set -euo pipefail

OUT="${1:-/tmp/loadtest-sweep-$(date +%Y%m%d-%H%M%S).tsv}"
BIN="${LOADTEST_BIN:-/tmp/validator-loadtest}"

if [[ ! -x "$BIN" ]]; then
  echo "loadtest binary not found at $BIN; run: go build -o $BIN ./cmd/validator-loadtest" >&2
  exit 1
fi

echo -e "case\tsubmitters\tconn_queue\tbatch_max_concurrent\tvalidate_batch\tduration\tsustained_tps\tp50_ms\tp95_ms\tp99_ms\terrors\tpeak_goroutines" > "$OUT"

run_case() {
  local case_name="$1" submitters="$2" conn_queue="$3" bmc="$4" vb="$5" dur="$6" pool="$7"
  echo "==> $case_name"
  local logfile="/tmp/loadtest-$case_name.log"
  "$BIN" \
    --submitters "$submitters" \
    --duration "$dur" \
    --warm-up 5s \
    --parent-pool-size "$pool" \
    --conn-queue-size "$conn_queue" \
    --batch-max-concurrent "$bmc" \
    --validate-batch="$vb" 2>&1 | tee "$logfile"

  local tps p50 p95 p99 errs peak
  tps=$(grep -oE 'Sustained TPS:\s+[0-9.]+' "$logfile" | head -1 | awk '{print $3}')
  # Latency line: "Latency p50/p95/p99: 8.2ms / 9.4ms / 11ms"
  p50=$(grep -oE 'Latency p50/p95/p99:\s+[0-9.]+m?s' "$logfile" | head -1 | awk '{print $2}' | sed -e 's/ms$//' -e 's/s$/000/')
  p95=$(grep 'Latency p50/p95/p99' "$logfile" | head -1 | awk -F'/' '{print $4}' | tr -d ' ms' )
  p99=$(grep 'Latency p50/p95/p99' "$logfile" | head -1 | awk -F'/' '{print $5}' | tr -d ' ms' )
  errs=$(grep -oE 'errors: [0-9]+' "$logfile" | head -1 | awk '{print $2}')
  peak=$(grep -oE 'Peak goroutines: [0-9]+' "$logfile" | head -1 | awk '{print $3}')

  echo -e "$case_name\t$submitters\t$conn_queue\t$bmc\t$vb\t$dur\t${tps:-NA}\t${p50:-NA}\t${p95:-NA}\t${p99:-NA}\t${errs:-NA}\t${peak:-NA}" >> "$OUT"
}

# At 7.7k TPS observed in smoke, 30s × 7700 = 231k. Use 250k pool for safety.
POOL_30S=250000

# Baseline at default knobs
run_case baseline-on 1024 128 64 true 30s "$POOL_30S"
run_case baseline-off 1024 128 64 false 30s 50000

# Connection-pool sweep (flag-on)
run_case conn-256 1024 256 64 true 30s "$POOL_30S"
run_case conn-512 1024 512 64 true 30s "$POOL_30S"
run_case conn-1024 1024 1024 64 true 30s "$POOL_30S"

# batch-max-concurrent sweep (flag-on, conn=512)
run_case bmc-128 1024 512 128 true 30s "$POOL_30S"
run_case bmc-256 1024 512 256 true 30s "$POOL_30S"
run_case bmc-512 1024 512 512 true 30s "$POOL_30S"

# Submitter scale (flag-on, conn=512, bmc=128)
run_case sub-128 128 512 128 true 30s 50000
run_case sub-512 512 512 128 true 30s 200000
run_case sub-2048 2048 512 128 true 30s "$POOL_30S"

echo "Sweep complete. Results: $OUT"

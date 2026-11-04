#!/usr/bin/env bash
# Nightly chain-integrity run on Stack A (multinode stack in chaos-test's layout: 3 teranodes,
# 2 SV nodes, BSV Broadcaster, merkle-service, wallet, chaos-test orchestrator).
#
#   phase 1  chain integrity: steady load, then the same load through faults; each ends with
#            chain_audit (every height on every node) and proof_audit (every wallet tx's merkle
#            path checked on every node). Runs under blaster load when a blaster is available.
#   phase 2  broadcaster end-to-end suite
#   phase 3  chaos-test's scenario library
#
# Writes everything to data/nightly/<run>/ (junit.xml, summary.md, run records, container
# logs, diagnostics) and exits non-zero only for failures not listed in
# suites/known-failures.txt. Runs the same locally and in .github/workflows.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CTL="$SCRIPT_DIR/chaosctl.sh"
MULTINODE="$REPO_ROOT/compose/multinode.sh"
SUITES="$SCRIPT_DIR/suites"

usage() {
  cat <<'EOF'
Usage: compose/chaostest/nightly.sh [options]
  --phases 1,2,3    phases to run (default 1,2,3)
  --quick           short phase 1 only (about 15 minutes): a smoke run of the whole pipeline
  --no-build        use the existing teranode:latest and orchestrator images
  --keep            leave the stack up at the end
  --previous DIR    an earlier run's output directory, to report what changed
  --out DIR         output root (default data/nightly)
  --dry-run         print what would run and exit
EOF
}

phases="1,2,3" quick=false build=true keep=false dry=false previous="" out_root="$REPO_ROOT/data/nightly"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --phases) phases="${2:?}"; shift ;;
    --quick) quick=true; phases=1 ;;
    --no-build) build=false ;;
    --keep) keep=true ;;
    --previous) previous="${2:?}"; shift ;;
    --out) out_root="${2:?}"; shift ;;
    --dry-run) dry=true ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

# Phase 1 lengths: full nightly vs --quick.
if $quick; then
  integrity_params=(--param phaseSeconds=60 --param calmSeconds=60 --param faultSeconds=45)
else
  integrity_params=(--param phaseSeconds=480 --param calmSeconds=240 --param faultSeconds=120)
fi
# Time boxes (seconds) per phase; a phase over its box is stopped and reported.
box_1=4200 box_2=2100 box_3=2700
$quick && box_1=1800
blaster=false
if [[ -n "${BLASTER_BIN:-}" || -d "$REPO_ROOT/../teranode-coinbase" ]]; then blaster=true; fi

note() { echo "-- $*"; }
has_phase() { [[ ",$phases," == *",$1,"* ]]; }

if $dry; then
  note "phases: $phases (quick=$quick, build=$build, keep=$keep, blaster=$blaster)"
  has_phase 1 && note "phase 1: scenario run --suite chain-integrity-nightly --reset ${integrity_params[*]}$($blaster && echo ' --under-load default')  (box ${box_1}s)"
  has_phase 2 && note "phase 2: scenario run --suite broadcaster-e2e --reset  (box ${box_2}s)"
  has_phase 3 && note "phase 3: scenario run --suite chaos-library --reset; svnode-dust-policy after a 110-block pre-mine  (box ${box_3}s)"
  exit 0
fi

mkdir -p "$out_root"
lock="$out_root/.lock"
if ! mkdir "$lock" 2>/dev/null; then
  echo "error: another nightly run holds $lock (remove it if that run is gone)" >&2
  exit 1
fi
run_id="$(date -u +%Y%m%d-%H%M%S)-$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo nogit)"
OUT="$out_root/$run_id"
mkdir -p "$OUT/runs" "$OUT/orchestrator-runs" "$OUT/logs"
start_marker="$OUT/.start"
touch "$start_marker"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

cleanup() {
  $CTL blast stop >/dev/null 2>&1 || true
  if ! $keep; then $CTL stack down a >"$OUT/teardown.log" 2>&1 || true; fi
  rm -rf "$lock"
}
trap cleanup EXIT

# run_boxed <seconds> <log> <cmd...> runs a command with a time limit (no coreutils timeout on
# macOS). Returns the command's status, or 124 when the box ran out.
run_boxed() {
  local secs="$1" log="$2"
  shift 2
  "$@" >>"$log" 2>&1 &
  local pid=$!
  ( sleep "$secs"; echo "-- time box of ${secs}s ran out" >>"$log"; pkill -TERM -P "$pid"; kill -TERM "$pid" ) 2>/dev/null &
  local watcher=$!
  wait "$pid"
  local st=$?
  if kill -0 "$watcher" 2>/dev/null; then
    pkill -P "$watcher" 2>/dev/null
    kill "$watcher" 2>/dev/null
    wait "$watcher" 2>/dev/null
  else
    st=124
  fi
  return "$st"
}

# collect <phase> copies the run records written since the phase began.
collect() {
  local phase="$1" marker="$2" f
  mkdir -p "$OUT/runs/phase$phase"
  for f in "$REPO_ROOT"/data/chaosctl/runs/*.json; do
    [[ -f "$f" && "$f" -nt "$marker" ]] && cp "$f" "$OUT/runs/phase$phase/"
  done
  for f in "$REPO_ROOT"/data/multinode-chaostest/data/runs/*.json; do
    [[ -f "$f" && "$f" -nt "$marker" ]] && cp "$f" "$OUT/orchestrator-runs/"
  done
}

declare -a phase_status=()
phase() {
  local n="$1" box="$2"
  shift 2
  local marker="$OUT/.phase$n" log="$OUT/phase$n.log"
  touch "$marker"
  note "phase $n: $*"
  run_boxed "$box" "$log" "$@"
  local st=$?
  $CTL blast stop >>"$log" 2>&1 || true
  collect "$n" "$marker"
  phase_status+=("$n=$st")
  note "phase $n exited $st (log: $log)"
}

# --- metadata and build -------------------------------------------------------------------------
{
  echo "{"
  echo "  \"run\": \"$run_id\", \"startedAt\": \"$started_at\", \"quick\": $quick, \"phases\": \"$phases\","
  echo "  \"teranode\": \"$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null)\","
  echo "  \"chaosTest\": \"$(git -C "${CHAOS_TEST_DIR:-$REPO_ROOT/../chaos-test}" rev-parse HEAD 2>/dev/null)\","
  echo "  \"blaster\": $blaster"
  echo "}"
} >"$OUT/meta.json"

if $build; then
  note "building teranode:latest and the orchestrator image"
  if ! { "$MULTINODE" build && "$MULTINODE" sim build; } >"$OUT/build.log" 2>&1; then
    echo "error: build failed; see $OUT/build.log" >&2
    python3 -I "$SCRIPT_DIR/nightly_report.py" "$OUT" "$SUITES/known-failures.txt" "$previous" --error "build failed" || true
    exit 1
  fi
fi

note "bringing up Stack A on a new chain"
$CTL stack down a >"$OUT/stack-up.log" 2>&1 || true
if ! $CTL stack up a >>"$OUT/stack-up.log" 2>&1; then
  echo "error: stack up failed; see $OUT/stack-up.log" >&2
  python3 -I "$SCRIPT_DIR/nightly_report.py" "$OUT" "$SUITES/known-failures.txt" "$previous" --error "stack up failed" || true
  exit 1
fi
for img in ghcr.io/bsv-blockchain/broadcaster:latest teranode:latest chaos-orchestrator:multinode; do
  echo "$img $(docker image inspect -f '{{.Id}}' "$img" 2>/dev/null)"
done >"$OUT/images.txt"
docker inspect -f '{{.Config.Image}} {{.Image}}' merkle-service-multinode >>"$OUT/images.txt" 2>/dev/null || true

# --- phases ---------------------------------------------------------------------------------------
if has_phase 1; then
  load=()
  $blaster && load=(--under-load default)
  phase 1 "$box_1" "$CTL" scenario run --suite chain-integrity-nightly --reset "${integrity_params[@]}" ${load[@]+"${load[@]}"}
fi
if has_phase 2; then
  phase 2 "$box_2" "$CTL" scenario run --suite broadcaster-e2e --reset
fi
if has_phase 3; then
  phase 3 "$box_3" bash -c '"$1" scenario run --suite chaos-library --reset; a=$?
    "$1" stack reset && "$2" generate 1,110 && "$1" scenario run svnode-dust-policy; b=$?
    exit $((a || b))' _ "$CTL" "$MULTINODE"
fi

# --- evidence ---------------------------------------------------------------------------------------
note "collecting logs and diagnostics"
since="$(date -u -r "$start_marker" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d "@$(stat -c %Y "$start_marker")" +%Y-%m-%dT%H:%M:%SZ)"
for c in $(docker ps -a --filter name=-multinode --format '{{.Names}}'); do
  docker logs --since "$since" "$c" 2>&1 | gzip >"$OUT/logs/$c.log.gz"
done
cp "$REPO_ROOT"/data/chaosctl/blast.log "$OUT/" 2>/dev/null || true
diag="$REPO_ROOT/.claude/skills/debug-teranode-docker/scripts/teranode-diag-docker.sh"
[[ -x "$diag" ]] && "$diag" --quick >"$OUT/diagnostics.txt" 2>&1
for g in block subtree subtree-worker callback; do
  docker exec kafka-shared-multinode rpk group describe "merkle-service-$g" 2>&1
done >"$OUT/merkle-groups.txt" 2>&1 || true
printf '%s\n' "${phase_status[@]}" >"$OUT/phases.txt"

python3 -I "$SCRIPT_DIR/nightly_report.py" "$OUT" "$SUITES/known-failures.txt" "$previous"
st=$?
note "results: $OUT (summary.md, junit.xml)"
exit "$st"

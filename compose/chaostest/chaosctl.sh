#!/usr/bin/env bash
# chaosctl: one entry point for chaos testing on the two local stacks.
#
#   Stack A  our multinode stack (compose/multinode.sh, Aerospike) + chaos-test's
#            orchestrator via 'multinode.sh sim'.
#   Stack B  chaos-test's own bsv-regtest fleet (SQLite, arcade, alert hub, SV nodes,
#            wallet) running our teranode image, + its orchestrator.
#
# Both publish the same host ports, so only one runs at a time. The blaster and
# scenarios work on whichever stack is up. See 'chaosctl.sh help'.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MULTINODE="$REPO_ROOT/compose/multinode.sh"
CT="${CHAOS_TEST_DIR:-$REPO_ROOT/../chaos-test}"
PRESETS="$SCRIPT_DIR/blast-presets"
STATE="$REPO_ROOT/data/chaosctl"
API="http://localhost:${SIM_HOST_PORT:-8600}/api"
TERANODE_IMAGE="${TERANODE_IMAGE:-teranode:latest}"

mkdir -p "$STATE/runs"

usage() {
  cat <<'EOF'
Usage: compose/chaostest/chaosctl.sh <command> [args]

Stacks (only one runs at a time):
  stack status                      Which stack is up, node tips, orchestrator, blaster
  stack up a [multinode up args]    Stack A: multinode stack + orchestrator (default 3 nodes)
  stack up b                        Stack B: chaos-test's fleet with our image + its orchestrator
  stack down a|b                    Stop a stack (and the blaster)
  stack reset                       Wipe the active stack's chain and bring it back
  --switch on 'stack up' brings the other stack down first.

Blaster (on the active stack):
  blast presets                     List mix presets (compose/chaostest/blast-presets)
  blast start [preset] [--workers N] [--fresh] [--no-fund] [--no-mine]
                                    Start in the background, fund it (mine 110 blocks) and
                                    wait until it sends. Default preset: default. It
                                    auto-mines on teranode1 unless --no-mine.
  blast status                      Running?, preset, latest stats line
  blast stop                        Stop it cleanly

Scenarios (chaos-test, on the active stack):
  scenario list                     Scenario ids the orchestrator has loaded
  scenario run <id>... [--under-load [preset]] [--reset] [--step] [--workers N]
                                    Run one or more scenarios in order and follow each.
                                    --under-load starts (or keeps) the blaster first,
                                    without auto-mine (scenarios check exact heights).
                                    --reset wipes the chain before each scenario (stopping
                                    and refunding the blaster around it).
  scenario last <id>                The last recorded result for a scenario on this stack

Environment: CHAOS_TEST_DIR (default ../chaos-test), TERANODE_IMAGE (default
teranode:latest, used for Stack B), SIM_HOST_PORT (default 8600).
EOF
  exit 2
}

die() { echo "error: $*" >&2; exit 1; }
note() { echo "-- $*"; }

# ---- stack detection --------------------------------------------------------

running() { docker ps --format '{{.Names}}' 2>/dev/null; }

active_stack() {
  local names a=0 b=0
  names=$(running)
  grep -qE '^teranode[0-9]+(-[a-z0-9]+)?-multinode$' <<<"$names" && a=1
  grep -qE '^bsv-regtest-teranode[0-9]+$' <<<"$names" && b=1
  if [[ $a -eq 1 && $b -eq 1 ]]; then echo both
  elif [[ $a -eq 1 ]]; then echo a
  elif [[ $b -eq 1 ]]; then echo b
  else echo none
  fi
}

require_stack() {
  local s
  s=$(active_stack)
  case "$s" in
    a|b) echo "$s" ;;
    both) die "both stacks are running and share host ports; bring one down ('stack down a|b')" ;;
    *) die "no stack is running; start one with 'stack up a' or 'stack up b'" ;;
  esac
}

orchestrator_up() { curl -sf -m 3 "$API/inventory" >/dev/null 2>&1; }

wait_orchestrator() {
  local deadline=$((SECONDS + ${1:-60}))
  until orchestrator_up; do
    ((SECONDS > deadline)) && return 1
    sleep 2
  done
}

# fleet_line prints "name=height/tip ..." from the orchestrator, and
# "CONVERGED" or "SPLIT" on a second line.
fleet_line() {
  curl -sf -m 5 "$API/state" | python3 -I -c '
import json, sys
nodes = json.load(sys.stdin)["snapshot"]["nodes"]
print(" ".join("%s=%s/%s" % (n["name"], n.get("height"), (n.get("tip") or "?")[:8]) for n in nodes))
tips = {n.get("tip") for n in nodes}
print("CONVERGED" if len(tips) == 1 and None not in tips and all(n.get("reachable") for n in nodes) else "SPLIT")
'
}

wait_converged() {
  local timeout="${1:-120}" deadline out
  deadline=$((SECONDS + timeout))
  while true; do
    out=$(fleet_line 2>/dev/null) || out=$'?\nSPLIT'
    if [[ "$(tail -1 <<<"$out")" == CONVERGED ]]; then
      note "fleet converged: $(head -1 <<<"$out")"
      return 0
    fi
    if ((SECONDS > deadline)); then
      echo "fleet not converged after ${timeout}s: $(head -1 <<<"$out")" >&2
      return 1
    fi
    sleep 5
  done
}

image_of_node1() {
  local c
  for c in teranode1-multinode teranode1-core-multinode bsv-regtest-teranode1; do
    if docker inspect "$c" >/dev/null 2>&1; then
      docker inspect -f '{{.Config.Image}} {{.Image}}' "$c" | awk '{print $1 "@" substr($2, 8, 12)}'
      return
    fi
  done
  echo unknown
}

# ---- stacks -----------------------------------------------------------------

stack_status() {
  local s
  s=$(active_stack)
  echo "stack:        $s"
  if [[ "$s" == a || "$s" == b ]]; then
    echo "image:        $(image_of_node1)"
    if orchestrator_up; then
      local out
      out=$(fleet_line)
      echo "orchestrator: up ($API)"
      if curl -sf -m 2 "http://localhost:$((${ARCADE_HOST_PORT:-18080} + 1))/health" >/dev/null 2>&1; then
        echo "arcade:       up (http://localhost:${ARCADE_HOST_PORT:-18080})"
      else
        echo "arcade:       down"
      fi
      echo "fleet:       $(head -1 <<<"$out") [$(tail -1 <<<"$out")]"
      local run
      run=$(curl -sf -m 5 "$API/runs" | python3 -I -c '
import json, sys
for r in json.load(sys.stdin):
    if r.get("status") in ("running", "paused"):
        print("%s (%s)" % (r["id"], r["status"]))
')
      [[ -n "$run" ]] && echo "active run:   $run"
    else
      echo "orchestrator: down"
    fi
  fi
  blast_status
}

check_ct() {
  [[ -f "$CT/sim/Dockerfile" && -f "$CT/bsv-regtest/compose.yaml" ]] ||
    die "chaos-test checkout not found at $CT (git clone https://github.com/galt-tr/chaos-test \"$CT\", or set CHAOS_TEST_DIR)"
}

stack_up() {
  local which="${1:-}" switch=false
  shift || true
  local -a rest=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --switch) switch=true ;;
      *) rest+=("$1") ;;
    esac
    shift
  done
  local other=b
  [[ "$which" == b ]] && other=a
  [[ "$which" == a || "$which" == b ]] || die "usage: stack up a|b"
  local s
  s=$(active_stack)
  if [[ "$s" == "$other" || "$s" == both ]]; then
    $switch || die "stack $other is running (shared host ports); pass --switch to bring it down first"
    stack_down "$other"
  fi
  docker image inspect "$TERANODE_IMAGE" >/dev/null 2>&1 || die "image $TERANODE_IMAGE not found; build it with 'make build' or set TERANODE_IMAGE"

  if [[ "$which" == a ]]; then
    [[ ${#rest[@]} -eq 0 ]] && rest=(3)
    "$MULTINODE" up "${rest[@]}" || die "multinode.sh up failed"
    "$MULTINODE" sim up || die "multinode.sh sim up failed"
  else
    check_ct
    local env="$CT/bsv-regtest/.env" ov="$CT/bsv-regtest/compose.override.yaml"
    if [[ ! -f "$env" ]]; then
      printf 'TERANODE_IMAGE_%s=%s\n' 1 "$TERANODE_IMAGE" 2 "$TERANODE_IMAGE" 3 "$TERANODE_IMAGE" >"$env"
      note "wrote $env (all teranodes on $TERANODE_IMAGE)"
    else
      note "keeping existing $env: $(tr '\n' ' ' <"$env")"
    fi
    if [[ ! -f "$ov" ]]; then
      cp "$SCRIPT_DIR/bsv-regtest.override.yaml" "$ov"
      note "installed $ov"
    elif ! cmp -s "$SCRIPT_DIR/bsv-regtest.override.yaml" "$ov"; then
      note "warning: $ov differs from compose/chaostest/bsv-regtest.override.yaml; keeping yours"
    fi
    local img
    for img in localhost/bsv-regtest/tools:local localhost/bsv-regtest/walletd:local; do
      docker image inspect "$img" >/dev/null 2>&1 || make -C "$CT/bsv-regtest" RUNTIME=docker "build-${img#localhost/bsv-regtest/}" >/dev/null || die "could not build $img"
    done
    docker images --format '{{.Repository}}' | grep -q '^localhost/bsv-regtest/alert-system$' ||
      make -C "$CT/bsv-regtest" RUNTIME=docker build-alert-system || die "could not build the alert-system image"
    docker image inspect localhost/chaos-orchestrator:local >/dev/null 2>&1 ||
      make -C "$CT" RUNTIME=docker sim-build || die "could not build the orchestrator image"
    note "starting chaos-test's fleet (make up wait)..."
    make -C "$CT/bsv-regtest" RUNTIME=docker up wait || die "fleet did not come up"
    make -C "$CT" RUNTIME=docker sim-up >/dev/null || die "orchestrator did not start"
  fi
  wait_orchestrator 60 || die "orchestrator API not answering on $API"
  wait_converged 180 || true
  stack_status
}

stack_down() {
  local which="${1:-}"
  [[ "$which" == a || "$which" == b ]] || die "usage: stack down a|b"
  blast_running && blast_stop
  if [[ "$which" == a ]]; then
    [[ -f "$REPO_ROOT/compose/generated/docker-compose-multinode.yml" ]] && "$MULTINODE" down
  else
    check_ct
    (cd "$CT/sim" && docker compose down >/dev/null 2>&1) || true
    make -C "$CT/bsv-regtest" RUNTIME=docker down
  fi
}

# stack_reset wipes the chain of the active stack. Callers that use the blaster
# restart it with --fresh afterwards: its coinbase DB belongs to the old chain.
stack_reset() {
  local s
  s=$(require_stack)
  blast_running && blast_stop
  if [[ "$s" == b ]]; then
    check_ct
    note "resetting chaos-test's fleet (make reset)..."
    make -C "$CT" RUNTIME=docker reset >"$STATE/reset.log" 2>&1 || die "make reset failed; see $STATE/reset.log"
  else
    local n
    n=$(python3 -I -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["nodes"]))' \
      "$REPO_ROOT/compose/generated/chaostest/inventory.json" 2>/dev/null || echo 3)
    note "resetting the multinode stack ($n nodes)..."
    "$MULTINODE" down >/dev/null && "$MULTINODE" up "$n" >"$STATE/reset.log" 2>&1 && "$MULTINODE" sim up >>"$STATE/reset.log" 2>&1 ||
      die "reset failed; see $STATE/reset.log"
  fi
  wait_orchestrator 120 || die "orchestrator API not answering after reset"
  wait_converged 180 || true
}

# ---- blaster ------------------------------------------------------------------

blast_running() {
  [[ -f "$STATE/blast.pid" ]] && kill -0 "$(cat "$STATE/blast.pid")" 2>/dev/null
}

blast_last_stats() {
  [[ -f "$STATE/blast.log" ]] && grep -aE '^\[[0-9:]+\] (\[[^]]*\] )?tx=' "$STATE/blast.log" | tail -1
}

blast_status() {
  if blast_running; then
    echo "blaster:      running (pid $(cat "$STATE/blast.pid"), preset $(cat "$STATE/blast.preset" 2>/dev/null), log $STATE/blast.log)"
    local st
    st=$(blast_last_stats)
    echo "              ${st:-no stats yet}"
  else
    echo "blaster:      not running"
  fi
}

blast_presets() {
  local f
  for f in "$PRESETS"/*.json; do
    printf '  %-26s %s\n' "$(basename "$f" .json)" \
      "$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["description"])' "$f")"
  done
}

# build_schedule merges a preset onto the blaster's default mix (a schedule phase
# REPLACES the whole mix, so every field must be present) and writes a one-phase,
# looping schedule. Prints the preset's burst pattern, if any.
build_schedule() {
  local preset="$1" out="$2"
  python3 -I - "$PRESETS/$preset.json" "$out" <<'EOF'
import json, sys
src, out = sys.argv[1], sys.argv[2]
p = json.load(open(src))
# DefaultTxMix() in teranode-coinbase/cmd/blaster/tx_types.go
mix = {"Weights": [80, 0, 5, 5, 0, 5, 2, 0, 3, 0, 0, 0, 0, 0, 0, 0],
       "FanOutN": 10, "MultisigM": 2, "MultisigN": 3, "ds_strategy": 0, "ds_delay_ms": 500,
       "large_data_size": 10240, "consolidate_n": 10, "invalid_mode": "bad-sig",
       "orphan_delay_ms": 1000}
for k, v in p.get("mix", {}).items():
    key = {"ConsolidateN": "consolidate_n"}.get(k, k)
    mix[key] = v
if len(mix["Weights"]) != 16 or sum(mix["Weights"]) != 100:
    sys.exit("preset weights must be 16 entries summing to 100")
# Node indices -> the propagation addresses multinode.sh blast passes.
chaos = {"localhost:%d" % (20000 + (int(n) - 1) * 2000 + 84): v for n, v in p.get("node_chaos", {}).items()}
if chaos:
    mix["node_chaos"] = chaos
# time.Duration has no text form in encoding/json: nanoseconds. One day, looping.
json.dump({"phases": [{"name": src.rsplit("/", 1)[-1][:-5], "mix": mix, "duration": 86400 * 10**9}], "loop": True},
          open(out, "w"), indent=2)
print(p.get("burst_pattern", ""))
EOF
}

rpc_generate() {
  curl -sf -m 120 -u bitcoin:bitcoin -H 'Content-Type: application/json' \
    -d "{\"method\":\"generate\",\"params\":[$1]}" http://localhost:21292 >/dev/null
}

# blast_fail stops a blaster that failed to start properly, then exits.
blast_fail() {
  tail -20 "$STATE/blast.log" >&2
  blast_stop >&2
  die "$*"
}

blast_start() {
  local preset=default workers=10 fresh=false fund=true mine=true
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --workers) workers="${2:?--workers needs a number}"; shift ;;
      --fresh) fresh=true ;;
      --no-fund) fund=false ;;
      --no-mine) mine=false ;;
      -*) die "blast start: unknown flag $1" ;;
      *) preset="$1" ;;
    esac
    shift
  done
  [[ -f "$PRESETS/$preset.json" ]] || die "unknown preset '$preset'; see 'blast presets'"
  local s
  s=$(require_stack)
  blast_running && die "the blaster is already running ($(cat "$STATE/blast.preset")); 'blast stop' first"

  local burst
  burst=$(build_schedule "$preset" "$STATE/schedule.json") || die "bad preset $preset"
  local -a cmd=("$MULTINODE" blast)
  [[ "$s" == b ]] && cmd+=(--target bsv-regtest)
  $fresh && cmd+=(--fresh)
  $mine && cmd+=(--auto-mine)
  cmd+=(-- --headless --stats-interval 10s --workers "$workers" --mix-schedule "$STATE/schedule.json")
  [[ -n "$burst" ]] && cmd+=(--burst-pattern "$burst")

  # Funding comes from the 110 blocks mined below; the auto-miner only tops it up.
  # Stack B's SQLite followers trail a block every 5 s under load and never reach
  # one tip, which fails every scenario's same_tip check, so mine slower there.
  local interval="${BLAST_AUTO_MINE_INTERVAL:-5}"
  [[ "$s" == b && -z "${BLAST_AUTO_MINE_INTERVAL:-}" ]] && interval=30

  local mining="a block every ${interval}s"
  $mine || mining="no auto-mine"
  note "starting blaster on stack $s: preset $preset, $workers workers, $mining"
  # Own process group, so 'blast stop' reaches multinode.sh, its auto-miner and the blaster.
  set -m
  BLAST_AUTO_MINE_INTERVAL="$interval" "${cmd[@]}" >"$STATE/blast.log" 2>&1 &
  echo $! >"$STATE/blast.pid"
  set +m
  echo "$preset" >"$STATE/blast.preset"
  echo "$mine" >"$STATE/blast.mine"

  local deadline=$((SECONDS + 90))
  until grep -aq 'Standalone coinbase service started' "$STATE/blast.log" 2>/dev/null; do
    blast_running || blast_fail "blaster exited during start-up"
    ((SECONDS > deadline)) && blast_fail "blaster did not start within 90s"
    sleep 2
  done

  if $fund; then
    # The embedded coinbase only sees blocks mined while it runs, and needs 100
    # confirmations: mine 110 now rather than waiting for the auto-miner.
    note "funding: mining 110 blocks on teranode1..."
    local i
    for i in $(seq 1 11); do rpc_generate 10 || blast_fail "generate failed on localhost:21292"; done
    note "waiting for the blaster to send..."
    deadline=$((SECONDS + 240))
    while true; do
      local st
      st=$(blast_last_stats)
      if [[ "$st" =~ tx=([0-9]+) ]] && ((BASH_REMATCH[1] > 0)); then
        note "sending: $st"
        break
      fi
      blast_running || blast_fail "blaster exited"
      ((SECONDS > deadline)) && blast_fail "no transactions after 240s; see $STATE/blast.log"
      sleep 5
    done
  fi
}

blast_stop() {
  blast_running || { note "blaster not running"; rm -f "$STATE/blast.pid"; return 0; }
  local pg
  pg=$(cat "$STATE/blast.pid")
  note "stopping blaster (process group $pg); last: $(blast_last_stats)"
  kill -INT -- "-$pg" 2>/dev/null
  local i
  for i in $(seq 1 20); do kill -0 "$pg" 2>/dev/null || break; sleep 1; done
  kill -0 "$pg" 2>/dev/null && kill -TERM -- "-$pg" 2>/dev/null && sleep 3
  kill -0 "$pg" 2>/dev/null && kill -KILL -- "-$pg" 2>/dev/null
  rm -f "$STATE/blast.pid"
}

# ---- scenarios --------------------------------------------------------------

scenario_list() {
  require_stack >/dev/null
  orchestrator_up || die "orchestrator not answering on $API"
  curl -sf "$API/scenarios" | python3 -I -c '
import json, sys
for s in json.load(sys.stdin):
    print("  %-32s %s" % (s["id"], s.get("name", "")))
'
}

# stats_json turns a blaster stats line into JSON (empty object when none).
stats_json() {
  python3 -I -c '
import json, re, sys
line = sys.argv[1]
d = {k: v for k, v in re.findall(r"([a-z_/]+)=([0-9.]+)", line)}
print(json.dumps({"line": line, **d} if line else {}))' "${1:-}"
}

# run_one starts a scenario, follows it, records a summary and compares it with
# the previous run of the same scenario on the same stack. Returns 0 if passed.
run_one() {
  local id="$1" mode="$2" stack="$3"
  local before run_id resp
  before=$(blast_last_stats)
  resp=$(curl -s -m 10 -X POST -H 'Content-Type: application/json' -d "{\"mode\":\"$mode\"}" "$API/scenarios/$id/run")
  run_id=$(python3 -I -c 'import json, sys; print(json.loads(sys.argv[1])["id"])' "$resp" 2>/dev/null) ||
    { echo "error: the orchestrator refused to start '$id': ${resp:-no response}" >&2; return 1; }
  echo "== $id: run $run_id ($mode) on stack $stack  ui=${API%/api}/#/scenarios"
  [[ "$mode" == step ]] && echo "   step mode: release each step in the UI or POST $API/runs/$run_id/next"

  local printed=0 out st
  while true; do
    out=$(curl -sf -m 10 "$API/runs/$run_id" | python3 -I -c '
import json, sys
run = json.load(sys.stdin)
done = [s for s in run.get("steps") or [] if s.get("status") not in (None, "", "pending", "running")]
for s in done[int(sys.argv[1]):]:
    print("STEP   %-7s %s%s" % (s.get("status"), s.get("name"), (" - " + s["error"]) if s.get("error") else ""))
print("COUNT %d" % len(done))
print("STATUS %s" % run.get("status"))' "$printed") || { sleep 3; continue; }
    grep '^STEP' <<<"$out" | sed 's/^STEP //'
    printed=$(sed -n 's/^COUNT //p' <<<"$out")
    st=$(sed -n 's/^STATUS //p' <<<"$out")
    [[ "$st" != running && "$st" != paused ]] && break
    sleep 3
  done

  local after summary
  after=$(blast_last_stats)
  summary="$STATE/runs/$run_id.json"
  curl -sf -m 10 "$API/runs/$run_id" >"$STATE/run.tmp" || { echo "error: could not fetch run $run_id" >&2; return 1; }
  python3 -I - "$STATE/run.tmp" "$summary" "$stack" "$(image_of_node1)" \
    "$(cat "$STATE/blast.preset" 2>/dev/null || true)" "$(stats_json "$before")" "$(stats_json "$after")" \
    "$(blast_running && echo on || echo off)" "$STATE/runs" <<'EOF'
import glob, json, os, re, sys
src, out, stack, image, preset, before, after, blaster, runs = sys.argv[1:]
r = json.load(open(src))

def key(f):
    # 'step "X": check (detail...)' -> 'X | check'; details carry counts that vary run to run.
    m = re.match(r'step "(.*?)": (\w+)', f)
    return "%s | %s" % m.groups() if m else f[:80]

failed = [s["name"] + (" - " + s["error"] if s.get("error") else "") for s in r.get("steps") or [] if s.get("status") == "failed"]
summary = {
    "run": r["id"], "scenario": r["scenarioId"], "stack": stack, "image": image, "status": r["status"],
    "steps": "%d/%d" % (sum(1 for s in r.get("steps") or [] if s.get("status") == "passed"), len(r.get("steps") or [])),
    "failed": failed, "findings": r.get("findings") or [],
    "blaster": {"state": blaster, "preset": preset if blaster == "on" else "", "before": json.loads(before), "after": json.loads(after)},
    "startedAt": r.get("startedAt"), "finishedAt": r.get("finishedAt"),
}
prev = None
for p in sorted(glob.glob(os.path.join(runs, "*.json"))):
    if p == out:
        continue
    d = json.load(open(p))
    if d.get("scenario") == summary["scenario"] and d.get("stack") == stack:
        prev = d
json.dump(summary, open(out, "w"), indent=2)

print("   result: %s, %s steps, %d findings, blaster %s%s" % (summary["status"], summary["steps"], len(summary["findings"]),
      blaster, (" (" + preset + ", " + json.loads(after).get("line", "") + ")") if blaster == "on" else ""))
for f in failed:
    print("   failed: " + f)
for f in summary["findings"]:
    print("   finding: " + f)
if prev:
    now, was = {key(f) for f in summary["findings"]}, {key(f) for f in prev["findings"]}
    changes = []
    if prev["status"] != summary["status"]:
        changes.append("status %s -> %s" % (prev["status"], summary["status"]))
    changes += ["new finding: " + k for k in sorted(now - was)]
    changes += ["finding gone: " + k for k in sorted(was - now)]
    print("   vs %s (%s, blaster %s): %s" % (prev["run"], prev["image"], prev["blaster"]["state"],
          "; ".join(changes) if changes else "same result"))
print("   saved: " + out)
EOF
  [[ "$st" == passed ]]
}

scenario_run() {
  local -a ids=()
  local load=false preset="" reset=false mode=auto workers=10
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --under-load)
        load=true
        if [[ $# -gt 1 && "$2" != -* && -f "$PRESETS/$2.json" ]]; then preset="$2"; shift; fi ;;
      --reset) reset=true ;;
      --step) mode=step ;;
      --workers) workers="${2:?}"; shift ;;
      -*) die "scenario run: unknown flag $1" ;;
      *) ids+=("$1") ;;
    esac
    shift
  done
  [[ ${#ids[@]} -gt 0 ]] || die "usage: scenario run <id>... [--under-load [preset]] [--reset] [--step]"
  local s
  s=$(require_stack)
  orchestrator_up || die "orchestrator not answering on $API ('stack up $s' starts it)"
  # Unknown ids fail now, not after a reset and a funding round.
  local missing
  missing=$(curl -sf -m 10 "$API/scenarios" | python3 -I -c '
import json, sys
have = {s["id"] for s in json.load(sys.stdin)}
print(" ".join(i for i in sys.argv[1:] if i not in have))' "${ids[@]}")
  if [[ -n "$missing" ]]; then
    local other=b
    [[ "$s" == b ]] && other=a
    die "not loaded on stack $s: $missing (bundled chaos-test scenarios run on stack b, multinode-* on stack a; 'scenario list' shows what is here, 'stack up $other --switch' changes stack)"
  fi
  if $load && blast_running && [[ -n "$preset" && "$preset" != "$(cat "$STATE/blast.preset")" ]]; then
    note "blaster already running with preset $(cat "$STATE/blast.preset"); keeping it (stop it to change presets)"
  fi
  [[ -z "$preset" ]] && preset=default
  if blast_running && [[ "$(cat "$STATE/blast.mine" 2>/dev/null)" == true ]]; then
    note "warning: the running blaster auto-mines on teranode1, a scenario role; checks on exact heights will fail. 'blast stop' and let --under-load restart it, or run with --reset"
  fi

  local id failures=0 first=true
  for id in "${ids[@]}"; do
    if $reset; then
      stack_reset
      # The old chain is gone: the blaster must forget it and be funded again.
      # --no-mine: the auto-miner mines on teranode1, which is a scenario role, and
      # scenarios check exact heights; they mine what they need themselves.
      $load && blast_start "$preset" --workers "$workers" --fresh --no-mine
    elif $load && ! blast_running; then
      blast_start "$preset" --workers "$workers" --no-mine
    fi
    if ! $first || ! $reset; then
      wait_converged 180 || note "starting anyway; the scenario's own same_tip checks decide"
    fi
    first=false
    run_one "$id" "$mode" "$s" || failures=$((failures + 1))
  done
  ((failures == 0))
}

scenario_last() {
  local id="${1:?usage: scenario last <id>}" s
  s=$(active_stack)
  python3 -I - "$STATE/runs" "$id" "$s" <<'EOF'
import glob, json, os, sys
runs, sid, stack = sys.argv[1:]
hits = [json.load(open(p)) for p in sorted(glob.glob(os.path.join(runs, "*.json")))]
hits = [h for h in hits if h["scenario"] == sid and (stack not in ("a", "b") or h["stack"] == stack)]
if not hits:
    sys.exit("no recorded run of %s" % sid)
print(json.dumps(hits[-1], indent=2))
EOF
}

# ---- dispatch ---------------------------------------------------------------

[[ $# -eq 0 ]] && usage
group="$1"; shift
sub="${1:-}"; [[ $# -gt 0 ]] && shift
case "$group:$sub" in
  stack:status) stack_status ;;
  stack:up) stack_up "$@" ;;
  stack:down) stack_down "$@" ;;
  stack:reset) stack_reset ;;
  blast:presets) blast_presets ;;
  blast:start) blast_start "$@" ;;
  blast:status) blast_status ;;
  blast:stop) blast_stop ;;
  scenario:list) scenario_list ;;
  scenario:run) scenario_run "$@" ;;
  scenario:last) scenario_last "$@" ;;
  *) usage ;;
esac

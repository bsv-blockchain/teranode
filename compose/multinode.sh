#!/usr/bin/env bash
set -euo pipefail

# This script uses associative arrays (`local -A`), which need bash >= 4.
# macOS still ships bash 3.2 as /bin/bash, and there `local -A` fails in a way
# `set -e` does NOT catch: the function aborts but the script exits 0, so
# `up` silently starts nothing and reports success. Re-exec under a newer
# bash when one is installed rather than leaving that trap in place.
if ((BASH_VERSINFO[0] < 4)); then
  for _newer_bash in /opt/homebrew/bin/bash /usr/local/bin/bash; do
    if [[ -x "$_newer_bash" ]]; then
      exec "$_newer_bash" "$0" "$@"
    fi
  done
  echo "error: multinode.sh needs bash >= 4 (found ${BASH_VERSION})." >&2
  echo "       Install one with 'brew install bash' - no need to change your login shell." >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GEN_DIR="$SCRIPT_DIR/generated"
BLASTER_DATA_DIR="$REPO_ROOT/data/multinode-blaster"
COMPOSE_FILE="$GEN_DIR/docker-compose-multinode.yml"

# Chaos primitives run iptables/tc inside the target container's netns via a
# privileged sidecar. Works on Linux and macOS (no host nsenter/sudo needed).
NETSHOOT_IMAGE="${NETSHOOT_IMAGE:-nicolaka/netshoot:v0.13}"

usage() {
  cat <<'EOF'
Usage: compose/multinode.sh <command> [args]

Commands:
  up <N> [--build] [-allinone=0|1] [--skip N:svc ...]
         [--limits svc=cpus[:mem],...] [--retention production|aggressive|prod-scaling]
                           Generate and start an N-node network (3-10).
                           -allinone=1 (default): one container per node runs
                             all services in a single process.
                           -allinone=0: split topology - each node becomes 9
                             containers (one per microservice: blockchain,
                             blockassembly, blockvalidation, subtreevalidation,
                             validator, propagation, p2p, asset, and a 'core'
                             sidecar for rpc/alert/blockpersister/etc.). Heavier
                             but lets chaos commands target individual services.
                           --skip N:svc: don't start the named service on node N
                             (split mode only; repeatable). Useful for chaos
                             scenarios where a node should come up missing a
                             specific service. Bring it up later with
                             'chaos start N svc'. blockchain cannot be --skipped
                             because sibling services depend on it.
                           --limits svc=cpus[:mem]: cap a service's CPU and
                             memory (split mode only; comma-separated for
                             several). Use it to put a service under
                             deterministic pressure rather than relying on
                             load alone, e.g. --limits blockassembly=2:8g.
                           --retention aggressive|prod-scaling: aggressive
                             shrinks the pruner retention
                             windows (288/144/1440 -> 20/10/40) so pruning is
                             observable within tens of blocks on a fresh
                             regtest chain. Default 'production' inherits
                             settings.conf unchanged. Values live in
                             compose/cmd/gennodes/main.go (aggressiveRetention).
  build                    Build the teranode Docker image
  down                     Stop and remove all containers and volumes
  restart                  Restart all containers (picks up config changes)
  status                   Show container status
  logs [node[-svc]]        Tail logs (all, or a node, or a split-mode service)
  dashboards               Open all dashboards in the browser
  generate <n,count> ...   Generate blocks on specific nodes
                           e.g. generate 1,10 3,5
  blast [nodes] [--build] [--auto-mine[=N]] [--fresh] [--target T] [-- args]
                           Run the coinbase blaster against the stack.
                           'nodes' is a comma/space-separated list (default: all
                           running). --build rebuilds the blaster first.
                           --auto-mine spawns a background miner on node N
                           (default: first target) every 5s; override with
                           BLAST_AUTO_MINE_INTERVAL=<seconds>. --fresh wipes the
                           blaster's saved state first. --target bsv-regtest
                           blasts chaos-test's fleet instead of this stack
                           (funds from PK2 so chaos-test scenarios keep PK1).
                           Flags after '--' are passed to blaster.

Chaos:
  chaos isolate <node>            Block peer traffic (RPC still works).
                                  In split mode targets teranode<N>-p2p only.
  chaos heal [node]               Restore peer traffic (or all nodes if omitted)
  chaos kill <node> [service]     Stop a node container, or one service in
                                  split mode (e.g. 'chaos kill 2 validator').
  chaos start <node> [service]    Start a stopped node or service container
  chaos pause <node> [service]    Freeze a node or service (simulates hang)
  chaos unpause <node> [service]  Unfreeze a paused node or service
  chaos slow <node> <ms>          Add network latency (split mode: -p2p only)
  chaos unslow <node>             Remove added latency

chaos-test scenarios (github.com/galt-tr/chaos-test, checkout in ../chaos-test
or CHAOS_TEST_DIR):
  sim up [--build] [--no-arcade]  Start the scenario orchestrator + UI on :8600
                                  (SIM_HOST_PORT) and arcade on :18080
                                  (ARCADE_HOST_PORT) against this stack. Builds
                                  the image on first use; --build rebuilds it.
  sim run <scenario> [--step]     Run a scenario and follow it to completion
  sim status | logs | down        Orchestrator state, logs, or remove it
                                  (also removed by 'down')

Examples:
  compose/multinode.sh up 5
  compose/multinode.sh up 3 -allinone=0          # split-services topology
  compose/multinode.sh up 3 -allinone=0 --skip 2:blockassembly  # node 2 starts without block assembly
  compose/multinode.sh generate 1,10 3,5
  compose/multinode.sh blast              # blast all running nodes (TUI)
  compose/multinode.sh blast --build --auto-mine  # rebuild, mine on node 1
  compose/multinode.sh blast 1,3 -- --headless --max-tps 50
  compose/multinode.sh blast --target bsv-regtest --fresh --auto-mine -- --workers 10
  compose/multinode.sh chaos isolate 3
  compose/multinode.sh chaos kill 2 validator    # split mode: only validator
  compose/multinode.sh chaos heal
  compose/multinode.sh chaos slow 2 500
  compose/multinode.sh logs 2
  compose/multinode.sh logs 2-validator          # split mode service log
  compose/multinode.sh sim up && compose/multinode.sh sim run multinode-smoke
  compose/multinode.sh down
EOF
  exit 2
}

require_stack() {
  if [[ ! -f "$COMPOSE_FILE" ]]; then
    echo "error: no multinode stack found. Run '$0 up <N>' first." >&2
    exit 1
  fi
}

# Canonical list of split-mode services. Must track buildSplitServices() in
# compose/cmd/gennodes/main.go. Used by --skip validation at 'up' time and by
# the chaos kill/start/pause/unpause service-arg validation so a typo like
# 'valdiator' surfaces a friendly error instead of a raw docker/compose one.
KNOWN_SPLIT_SERVICES=" blockchain blockassembly blockvalidation subtreevalidation validator propagation p2p asset core "

# assert_known_service exits 2 with a friendly error if $1 is not one of the
# split-mode service names. $2 is a short label used in the error message
# (e.g. "chaos kill", "--skip") so the user knows which arg was rejected.
assert_known_service() {
  local svc="$1"
  local ctx="$2"
  if [[ "$KNOWN_SPLIT_SERVICES" != *" $svc "* ]]; then
    echo "error: $ctx: unknown service '$svc' (valid:$KNOWN_SPLIT_SERVICES)" >&2
    exit 2
  fi
}

# clean_blaster_state removes the embedded coinbase's sqlite DB.
#
# This MUST happen whenever the chain is destroyed, and only then: 'up' keeps
# the existing chain, so the DB stays valid across it. The coinbase records the
# UTXOs it owns in that DB; pointed at a new chain it keeps trying to spend
# outputs of transactions that chain has never seen, and every funding attempt
# fails with TX_MISSING_PARENT on a transaction no node can produce. The
# symptom is remote from the cause - the blaster reports 100% fund errors and
# logs the reason only to an in-memory buffer headless mode never prints.
#
# The DB does not live where you would expect. The blaster stores it at
# sqlite:///<snapshot dir>/coinbase.db, and teranode's sqlite store resolves
# that by joining dataFolder with the path minus its leading slash, then
# appending ".db". With the absolute snapshot path cmd_blast passes and
# dataFolder pinned to $REPO_ROOT/data, the DB therefore lands at
# $REPO_ROOT/data$BLASTER_DATA_DIR/coinbase.db.db - data/Users/... on macOS,
# data/home/... on Linux. Remove exactly that tree, then any parents it leaves
# empty, rather than anything else under the shared data/ directory.
#
# $1 is the blaster state directory, default $BLASTER_DATA_DIR ('blast
# --target bsv-regtest' keeps its own).
clean_blaster_state() {
  local stale="$REPO_ROOT/data${1:-$BLASTER_DATA_DIR}"
  if [[ -d "$stale" ]]; then
    rm -rf "$stale"
    echo "cleaned stray coinbase DB: $stale"
    local parent
    parent="$(dirname "$stale")"
    while [[ "$parent" != "$REPO_ROOT/data" ]] && rmdir "$parent" 2>/dev/null; do
      parent="$(dirname "$parent")"
    done
  fi
}

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

cmd_build() {
  require_stack
  echo "building teranode image..."
  compose build teranode-builder
  echo "teranode:latest image built"
}

cmd_up() {
  local n=""
  local do_build=false
  local allinone=1
  local limits=""
  local retention="production"
  local -a skip_specs=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --build) do_build=true; shift ;;
      --limits=*|-limits=*) limits="${1#*=}"; shift ;;
      --limits|-limits)
        if [[ $# -lt 2 ]]; then
          echo "error: --limits requires an argument (e.g. --limits blockassembly=2:8g)" >&2
          exit 2
        fi
        limits="$2"; shift 2 ;;
      --retention=*|-retention=*) retention="${1#*=}"; shift ;;
      --retention|-retention)
        if [[ $# -lt 2 ]]; then
          echo "error: --retention requires an argument (production|aggressive|prod-scaling)" >&2
          exit 2
        fi
        retention="$2"; shift 2 ;;
      -allinone=0|--allinone=0) allinone=0; shift ;;
      -allinone=1|--allinone=1) allinone=1; shift ;;
      -allinone=*|--allinone=*)
        echo "error: -allinone must be 0 or 1, got '${1#*=}'" >&2
        exit 2
        ;;
      --skip=*|-skip=*) skip_specs+=("${1#*=}"); shift ;;
      --skip|-skip)
        if [[ $# -lt 2 ]]; then
          echo "error: --skip requires an argument (e.g. --skip 2:blockassembly)" >&2
          exit 2
        fi
        skip_specs+=("$2"); shift 2 ;;
      *)       n="$1"; shift ;;
    esac
  done
  if [[ -z "$n" ]]; then
    echo "error: specify number of nodes, e.g. '$0 up 5'" >&2
    exit 2
  fi
  # Validate --limits / --retention up front, before we spend time generating.
  # gennodes re-validates both; these checks just fail faster and with a
  # message that names the multinode.sh flag the user actually typed.
  if [[ -n "$limits" ]]; then
    if [[ "$allinone" -ne 0 ]]; then
      echo "error: --limits requires -allinone=0; there's no per-service granularity in all-in-one mode" >&2
      exit 2
    fi
    local entry svc_name
    IFS=',' read -ra _limit_entries <<< "$limits"
    for entry in "${_limit_entries[@]}"; do
      if [[ ! "$entry" =~ ^([a-z0-9]+)=(([0-9]+(\.[0-9]+)?)(:[0-9]+[kmgKMG]?)?|:[0-9]+[kmgKMG]?)$ ]]; then
        echo "error: --limits must be svc=cpus[:mem] or svc=:mem (e.g. blockassembly=2:8g), got '$entry'" >&2
        exit 2
      fi
      svc_name="${BASH_REMATCH[1]}"
      assert_known_service "$svc_name" "--limits"
    done
  fi
  case "$retention" in
    production|aggressive|prod-scaling) ;;
    *)
      echo "error: --retention must be production, aggressive or prod-scaling, got '$retention'" >&2
      exit 2
      ;;
  esac

  # Validate --skip specs up front, before we spend time generating.
  # Service list must track buildSplitServices() in compose/cmd/gennodes/main.go.
  local -A skip_set=()
  if [[ ${#skip_specs[@]} -gt 0 ]]; then
    if [[ "$allinone" -ne 0 ]]; then
      echo "error: --skip requires -allinone=0; there's no per-service granularity in all-in-one mode" >&2
      exit 2
    fi
    for spec in "${skip_specs[@]}"; do
      if [[ ! "$spec" =~ ^([0-9]+):([a-z0-9]+)$ ]]; then
        echo "error: --skip must be N:service (e.g. --skip 2:blockassembly), got '$spec'" >&2
        exit 2
      fi
      local skip_n="${BASH_REMATCH[1]}"
      local skip_svc="${BASH_REMATCH[2]}"
      if [[ "$skip_n" -lt 1 || "$skip_n" -gt "$n" ]]; then
        echo "error: --skip node '$skip_n' out of range 1..$n" >&2
        exit 2
      fi
      assert_known_service "$skip_svc" "--skip"
      if [[ "$skip_svc" == "blockchain" ]]; then
        # Every other split service on the same node has
        # depends_on: teranodeN-blockchain {condition: service_healthy},
        # so 'compose up -d <keep-list>' would auto-resolve and start it
        # anyway. Refuse the spec so the user isn't surprised.
        echo "error: cannot --skip blockchain via 'up' - sibling services depend on it via service_healthy and compose will auto-start it." >&2
        echo "       To stop blockchain after the stack is up, use: $0 chaos kill $skip_n blockchain" >&2
        exit 2
      fi
      skip_set["teranode${skip_n}-${skip_svc}"]=1
    done
  fi
  local topology="all-in-one"
  [[ "$allinone" -eq 0 ]] && topology="split-per-service"
  echo "generating $n-node stack (topology: $topology, retention: $retention${limits:+, limits: $limits})..."
  (cd "$REPO_ROOT" && go run ./compose/cmd/gennodes \
    -n "$n" -allinone="$allinone" -retention "$retention" -limits "$limits" -o compose/generated)
  if [[ "$do_build" == true ]]; then
    cmd_build
  fi
  if [[ ${#skip_set[@]} -gt 0 ]]; then
    # Build keep-list = all compose services - skipped services. We pass
    # explicit names so docker compose only materialises those containers;
    # the skipped services stay declared in the file (so 'chaos start'
    # can bring them up later via 'compose up -d <svc>').
    local -a keep_list=()
    while IFS= read -r svc; do
      [[ -z "$svc" ]] && continue
      if [[ -z "${skip_set[$svc]:-}" ]]; then
        keep_list+=("$svc")
      fi
    done < <(compose config --services 2>/dev/null | sort)
    echo "skipping services: ${!skip_set[*]}"
    echo "starting containers..."
    compose up -d "${keep_list[@]}"
  else
    echo "starting containers..."
    compose up -d
  fi
  echo ""
  echo "dashboards:"
  for f in "$GEN_DIR"/open-dashboards.sh; do
    [[ -x "$f" ]] && grep -oE 'http://localhost:[0-9]+' "$f" | while read -r url; do
      echo "  $url"
    done
  done
  echo ""
  echo "run '$0 status' to check health"
}

cmd_down() {
  require_stack
  # The orchestrator is attached to the stack's network; remove it first or
  # compose cannot remove the network.
  sim_down
  compose down -v --remove-orphans

  # 'compose down -v' removes named volumes but NOT bind mounts. Aerospike
  # and teranode data dirs under data/multinode/ are bind-mounted and will
  # otherwise persist stale UTXO/block state across runs, which breaks a
  # fresh 'up' with errors like "utxo already spent by tx ...". Wipe them
  # via a root-privileged container because docker created them as root.
  local state_dir="$REPO_ROOT/data/multinode"
  if [[ -d "$state_dir" ]]; then
    echo "wiping bind-mounted state in data/multinode/..."
    if ! docker run --rm -v "$state_dir:/data" alpine sh -c 'rm -rf /data/aerospike* /data/teranode* 2>/dev/null' >/dev/null 2>&1; then
      echo "warning: could not wipe data/multinode/ state; next 'up' may see stale data" >&2
    fi
  fi

  # Wipe blaster local state so a subsequent 'blast' against a fresh chain
  # doesn't try to spend UTXOs that no longer exist.
  local blaster_data_dir="$BLASTER_DATA_DIR"
  clean_blaster_state
  if [[ -d "$blaster_data_dir" ]]; then
    rm -rf "$blaster_data_dir"
    echo "cleaned blaster state: $blaster_data_dir"
  fi
}

cmd_restart() {
  require_stack
  compose down
  compose up -d
}

cmd_status() {
  require_stack

  local json
  json=$(compose ps --format json 2>/dev/null)

  # Collect infra rows + a per-node tally of split-service rows.
  # In all-in-one mode each node produces exactly one service named
  # `teranodeN`; in split mode each node produces 9 services named
  # `teranodeN-<svc>`. We normalise both into a node-indexed map of
  # running/total service counts so the output shape stays consistent.
  local infra_lines=""
  local -a node_order=()        # node indices in first-seen order
  local -A node_seen=()         # 1 if node idx has been recorded
  local -A node_running=()      # count of running services per node
  local -A node_total=()        # count of all services per node
  local -A node_service_lines=() # newline-joined per-service status (split mode only)
  local -A node_status_oneline=() # all-in-one mode: the single-container status string

  while IFS= read -r line; do
    local service state status
    service=$(echo "$line" | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['Service'])" 2>/dev/null) || continue
    state=$(echo "$line" | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['State'])" 2>/dev/null)
    status=$(echo "$line" | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['Status'])" 2>/dev/null)

    if [[ "$service" =~ ^teranode([0-9]+)(-([a-z0-9]+))?$ ]]; then
      local idx="${BASH_REMATCH[1]}"
      local svc="${BASH_REMATCH[3]:-}"   # empty in all-in-one mode; [a-z0-9] so "p2p" matches

      if [[ -z "${node_seen[$idx]:-}" ]]; then
        node_seen[$idx]=1
        node_order+=("$idx")
        node_running[$idx]=0
        node_total[$idx]=0
      fi
      node_total[$idx]=$(( node_total[$idx] + 1 ))
      [[ "$state" == "running" ]] && node_running[$idx]=$(( node_running[$idx] + 1 ))

      if [[ -n "$svc" ]]; then
        local icon="x"
        [[ "$state" == "running" ]] && icon="+"
        [[ "$state" == "paused" ]]  && icon="P"
        node_service_lines[$idx]+=$(printf "\n      [%s] %-18s %s" "$icon" "$svc" "$status")
      else
        node_status_oneline[$idx]="$status:$state"
      fi
    else
      local state_icon="x"
      [[ "$state" == "running" ]] && state_icon="+"
      infra_lines+=$(printf "\n  [%s] %-22s %s" "$state_icon" "$service" "$status")
    fi
  done <<< "$json"

  echo "Infrastructure:$infra_lines"
  echo ""

  # Build node lines outside the herestring loop so curl and docker sidecar spawns work
  local node_lines=""
  local node_count=${#node_order[@]}
  local nodes_ok=0

  for idx in "${node_order[@]}"; do
    local base=$((20000 + (idx - 1) * 2000))
    local dashboard=$((base + 90))
    local rpc=$((base + 1292))
    local health=$((base))
    local state_icon="x"
    local chaos_tag=""
    local height_tag=""
    local total=${node_total[$idx]}
    local running=${node_running[$idx]}

    # Pick a container to probe for height / chaos state: in all-in-one mode
    # there's only one (teranodeN); in split mode probe the -core sidecar
    # (which hosts the RPC) for height, and the -p2p container (which holds
    # the network namespace targeted by chaos isolate/slow) for chaos tags.
    local probe_ctr="" net_ctr=""
    if [[ "$total" -eq 1 ]]; then
      probe_ctr=$(container_name "$idx")
      net_ctr="$probe_ctr"
    else
      probe_ctr="teranode${idx}-core-multinode"
      net_ctr="teranode${idx}-p2p-multinode"
    fi

    if [[ "$running" -gt 0 ]]; then
      [[ "$running" -eq "$total" ]] && state_icon="+" || state_icon="!"
      nodes_ok=$((nodes_ok + 1))
      # One sidecar per node: emit DROP count on line 1, qdisc info on line 2.
      # In split mode net_ctr points at the -p2p container (the netns owner);
      # in all-in-one it's the same container as probe_ctr.
      local chaos_info drop_rules qdisc_line delay
      chaos_info=$(netns_sh "$net_ctr" '
        iptables -L INPUT --line-numbers 2>/dev/null | grep -c DROP || echo 0
        tc qdisc show dev eth0 2>/dev/null
      ' 2>/dev/null || true)
      drop_rules=$(echo "$chaos_info" | sed -n '1p')
      qdisc_line=$(echo "$chaos_info" | sed -n '2,$p')
      if [[ "${drop_rules:-0}" -gt 0 ]]; then chaos_tag+=" ISOLATED"; fi
      if echo "$qdisc_line" | grep -q netem; then
        delay=$(echo "$qdisc_line" | grep -oE '[0-9]+(\.[0-9]+)?ms' | head -1)
        chaos_tag+=" SLOW(${delay})"
      fi
      local height
      height=$(docker exec "$probe_ctr" wget -qO- --timeout=2 \
        --user=bitcoin --password=bitcoin \
        --header='Content-Type: application/json' \
        --post-data='{"method":"getinfo","params":[]}' \
        "http://localhost:9292" 2>/dev/null \
        | grep -o '"blocks":[0-9]*' | cut -d: -f2 || true)
      if [[ -n "$height" ]]; then
        height_tag="  height=$height"
      fi
    fi

    if [[ "$total" -eq 1 ]]; then
      # All-in-one: preserve the historical one-line node row.
      local oneline="${node_status_oneline[$idx]}"
      local status_str="${oneline%:*}"
      node_lines+=$(printf "\n  [%s] teranode%-3s %-24s dashboard=localhost:%d  rpc=localhost:%d  health=localhost:%d%s%s" \
        "$state_icon" "$idx" "$status_str" "$dashboard" "$rpc" "$health" "$height_tag" "$chaos_tag")
    else
      # Split: node summary line, then a row per service container.
      node_lines+=$(printf "\n  [%s] teranode%-3s (%d/%d services)            dashboard=localhost:%d  rpc=localhost:%d  health=localhost:%d%s%s%s" \
        "$state_icon" "$idx" "$running" "$total" "$dashboard" "$rpc" "$health" "$height_tag" "$chaos_tag" "${node_service_lines[$idx]}")
    fi
  done

  echo "Nodes ($nodes_ok/$node_count running):$node_lines"
}

cmd_logs() {
  require_stack
  local target="${1:-}"
  if [[ -z "$target" ]]; then
    compose logs -f
    return
  fi
  # Accept three forms:
  #   2            - all-in-one node, or all services for split-mode node 2
  #   2-validator  - one specific service container in split mode
  #   teranode2-validator (and other compose service names) - passthrough
  case "$target" in
    teranode*) compose logs -f "$target" ;;
    [0-9]*-*)  compose logs -f "teranode${target}" ;;
    [0-9]*)
      if is_split_mode "$target"; then
        local svcs=()
        while read -r ctr; do
          [[ -z "$ctr" ]] && continue
          svcs+=("${ctr%-multinode}")
        done < <(node_service_containers "$target")
        if [[ ${#svcs[@]} -eq 0 ]]; then
          echo "error: no containers found for node $target" >&2
          exit 1
        fi
        compose logs -f "${svcs[@]}"
      else
        compose logs -f "teranode${target}"
      fi
      ;;
    *) compose logs -f "$target" ;;
  esac
}

cmd_dashboards() {
  require_stack
  "$GEN_DIR/open-dashboards.sh"
}

container_name() {
  # In all-in-one mode the container is teranode<N>-multinode.
  # In split mode it's teranode<N>-<svc>-multinode. The caller passes
  # `<n>` (any mode) and optionally `<svc>` for split mode. Without
  # an explicit service in split mode we default to -p2p (the network
  # namespace target for isolate/slow) since that's what made sense
  # for the historical chaos commands.
  local n="$1"
  local svc="${2:-}"
  if [[ -n "$svc" ]]; then
    echo "teranode${n}-${svc}-multinode"
    return
  fi
  # No service specified: pick whichever container exists.
  if docker ps -a --format '{{.Names}}' | grep -qx "teranode${n}-multinode"; then
    echo "teranode${n}-multinode"
  else
    # Default split-mode chaos target is the p2p container (peer traffic).
    echo "teranode${n}-p2p-multinode"
  fi
}

# is_split_mode reports 0 (true) iff node <n> is running in split topology,
# detected by the absence of the monolithic teranode<n>-multinode container.
is_split_mode() {
  local n="$1"
  if docker ps -a --format '{{.Names}}' | grep -qx "teranode${n}-multinode"; then
    return 1
  fi
  return 0
}

# node_service_containers prints every container name for node <n>, one per
# line. Used by chaos kill/start/pause/unpause when no specific service is
# given in split mode so "chaos kill 2" still does something sensible (stop
# every service container for that node).
node_service_containers() {
  local n="$1"
  docker ps -a --format '{{.Names}}' \
    | grep -E "^teranode${n}(-[a-z0-9]+)?-multinode$" || true
}

cmd_chaos() {
  require_stack
  local action="${1:-}"
  shift 2>/dev/null || true

  case "$action" in
    isolate)   chaos_isolate "$@" ;;
    heal)      chaos_heal "$@" ;;
    kill)      chaos_kill "$@" ;;
    start)     chaos_start "$@" ;;
    pause)     chaos_pause "$@" ;;
    unpause)   chaos_unpause "$@" ;;
    slow)      chaos_slow "$@" ;;
    unslow)    chaos_unslow "$@" ;;
    *)
      echo "error: unknown chaos action '$action'" >&2
      echo "actions: isolate, heal, kill, start, pause, unpause, slow, unslow" >&2
      exit 2
      ;;
  esac
}

# Run a single iptables command inside the target container's netns.
netns_iptables() {
  local ctr="$1"
  shift
  docker run --rm \
    --net="container:${ctr}" \
    --cap-add=NET_ADMIN \
    --entrypoint iptables \
    "$NETSHOOT_IMAGE" "$@"
}

# Run a batched shell script inside the target container's netns. Used to
# avoid spawning one sidecar per iptables/tc invocation.
netns_sh() {
  local ctr="$1"
  local script="$2"
  docker run --rm \
    --net="container:${ctr}" \
    --cap-add=NET_ADMIN \
    --entrypoint sh \
    "$NETSHOOT_IMAGE" -c "$script"
}

# peer_containers prints the one container per node that owns the peer
# network namespace: -p2p in split mode, the monolithic container otherwise.
# Used by chaos isolate / heal / slow to enumerate peer targets without
# multi-counting split-mode service containers.
peer_containers() {
  docker ps --filter "name=-multinode" --format '{{.Names}}' \
    | grep -E '^teranode[0-9]+(-p2p)?-multinode$'
}

chaos_isolate() {
  local node="${1:?usage: chaos isolate <node>}"
  local ctr
  ctr=$(container_name "$node")

  local script=""
  local blocked=0
  for other in $(peer_containers | grep -v "^${ctr}$"); do
    local ip
    ip=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$other" 2>/dev/null)
    if [[ -n "$ip" ]]; then
      script+="iptables -A INPUT -s ${ip} -j DROP; "
      script+="iptables -A OUTPUT -d ${ip} -j DROP; "
      blocked=$((blocked + 1))
    fi
  done

  if [[ $blocked -eq 0 ]]; then
    echo "teranode$node has no peers to isolate from"
    return
  fi

  netns_sh "$ctr" "$script"
  echo "teranode$node is isolated from $blocked peer(s) (RPC still accessible)"
}

chaos_heal() {
  local node_num="${1:-}"
  if [[ -n "$node_num" ]]; then
    local ctr
    ctr=$(container_name "$node_num")
    echo "restoring teranode$node_num peer traffic..."
    netns_iptables "$ctr" -F 2>/dev/null || true
    echo "teranode$node_num healed"
    return
  fi
  echo "restoring all nodes..."
  local healed=0
  for ctr in $(peer_containers); do
    local count
    count=$(netns_iptables "$ctr" -L INPUT --line-numbers 2>/dev/null | grep -c DROP || true)
    if [[ "$count" -gt 0 ]]; then
      netns_iptables "$ctr" -F
      echo "  healed $ctr ($count rules cleared)"
      healed=$((healed + 1))
    fi
  done
  if [[ $healed -eq 0 ]]; then
    echo "  all nodes already healthy"
  fi
}

chaos_kill() {
  local node="${1:?usage: chaos kill <node> [service]}"
  local svc="${2:-}"
  if [[ -n "$svc" ]]; then
    assert_known_service "$svc" "chaos kill"
    local target="teranode${node}-${svc}"
    echo "stopping $target..."
    compose stop "$target"
    echo "$target is down"
    return
  fi
  if is_split_mode "$node"; then
    local stopped=0
    while read -r ctr; do
      [[ -z "$ctr" ]] && continue
      local svcname="${ctr%-multinode}"
      compose stop "$svcname"
      stopped=$((stopped + 1))
    done < <(node_service_containers "$node")
    echo "teranode$node is down ($stopped service container(s) stopped)"
    return
  fi
  echo "stopping teranode$node..."
  compose stop "teranode${node}"
  echo "teranode$node is down"
}

chaos_start() {
  local node="${1:?usage: chaos start <node> [service]}"
  local svc="${2:-}"
  if [[ -n "$svc" ]]; then
    assert_known_service "$svc" "chaos start"
    local target="teranode${node}-${svc}"
    echo "starting $target..."
    # 'compose up -d' (rather than 'compose start') so this also works when
    # the container was never created in the first place - the case for any
    # service that was passed to 'up --skip N:svc'. Idempotent for an
    # already-running container.
    compose up -d "$target"
    echo "$target is up"
    return
  fi
  if is_split_mode "$node"; then
    local started=0
    while read -r ctr; do
      [[ -z "$ctr" ]] && continue
      local svcname="${ctr%-multinode}"
      compose start "$svcname"
      started=$((started + 1))
    done < <(node_service_containers "$node")
    echo "teranode$node is up ($started service container(s) started)"
    return
  fi
  echo "starting teranode$node..."
  compose start "teranode${node}"
  echo "teranode$node is up"
}

chaos_pause() {
  local node="${1:?usage: chaos pause <node> [service]}"
  local svc="${2:-}"
  if [[ -n "$svc" ]]; then
    assert_known_service "$svc" "chaos pause"
    local ctr="teranode${node}-${svc}-multinode"
    echo "pausing $ctr (simulating freeze)..."
    docker pause "$ctr"
    echo "$ctr is frozen"
    return
  fi
  if is_split_mode "$node"; then
    local paused=0
    while read -r ctr; do
      [[ -z "$ctr" ]] && continue
      docker pause "$ctr"
      paused=$((paused + 1))
    done < <(node_service_containers "$node")
    echo "teranode$node is frozen ($paused service container(s) paused)"
    return
  fi
  local ctr
  ctr=$(container_name "$node")
  echo "pausing $ctr (simulating freeze)..."
  docker pause "$ctr"
  echo "teranode$node is frozen"
}

chaos_unpause() {
  local node="${1:?usage: chaos unpause <node> [service]}"
  local svc="${2:-}"
  # Suppress 'docker unpause' errors on every branch (matching the existing
  # bulk-split branch below): tests routinely run a defensive defer-unpause
  # alongside an explicit one on the happy path, and a bare 'docker unpause'
  # exits non-zero on an already-running container, which MustRun turns into
  # t.Fatalf. Idempotent semantics are what "chaos cleanup" actually wants.
  if [[ -n "$svc" ]]; then
    assert_known_service "$svc" "chaos unpause"
    local ctr="teranode${node}-${svc}-multinode"
    echo "unpausing $ctr..."
    docker unpause "$ctr" 2>/dev/null || true
    echo "$ctr is unfrozen"
    return
  fi
  if is_split_mode "$node"; then
    local unpaused=0
    while read -r ctr; do
      [[ -z "$ctr" ]] && continue
      docker unpause "$ctr" 2>/dev/null || true
      unpaused=$((unpaused + 1))
    done < <(node_service_containers "$node")
    echo "teranode$node is unfrozen ($unpaused service container(s) unpaused)"
    return
  fi
  local ctr
  ctr=$(container_name "$node")
  echo "unpausing $ctr..."
  docker unpause "$ctr" 2>/dev/null || true
  echo "teranode$node is unfrozen"
}

# Run a single tc command inside the target container's netns.
netns_tc() {
  local ctr="$1"
  shift
  docker run --rm \
    --net="container:${ctr}" \
    --cap-add=NET_ADMIN \
    --entrypoint tc \
    "$NETSHOOT_IMAGE" "$@"
}

chaos_slow() {
  local node="${1:?usage: chaos slow <node> <ms>}"
  local ms="${2:?usage: chaos slow <node> <ms>}"
  local ctr
  ctr=$(container_name "$node")
  echo "adding ${ms}ms latency to teranode$node..."
  # Try add first; if a qdisc is already set, fall back to change.
  if netns_sh "$ctr" "tc qdisc add dev eth0 root netem delay ${ms}ms 2>/dev/null || tc qdisc change dev eth0 root netem delay ${ms}ms"; then
    echo "teranode$node now has ${ms}ms added latency"
  else
    echo "error: failed to set latency on teranode$node" >&2
    return 1
  fi
}

chaos_unslow() {
  local node="${1:?usage: chaos unslow <node>}"
  local ctr
  ctr=$(container_name "$node")
  echo "removing latency from teranode$node..."
  netns_tc "$ctr" qdisc del dev eth0 root 2>/dev/null || true
  echo "teranode$node latency restored to normal"
}

cmd_generate() {
  require_stack
  if [[ $# -eq 0 ]]; then
    echo "error: specify node,count pairs, e.g. '$0 generate 1,10 3,5'" >&2
    exit 2
  fi
  "$GEN_DIR/generate-blocks.sh" "$@"
}

cmd_blast() {
  local -a nodes=()
  local -a passthrough=()
  local sawDashDash=false
  local auto_mine_enabled=false
  local auto_mine_node=""
  local auto_mine_interval="${BLAST_AUTO_MINE_INTERVAL:-5}"
  local do_build=false
  local fresh=false
  local target="multinode"
  while [[ $# -gt 0 ]]; do
    if [[ "$1" == "--" ]]; then
      sawDashDash=true
      shift
      continue
    fi
    if $sawDashDash; then
      passthrough+=("$1")
      shift
      continue
    fi
    case "$1" in
      --build)
        do_build=true
        shift
        ;;
      --auto-mine)
        auto_mine_enabled=true
        shift
        ;;
      --auto-mine=*)
        auto_mine_enabled=true
        auto_mine_node="${1#--auto-mine=}"
        shift
        ;;
      --fresh)
        fresh=true
        shift
        ;;
      --target)
        target="${2:?--target needs multinode or bsv-regtest}"
        shift 2
        ;;
      --target=*)
        target="${1#--target=}"
        shift
        ;;
      *)
        IFS=', ' read -ra items <<< "$1"
        for item in "${items[@]}"; do
          [[ -n "$item" ]] && nodes+=("$item")
        done
        shift
        ;;
    esac
  done

  # Target fleet. Both use the same host port scheme (node N: base
  # 20000 + (N-1)*2000; RPC base+1292, propagation gRPC base+84), so only the
  # container naming and state directory differ. bsv-regtest is chaos-test's
  # fleet (CHAOS_TEST_DIR/bsv-regtest).
  local ctr_re ctr_fmt blaster_data_dir
  case "$target" in
    multinode)
      require_stack
      ctr_re='^teranode([0-9]+)(-[a-z0-9]+)?-multinode$'
      ctr_fmt='teranode%s-multinode'
      blaster_data_dir="$BLASTER_DATA_DIR"
      ;;
    bsv-regtest)
      ctr_re='^bsv-regtest-teranode([0-9]+)$'
      ctr_fmt='bsv-regtest-teranode%s'
      blaster_data_dir="$REPO_ROOT/data/bsv-regtest-blaster"
      ;;
    *)
      echo "error: --target must be multinode or bsv-regtest, got '$target'" >&2
      exit 2
      ;;
  esac

  # On both fleets the blaster funds itself from PK2, the coinbase's second
  # output: chaos-test scenarios spend the first (PK1), and sharing a key
  # would make the two double-spend each other's coins. A blaster state
  # directory funded from PK1 by an older version needs --fresh.
  local pk2="${PK2:-}"
  if [[ -z "$pk2" ]]; then
    pk2=$(sed -nE 's/^PK2[[:space:]]*=[[:space:]]*([^[:space:]]+).*/\1/p' "$REPO_ROOT/settings.conf" | head -1)
  fi
  if [[ -z "$pk2" ]]; then
    echo "error: PK2 not found in settings.conf; set PK2 in the environment" >&2
    exit 1
  fi
  export coinbase_wallet_private_key="$pk2"

  if $fresh; then
    rm -rf "$blaster_data_dir"
    clean_blaster_state "$blaster_data_dir"
    echo "wiped blaster state: $blaster_data_dir"
  fi

  if $do_build; then
    if [[ -n "${BLASTER_BIN:-}" ]]; then
      echo "warning: --build ignored because BLASTER_BIN is set" >&2
    else
      echo "building blaster..."
      (cd "$REPO_ROOT/../teranode-coinbase" && make build-blaster) || {
        echo "error: blaster build failed" >&2
        exit 1
      }
    fi
  fi

  local blaster_bin=""
  if [[ -n "${BLASTER_BIN:-}" ]]; then
    blaster_bin="$BLASTER_BIN"
  else
    # The Makefile builds './blaster-tui.run', but users often `go build -o blaster`
    # manually. Pick whichever exists and is newest so a fresh rebuild wins.
    local candidates=(
      "$REPO_ROOT/../teranode-coinbase/blaster"
      "$REPO_ROOT/../teranode-coinbase/blaster-tui.run"
    )
    for c in "${candidates[@]}"; do
      if [[ -x "$c" ]] && { [[ -z "$blaster_bin" ]] || [[ "$c" -nt "$blaster_bin" ]]; }; then
        blaster_bin="$c"
      fi
    done
  fi
  if [[ -z "$blaster_bin" || ! -x "$blaster_bin" ]]; then
    echo "error: blaster binary not found" >&2
    echo "build it: '$0 blast --build' or (cd $REPO_ROOT/../teranode-coinbase && make build-blaster)" >&2
    echo "or set BLASTER_BIN to the binary path" >&2
    exit 1
  fi

  # Pre-flight: make sure the binary isn't a stale build that predates the
  # multinode-compatible CLI flags. Use || true so Go's -h exit(2) doesn't
  # poison the pipeline exit code under set -o pipefail.
  if ! { "$blaster_bin" -h 2>&1 || true; } | grep -q -- '-propagation-addr'; then
    echo "error: $blaster_bin looks stale (no -propagation-addr flag)." >&2
    echo "rebuild it: '$0 blast --build'" >&2
    exit 1
  fi

  if [[ ${#nodes[@]} -eq 0 ]]; then
    # Enumerate distinct node indices, ignoring per-service container suffixes
    # in split mode (teranodeN-propagation, teranodeN-validator, ...).
    declare -A seen_nodes=()
    while read -r name; do
      [[ -z "$name" ]] && continue
      if [[ "$name" =~ $ctr_re ]]; then
        local idx="${BASH_REMATCH[1]}"
        if [[ -z "${seen_nodes[$idx]:-}" ]]; then
          seen_nodes[$idx]=1
          nodes+=("$idx")
        fi
      fi
    done < <(docker ps --filter "name=teranode" --format '{{.Names}}' | sort -V)
  fi

  if [[ ${#nodes[@]} -eq 0 ]]; then
    if [[ "$target" == "bsv-regtest" ]]; then
      echo "error: no running bsv-regtest teranodes. Start chaos-test's fleet (make up && make wait in CHAOS_TEST_DIR) first." >&2
    else
      echo "error: no running teranode containers. Start the stack with '$0 up <N>' first." >&2
    fi
    exit 1
  fi

  # Verify propagation port is exposed (older stacks generated before 8084 was added won't have it).
  local first="${nodes[0]}"
  local first_prop=$((20000 + (first - 1) * 2000 + 84))
  local prop_ctr
  # shellcheck disable=SC2059 # ctr_fmt is a fixed format chosen above
  prop_ctr=$(printf "$ctr_fmt" "$first")
  if [[ "$target" == "multinode" ]] && is_split_mode "$first"; then
    prop_ctr="teranode${first}-propagation-multinode"
  fi
  if ! docker ps --filter "name=${prop_ctr}" --format '{{.Ports}}' | grep -q ":${first_prop}->8084"; then
    echo "error: propagation port not exposed on ${prop_ctr} (expected host port ${first_prop})." >&2
    if [[ "$target" == "bsv-regtest" ]]; then
      echo "chaos-test's fleet does not publish it by default. Apply our override and restart it:" >&2
      echo "  cp $SCRIPT_DIR/chaostest/bsv-regtest.override.yaml \$CHAOS_TEST_DIR/bsv-regtest/compose.override.yaml" >&2
      echo "  make -C \$CHAOS_TEST_DIR/bsv-regtest up" >&2
    else
      echo "Regenerate the stack: '$0 down && $0 up <N>'" >&2
    fi
    exit 1
  fi

  local prop_addrs=""
  for n in "${nodes[@]}"; do
    local base=$((20000 + (n - 1) * 2000))
    local prop=$((base + 84))
    [[ -n "$prop_addrs" ]] && prop_addrs+=","
    prop_addrs+="localhost:${prop}"
  done
  local rpc_port=$((20000 + (first - 1) * 2000 + 1292))
  local rpc_url_default="http://localhost:${rpc_port}"

  # Wait for each target node's RPC to answer before launching the blaster.
  # Without this, gRPC dials to propagation can race container startup and
  # end up in a stuck state where the blaster thinks it's connected but
  # broadcasts silently fail. 'up -d' only waits for containers to start,
  # not for teranode services inside them to be ready.
  local ready_timeout="${BLAST_READY_TIMEOUT:-60}"
  for n in "${nodes[@]}"; do
    local node_rpc=$((20000 + (n - 1) * 2000 + 1292))
    local waited=0
    printf "waiting for teranode%s RPC (localhost:%d)..." "$n" "$node_rpc"
    until curl -sf --max-time 2 -u bitcoin:bitcoin \
          -H 'Content-Type: application/json' \
          -d '{"method":"getinfo","params":[]}' \
          "http://localhost:${node_rpc}" >/dev/null 2>&1; do
      if [[ "$waited" -ge "$ready_timeout" ]]; then
        printf " TIMEOUT\n"
        echo "error: teranode${n} RPC did not become ready within ${ready_timeout}s" >&2
        echo "       check 'docker compose logs teranode${n}' or raise BLAST_READY_TIMEOUT" >&2
        exit 1
      fi
      sleep 2
      waited=$((waited + 2))
    done
    printf " ok\n"
  done

  if $auto_mine_enabled && [[ -z "$auto_mine_node" ]]; then
    auto_mine_node="$first"
  fi

  if $auto_mine_enabled; then
    # Ensure the target node has at least one running container (split mode:
    # any of the 9 service containers; all-in-one: the single teranodeN
    # container). Without this the background mining loop silently does
    # nothing and the user is left wondering why funding stalls.
    local mine_re="^teranode${auto_mine_node}(-[a-z0-9]+)?-multinode$"
    [[ "$target" == "bsv-regtest" ]] && mine_re="^bsv-regtest-teranode${auto_mine_node}$"
    if ! docker ps --filter "status=running" --format '{{.Names}}' | grep -qE "$mine_re"; then
      echo "error: auto-mine target teranode${auto_mine_node} is not running" >&2
      exit 1
    fi
    if [[ "$auto_mine_node" != "$first" ]]; then
      echo "warning: auto-mine is on teranode${auto_mine_node} but funding RPC is on teranode${first}."
      echo "         Split txs must propagate ${auto_mine_node}->${first} before funding flows."
      echo "         Prefer matching them (e.g. omit --auto-mine=N to default to the first target)."
    fi
  fi

  # Control where the blaster writes snapshot + embedded coinbase DB so that
  # 'multinode.sh down' (or --fresh) can clean it up alongside the chain state.
  # Only inject our path if the user didn't pass one via '--'.
  local user_snapshot=false
  for arg in ${passthrough[@]+"${passthrough[@]}"}; do
    case "$arg" in
      --snapshot-path|--snapshot-path=*|-snapshot-path|-snapshot-path=*)
        user_snapshot=true
        break
        ;;
    esac
  done
  local snapshot_path="$blaster_data_dir/utxos.json"
  if ! $user_snapshot; then
    mkdir -p "$blaster_data_dir"
    passthrough=(--snapshot-path "$snapshot_path" ${passthrough[@]+"${passthrough[@]}"})
  fi

  echo "blaster:     $blaster_bin"
  echo "target:      $target"
  echo "nodes:       ${nodes[*]}"
  echo "propagation: $prop_addrs"
  echo "rpc:         $rpc_url_default  (funding source for embedded coinbase)"
  if ! $user_snapshot; then
    if [[ "$target" == "bsv-regtest" ]]; then
      echo "snapshot:    $snapshot_path  (pass --fresh after chaos-test's 'make reset')"
      echo "funding:     PK2 (coinbase output 1; chaos-test scenarios spend output 0)"
    else
      echo "snapshot:    $snapshot_path  (wiped by '$0 down')"
    fi
    echo "logs:        $blaster_data_dir/blaster.log  (blaster writes service logs here in TUI mode)"
  fi
  if $auto_mine_enabled; then
    echo "auto-mine:   node $auto_mine_node, every ${auto_mine_interval}s"
  fi
  echo ""

  local miner_pid=""
  if $auto_mine_enabled; then
    local mine_rpc="http://localhost:$((20000 + (auto_mine_node - 1) * 2000 + 1292))"
    (
      while true; do
        if [[ "$target" == "bsv-regtest" ]]; then
          # No generated helper script for chaos-test's fleet; mine over RPC.
          curl -sf --max-time 30 -u bitcoin:bitcoin -H 'Content-Type: application/json' \
            -d '{"method":"generate","params":[1]}' "$mine_rpc" >/dev/null 2>&1 || true
        else
          "$GEN_DIR/generate-blocks.sh" "${auto_mine_node},1" >/dev/null 2>&1 || true
        fi
        sleep "$auto_mine_interval"
      done
    ) &
    miner_pid=$!
    trap 'if [[ -n "'"$miner_pid"'" ]]; then kill '"$miner_pid"' 2>/dev/null || true; wait '"$miner_pid"' 2>/dev/null || true; fi' EXIT INT TERM
  fi

  # rpc_url / rpc_user / rpc_pass (gocore keys, resolved from the environment
  # before any settings file) point the embedded coinbase's BLOCK MONITOR at
  # the node. They are NOT the same thing as the --rpc-url flag below: that
  # one is used for broadcasting funding transactions, while the monitor
  # reads gocore's "rpc_url", whose default is http://localhost:8332.
  #
  # Without these the monitor silently talks to nothing, so the coinbase never
  # learns which coinbase outputs it owns, its DB stays empty, and every
  # funding attempt fails with "sql: no rows in result set" while the blaster
  # reports 100% fund errors and sends zero transactions.
  #
  # Note the monitor only records blocks mined WHILE IT IS RUNNING, and a
  # coinbase needs 100 confirmations before it is spendable. So a chain mined
  # entirely before the blaster starts yields no funds: mine at least ~100
  # blocks with the blaster up (e.g. --auto-mine) before expecting throughput.
  export rpc_url="${rpc_url:-$rpc_url_default}"
  export rpc_user="${rpc_user:-bitcoin}"
  export rpc_pass="${rpc_pass:-bitcoin}"
  # Newer blaster builds read coinbase_rpc_* instead; set both so either works.
  export coinbase_rpc_url="${coinbase_rpc_url:-$rpc_url}"
  export coinbase_rpc_user="${coinbase_rpc_user:-$rpc_user}"
  export coinbase_rpc_pass="${coinbase_rpc_pass:-$rpc_pass}"
  echo "monitor rpc: $rpc_url  (embedded coinbase block monitor)"
  echo ""

  # Pin the blaster's dataFolder (a gocore key; the environment wins over
  # settings.conf) so the coinbase DB lands where clean_blaster_state looks
  # for it whatever directory this script was invoked from. The blaster keeps
  # the caller's working directory, so relative passthrough paths such as
  # --mix-schedule still resolve as typed.
  if ! $user_snapshot; then
    export dataFolder="$REPO_ROOT/data"
  fi
  "$blaster_bin" \
    --propagation-addr "$prop_addrs" \
    --rpc-url "$rpc_url_default" \
    ${passthrough[@]+"${passthrough[@]}"}
}

# ---- chaos-test simulator --------------------------------------------------
#
# Runs galt-tr/chaos-test's orchestrator (scenario engine + UI) against this
# stack. gennodes writes generated/chaostest/inventory.json describing the
# nodes; sim_up adds the private bits at start time (minerWIF = PK1, and
# chaos-test's own dev keys.json) into data/multinode-chaostest/config/, which
# is gitignored, so no key is ever written into the generated tree.
#
# The orchestrator joins the stack's compose network, so it reaches nodes by
# DNS name and consumes the Kafka verdict topics (rejectedtx-*, invalid-blocks-*)
# that scenario 'event' checks match on.

SIM_CONTAINER="chaos-orchestrator-multinode"
SIM_IMAGE="${CHAOS_TEST_IMAGE:-chaos-orchestrator:multinode}"
SIM_PORT="${SIM_HOST_PORT:-8600}"
SIM_DATA_DIR="$REPO_ROOT/data/multinode-chaostest"
SIM_SCENARIOS_DIR="${SCENARIOS_DIR:-$SCRIPT_DIR/chaostest/scenarios}"

# Arcade (ARC-compatible broadcaster) runs next to the orchestrator so the
# scenarios that submit through it (freeze-skew-rpc, arcade-*) work here as on
# chaos-test's own fleet. Same image and settings as bsv-regtest's 'arcade'
# profile, pointed at this stack's datahubs and p2p peers. No merkle-service:
# nothing that runs here needs merkle callbacks. State lives in the container
# and goes with it, since every 'up' starts a new chain.
ARCADE_CONTAINER="arcade-multinode"
ARCADE_IMAGE="${ARCADE_IMAGE:-ghcr.io/bsv-blockchain/arcade:latest}"
ARCADE_PORT="${ARCADE_HOST_PORT:-18080}"

chaos_test_dir() {
  local dir="${CHAOS_TEST_DIR:-$REPO_ROOT/../chaos-test}"
  if [[ ! -f "$dir/sim/Dockerfile" ]]; then
    echo "error: chaos-test checkout not found at $dir" >&2
    echo "       git clone https://github.com/galt-tr/chaos-test \"$dir\"" >&2
    echo "       or set CHAOS_TEST_DIR to an existing checkout" >&2
    exit 1
  fi
  (cd "$dir" && pwd)
}

sim_running() {
  [[ "$(docker inspect -f '{{.State.Running}}' "$SIM_CONTAINER" 2>/dev/null)" == "true" ]]
}

sim_build() {
  local dir
  dir=$(chaos_test_dir)
  echo "building $SIM_IMAGE from $dir ($(git -C "$dir" rev-parse --short HEAD 2>/dev/null || echo 'unknown ref'))..."
  docker build -t "$SIM_IMAGE" --build-arg UI="${SIM_UI:-1}" -f "$dir/sim/Dockerfile" "$dir"
}

# arcade_up <inventory> <network> writes arcade's config from the node list
# and starts it on the stack network. Host ports follow bsv-regtest's:
# API 18080, health 18081, events (SSE) 18082, chaintracks 18083.
arcade_up() {
  local inv="$1" network="$2"
  local cfg="$SIM_DATA_DIR/arcade"
  mkdir -p "$cfg"
  python3 -I - "$inv" "$cfg/config.yaml" "$ARCADE_CONTAINER" <<'PY'
import json, secrets, sys
inv, out, name = json.load(open(sys.argv[1])), sys.argv[2], sys.argv[3]
nodes = [n for n in inv["nodes"] if n["kind"] == "teranode"]
lines = [
    "# Written by multinode.sh sim up; mirrors bsv-regtest's arcade profile.",
    "mode: all", "log_level: info", "network: regtest", "storage_path: /data/arcade",
    f"callback_url: http://{name}:8080/api/v1/merkle-service/callback",
    f"callback_token: {secrets.token_hex(32)}",
    "api:", "  host: 0.0.0.0", "  port: 8080",
    "health:", "  port: 8081",
    "kafka:", "  backend: memory", "  consumer_group: arcade", "  max_retries: 5", "  buffer_size: 10000",
    "store:", "  backend: pebble", "  pebble:", "    path: /data/pebble", "    sync_writes: false",
    "datahub_urls:", *[f"  - {n['assetURL']}" for n in nodes],
    "teranode:", '  auth_token: ""',
    "p2p:", "  seeds: []", "  datahub_discovery: false", '  dht_mode: "off"', "  listen_port: 9905",
    "  storage_path: /data/p2p", "  allow_private_urls: true",
    "  bootstrap_peers:", *[f"    - {n['p2pAddr']}" for n in nodes],
    "chaintracks_server:", "  enabled: true", "  host: 0.0.0.0", "  port: 8083", "  tie_scan_min_interval_ms: 500",
    "callback:", "  allow_private_ips: true",
    "validator:", "  accept_zero_fee: true", "  min_fee_per_kb: 0", "  standard_format_supported: true",
    "bump_builder:", "  grace_window_ms: 5000", "  reconciler:", "    interval_ms: 2000", "    max_defer_attempts: 2",
    "chaintracks:", "  mode: embedded", "  storage_path: /data/chaintracks",
    f"  bootstrap_url: {nodes[0]['assetURL']}", "  bootstrap_mode: api",
]
open(out, "w").write("\n".join(lines) + "\n")
PY

  docker rm -f "$ARCADE_CONTAINER" >/dev/null 2>&1 || true
  echo "starting $ARCADE_CONTAINER on $network..."
  docker run -d --name "$ARCADE_CONTAINER" \
    --network "$network" \
    -p "$ARCADE_PORT:8080" -p "$((ARCADE_PORT + 1)):8081" \
    -p "$((ARCADE_PORT + 2)):8082" -p "$((ARCADE_PORT + 3)):8083" \
    -v "$cfg/config.yaml:/etc/arcade/config.yaml:ro" \
    --memory 1g \
    "$ARCADE_IMAGE" --mode all --config /etc/arcade/config.yaml >/dev/null

  local deadline=$((SECONDS + 60))
  until curl -sf -m 2 "http://localhost:$((ARCADE_PORT + 1))/health" >/dev/null 2>&1; do
    if [[ "$(docker inspect -f '{{.State.Running}}' "$ARCADE_CONTAINER" 2>/dev/null)" != "true" ]] || ((SECONDS > deadline)); then
      echo "error: arcade did not come up; last log lines:" >&2
      docker logs --tail 30 "$ARCADE_CONTAINER" >&2 || true
      exit 1
    fi
    sleep 1
  done
}

sim_up() {
  local build=false arcade=true
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --build) build=true; shift ;;
      --no-arcade) arcade=false; shift ;;
      *) echo "error: sim up: unknown flag '$1'" >&2; exit 2 ;;
    esac
  done
  require_stack

  local inv="$GEN_DIR/chaostest/inventory.json"
  if [[ ! -f "$inv" ]]; then
    echo "error: $inv not found; this stack predates chaos-test support. Run '$0 down' and '$0 up <N>' again." >&2
    exit 1
  fi

  local dir
  dir=$(chaos_test_dir)
  if $build || ! docker image inspect "$SIM_IMAGE" >/dev/null 2>&1; then
    sim_build
  fi

  # The coinbase pays PK1 (miner_wallet_private_keys.docker, output 0), so
  # chaos-test's 'miner' key must be PK1 for coinbase spends to sign.
  local pk1="${PK1:-}"
  if [[ -z "$pk1" ]]; then
    pk1=$(sed -nE 's/^PK1[[:space:]]*=[[:space:]]*([^[:space:]]+).*/\1/p' "$REPO_ROOT/settings.conf" | head -1)
  fi
  if [[ -z "$pk1" ]]; then
    echo "error: could not read PK1 from settings.conf; set PK1 in the environment" >&2
    exit 1
  fi

  local cfg="$SIM_DATA_DIR/config"
  mkdir -p "$cfg" "$SIM_DATA_DIR/data"
  # The key is passed through the environment, not argv, so it never shows in ps.
  MINER_WIF="$pk1" python3 -I -c '
import json, os, sys
inv = json.load(open(sys.argv[1]))
inv["minerWIF"] = os.environ["MINER_WIF"]
with open(sys.argv[2], "w") as f:
    json.dump(inv, f, indent=2)
' "$inv" "$cfg/inventory.json"
  cp "$dir/bsv-regtest/config/keys.json" "$cfg/keys.json"
  chmod 600 "$cfg/inventory.json" "$cfg/keys.json"

  local network
  network=$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))["networks"]["chaosnetName"])' "$inv")

  if $arcade; then
    arcade_up "$inv" "$network"
    python3 -I - "$cfg/inventory.json" "$ARCADE_CONTAINER" "$ARCADE_PORT" <<'PY'
import json, sys
path, name, port = sys.argv[1], sys.argv[2], int(sys.argv[3])
inv = json.load(open(path))
pair = lambda p: {"url": f"http://{name}:{p}", "hostURL": f"http://localhost:{port + p - 8080}"}
inv["services"]["arcade"] = {
    **pair(8080), "container": name,
    "urls": {"health": pair(8081), "events": pair(8082), "chaintracks": pair(8083)},
}
with open(path, "w") as f:
    json.dump(inv, f, indent=2)
PY
  else
    docker rm -f "$ARCADE_CONTAINER" >/dev/null 2>&1 || true
  fi

  docker rm -f "$SIM_CONTAINER" >/dev/null 2>&1 || true
  echo "starting $SIM_CONTAINER on $network..."
  # -walletd= turns the wallet feature off; the env var cannot, because an
  # empty WALLETD_URL falls back to bsv-regtest's walletd address.
  # host.docker.internal lets scenarios reach host-side tools (the blaster).
  docker run -d --name "$SIM_CONTAINER" \
    --network "$network" \
    --add-host host.docker.internal:host-gateway \
    -p "$SIM_PORT:8600" \
    -e KAFKA_BROKERS=kafka-shared:9092 \
    -e SCENARIOS=/scenarios,/scenarios-multinode \
    -v "$cfg:/config:ro" \
    -v "$SIM_SCENARIOS_DIR:/scenarios-multinode:ro" \
    -v "$SIM_DATA_DIR/data:/data" \
    -v "${CONTAINER_SOCKET:-/var/run/docker.sock}:/var/run/docker.sock" \
    "$SIM_IMAGE" -walletd= >/dev/null

  local deadline=$((SECONDS + 30))
  until curl -sf -m 2 "http://localhost:$SIM_PORT/api/inventory" >/dev/null 2>&1; do
    if ! sim_running || ((SECONDS > deadline)); then
      echo "error: orchestrator did not come up; last log lines:" >&2
      docker logs --tail 30 "$SIM_CONTAINER" >&2 || true
      exit 1
    fi
    sleep 1
  done
  echo "chaos-test UI: http://localhost:$SIM_PORT"
  echo "run a scenario: $0 sim run multinode-smoke"
}

sim_down() {
  if docker inspect "$SIM_CONTAINER" >/dev/null 2>&1; then
    docker rm -f "$SIM_CONTAINER" >/dev/null
    echo "removed $SIM_CONTAINER (runs kept in ${SIM_DATA_DIR#"$REPO_ROOT"/}/data/runs)"
  fi
  if docker inspect "$ARCADE_CONTAINER" >/dev/null 2>&1; then
    docker rm -f "$ARCADE_CONTAINER" >/dev/null
    echo "removed $ARCADE_CONTAINER"
  fi
}

sim_status() {
  if ! sim_running; then
    echo "chaos-test orchestrator: not running ('$0 sim up' to start)"
    return
  fi
  echo "chaos-test orchestrator: running  ui=http://localhost:$SIM_PORT"
  curl -sf -m 5 "http://localhost:$SIM_PORT/api/scenarios" | python3 -I -c '
import json, sys
for s in json.load(sys.stdin):
    print("  " + s["id"])
' || echo "  (scenario list unavailable)"
}

# sim_run starts a scenario and follows it to completion, printing each step
# as it finishes. Exit status: 0 passed (findings are printed, not fatal),
# 1 failed or aborted.
sim_run() {
  local id="${1:?usage: sim run <scenario-id> [--step]}"
  local mode="auto"
  [[ "${2:-}" == "--step" ]] && mode="step"
  sim_running || { echo "error: orchestrator not running; '$0 sim up' first" >&2; exit 1; }

  local base="http://localhost:$SIM_PORT/api"
  local run_id
  run_id=$(curl -sf -m 10 -X POST -H 'Content-Type: application/json' \
    -d "{\"mode\":\"$mode\"}" "$base/scenarios/$id/run" \
    | python3 -I -c 'import json, sys; print(json.load(sys.stdin)["id"])') || {
    echo "error: could not start '$id' (unknown scenario, or another run is active)" >&2
    exit 1
  }
  echo "run $run_id: $id ($mode)  ui=http://localhost:$SIM_PORT"
  [[ "$mode" == "step" ]] && echo "step mode: release each step from the UI or POST $base/runs/$run_id/next"

  local printed=0 status
  while true; do
    status=$(curl -sf -m 10 "$base/runs/$run_id" | python3 -I -c '
import json, sys
run = json.load(sys.stdin)
done = [s for s in run.get("steps") or [] if s.get("status") not in (None, "", "pending", "running")]
skip = int(sys.argv[1])
for s in done[skip:]:
    err = (" - " + s["error"]) if s.get("error") else ""
    print("STEP %-7s %s%s" % (s.get("status"), s.get("name"), err))
print("COUNT %d" % len(done))
print("STATUS %s" % run.get("status"))
for f in run.get("findings") or []:
    print("FINDING %s" % (f if isinstance(f, str) else json.dumps(f)))
' "$printed") || { sleep 3; continue; }
    while IFS= read -r line; do
      case "$line" in
        STEP*) echo "  ${line#STEP }" ;;
        COUNT*) printed="${line#COUNT }" ;;
      esac
    done <<<"$status"
    local st
    st=$(sed -n 's/^STATUS //p' <<<"$status")
    if [[ "$st" != "running" && "$st" != "paused" ]]; then
      grep '^FINDING' <<<"$status" | sed 's/^FINDING /  finding: /' || true
      echo "run $run_id: $st"
      [[ "$st" == "passed" ]]
      return
    fi
    sleep 3
  done
}

cmd_sim() {
  local sub="${1:-}"
  [[ $# -gt 0 ]] && shift
  case "$sub" in
    up)     sim_up "$@" ;;
    down)   sim_down ;;
    build)  sim_build ;;
    status) sim_status ;;
    logs)   docker logs -f "$SIM_CONTAINER" ;;
    run)    sim_run "$@" ;;
    *)      echo "usage: $0 sim up [--build] [--no-arcade] | down | build | status | logs | run <scenario> [--step]" >&2; exit 2 ;;
  esac
}

[[ $# -eq 0 ]] && usage

command="$1"
shift

case "$command" in
  up)         cmd_up "$@" ;;
  build)      cmd_build ;;
  down)       cmd_down ;;
  restart)    cmd_restart ;;
  status)     cmd_status ;;
  logs)       cmd_logs "$@" ;;
  dashboards) cmd_dashboards ;;
  generate)   cmd_generate "$@" ;;
  blast)      cmd_blast "$@" ;;
  chaos)      cmd_chaos "$@" ;;
  sim)        cmd_sim "$@" ;;
  help|-h)    usage ;;
  *)          echo "error: unknown command '$command'" >&2; usage ;;
esac

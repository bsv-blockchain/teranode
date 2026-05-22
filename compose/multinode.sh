#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GEN_DIR="$SCRIPT_DIR/generated"
COMPOSE_FILE="$GEN_DIR/docker-compose-multinode.yml"

# Chaos primitives run iptables/tc inside the target container's netns via a
# privileged sidecar. Works on Linux and macOS (no host nsenter/sudo needed).
NETSHOOT_IMAGE="${NETSHOOT_IMAGE:-nicolaka/netshoot:v0.13}"

usage() {
  cat <<'EOF'
Usage: compose/multinode.sh <command> [args]

Commands:
  up <N> [--build]          Generate and start an N-node network (3-10)
  build                    Build the teranode Docker image
  down                     Stop and remove all containers and volumes
  restart                  Restart all containers (picks up config changes)
  status                   Show container status
  logs [node]              Tail logs (all nodes, or a specific node number)
  dashboards               Open all dashboards in the browser
  generate <n,count> ...   Generate blocks on specific nodes
                           e.g. generate 1,10 3,5
  blast [nodes] [--build] [--auto-mine[=N]] [-- args]
                           Run the coinbase blaster against the stack.
                           'nodes' is a comma/space-separated list (default: all
                           running). --build rebuilds the blaster first.
                           --auto-mine spawns a background miner on node N
                           (default: first target) every 5s; override with
                           BLAST_AUTO_MINE_INTERVAL=<seconds>. Flags after '--'
                           are passed to blaster.

Chaos:
  chaos isolate <node>     Block peer traffic (RPC still works)
  chaos heal [node]        Restore peer traffic (or all nodes if omitted)
  chaos kill <node>        Stop a node container
  chaos start <node>       Start a stopped node container
  chaos pause <node>       Freeze a node (simulates hang/GC pause)
  chaos unpause <node>     Unfreeze a paused node
  chaos slow <node> <ms>   Add network latency to a node
  chaos unslow <node>      Remove added latency from a node

Examples:
  compose/multinode.sh up 5
  compose/multinode.sh generate 1,10 3,5
  compose/multinode.sh blast              # blast all running nodes (TUI)
  compose/multinode.sh blast --build --auto-mine  # rebuild, mine on node 1
  compose/multinode.sh blast 1,3 -- --headless --max-tps 50
  compose/multinode.sh chaos isolate 3
  compose/multinode.sh chaos heal
  compose/multinode.sh chaos slow 2 500
  compose/multinode.sh logs 2
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
  for arg in "$@"; do
    case "$arg" in
      --build) do_build=true ;;
      *)       n="$arg" ;;
    esac
  done
  if [[ -z "$n" ]]; then
    echo "error: specify number of nodes, e.g. '$0 up 5'" >&2
    exit 2
  fi
  echo "generating $n-node stack..."
  (cd "$REPO_ROOT" && go run ./compose/cmd/gennodes -n "$n" -o compose/generated)
  if [[ "$do_build" == true ]]; then
    cmd_build
  fi
  echo "starting containers..."
  compose up -d
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
  local blaster_data_dir="$REPO_ROOT/data/multinode-blaster"
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

  # Collect infra and node info from JSON lines
  local infra_lines=""
  local -a node_indices=()
  local -A node_states=()
  local -A node_statuses=()

  while IFS= read -r line; do
    local service state status
    service=$(echo "$line" | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['Service'])" 2>/dev/null) || continue
    state=$(echo "$line" | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['State'])" 2>/dev/null)
    status=$(echo "$line" | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['Status'])" 2>/dev/null)

    case "$service" in
      teranode*)
        local idx="${service#teranode}"
        node_indices+=("$idx")
        node_states[$idx]="$state"
        node_statuses[$idx]="$status"
        ;;
      *)
        local state_icon="x"
        [[ "$state" == "running" ]] && state_icon="+"
        infra_lines+=$(printf "\n  [%s] %-22s %s" "$state_icon" "$service" "$status")
        ;;
    esac
  done <<< "$json"

  echo "Infrastructure:$infra_lines"
  echo ""

  # Build node lines outside the herestring loop so curl and docker sidecar spawns work
  local node_lines=""
  local node_count=${#node_indices[@]}
  local nodes_ok=0

  for idx in "${node_indices[@]}"; do
    local state="${node_states[$idx]}"
    local status="${node_statuses[$idx]}"
    local base=$((20000 + (idx - 1) * 2000))
    local dashboard=$((base + 90))
    local rpc=$((base + 1292))
    local health=$((base))
    local state_icon="x"
    local chaos_tag=""
    local height_tag=""

    if [[ "$state" == "running" ]]; then
      state_icon="+"
      nodes_ok=$((nodes_ok + 1))
      local ctr
      ctr=$(container_name "$idx")
      # One sidecar per node: emit DROP count on line 1, qdisc info on line 2.
      local chaos_info drop_rules qdisc_line delay
      chaos_info=$(netns_sh "$ctr" '
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
      height=$(docker exec "$ctr" wget -qO- --timeout=2 \
        --user=bitcoin --password=bitcoin \
        --header='Content-Type: application/json' \
        --post-data='{"method":"getinfo","params":[]}' \
        "http://localhost:9292" 2>/dev/null \
        | grep -o '"blocks":[0-9]*' | cut -d: -f2 || true)
      if [[ -n "$height" ]]; then
        height_tag="  height=$height"
      fi
    elif [[ "$state" == "paused" ]]; then
      chaos_tag=" PAUSED"
    fi

    node_lines+=$(printf "\n  [%s] teranode%-3s %-24s dashboard=localhost:%d  rpc=localhost:%d  health=localhost:%d%s%s" \
      "$state_icon" "$idx" "$status" "$dashboard" "$rpc" "$health" "$height_tag" "$chaos_tag")
  done

  echo "Nodes ($nodes_ok/$node_count running):$node_lines"
}

cmd_logs() {
  require_stack
  local node="${1:-}"
  if [[ -n "$node" ]]; then
    compose logs -f "teranode${node}"
  else
    compose logs -f
  fi
}

cmd_dashboards() {
  require_stack
  "$GEN_DIR/open-dashboards.sh"
}

container_name() {
  echo "teranode${1}-multinode"
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

chaos_isolate() {
  local node="${1:?usage: chaos isolate <node>}"
  local ctr
  ctr=$(container_name "$node")

  local script=""
  local blocked=0
  for other in $(docker ps --filter "name=-multinode" --format '{{.Names}}' | grep '^teranode[0-9]' | grep -v "^${ctr}$"); do
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
  if [[ -n "${1:-}" ]]; then
    local ctr
    ctr=$(container_name "$1")
    echo "restoring teranode$1 peer traffic..."
    netns_iptables "$ctr" -F 2>/dev/null || true
    echo "teranode$1 healed"
    return
  fi
  echo "restoring all nodes..."
  local healed=0
  for ctr in $(docker ps --filter "name=-multinode" --format '{{.Names}}' | grep '^teranode[0-9]'); do
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
  local node="${1:?usage: chaos kill <node>}"
  echo "stopping teranode$node..."
  compose stop "teranode${node}"
  echo "teranode$node is down"
}

chaos_start() {
  local node="${1:?usage: chaos start <node>}"
  echo "starting teranode$node..."
  compose start "teranode${node}"
  echo "teranode$node is up"
}

chaos_pause() {
  local node="${1:?usage: chaos pause <node>}"
  local ctr
  ctr=$(container_name "$node")
  echo "pausing $ctr (simulating freeze)..."
  docker pause "$ctr"
  echo "teranode$node is frozen"
}

chaos_unpause() {
  local node="${1:?usage: chaos unpause <node>}"
  local ctr
  ctr=$(container_name "$node")
  echo "unpausing $ctr..."
  docker unpause "$ctr"
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
  require_stack

  local -a nodes=()
  local -a passthrough=()
  local sawDashDash=false
  local auto_mine_enabled=false
  local auto_mine_node=""
  local auto_mine_interval="${BLAST_AUTO_MINE_INTERVAL:-5}"
  local do_build=false
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
      *)
        IFS=', ' read -ra items <<< "$1"
        for item in "${items[@]}"; do
          [[ -n "$item" ]] && nodes+=("$item")
        done
        shift
        ;;
    esac
  done

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
    while read -r name; do
      local idx="${name#teranode}"
      idx="${idx%-multinode}"
      nodes+=("$idx")
    done < <(docker ps --filter "name=-multinode" --format '{{.Names}}' | grep '^teranode[0-9]' | sort -V)
  fi

  if [[ ${#nodes[@]} -eq 0 ]]; then
    echo "error: no running teranode containers. Start the stack with '$0 up <N>' first." >&2
    exit 1
  fi

  # Verify propagation port is exposed (older stacks generated before 8084 was added won't have it).
  local first="${nodes[0]}"
  local first_prop=$((20000 + (first - 1) * 2000 + 84))
  if ! docker ps --filter "name=teranode${first}-multinode" --format '{{.Ports}}' | grep -q ":${first_prop}->8084"; then
    echo "error: propagation port not exposed on teranode${first} (expected host port ${first_prop})." >&2
    echo "Regenerate the stack: '$0 down && $0 up <N>'" >&2
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
  local rpc_url="http://localhost:${rpc_port}"

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
    # Ensure the target is actually running (otherwise the background loop
    # silently does nothing and the user is left wondering why funding stalls).
    if ! docker ps --filter "name=teranode${auto_mine_node}-multinode" --filter "status=running" --format '{{.Names}}' | grep -q '.'; then
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
  # 'multinode.sh down' can clean it up alongside the chain state. Only inject
  # our path if the user didn't pass one via '--'.
  local blaster_data_dir="$REPO_ROOT/data/multinode-blaster"
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
  echo "nodes:       ${nodes[*]}"
  echo "propagation: $prop_addrs"
  echo "rpc:         $rpc_url  (funding source for embedded coinbase)"
  if ! $user_snapshot; then
    echo "snapshot:    $snapshot_path  (wiped by '$0 down')"
    echo "logs:        $blaster_data_dir/blaster.log  (blaster writes service logs here in TUI mode)"
  fi
  if $auto_mine_enabled; then
    echo "auto-mine:   node $auto_mine_node, every ${auto_mine_interval}s"
  fi
  echo ""

  local miner_pid=""
  if $auto_mine_enabled; then
    (
      while true; do
        "$GEN_DIR/generate-blocks.sh" "${auto_mine_node},1" >/dev/null 2>&1 || true
        sleep "$auto_mine_interval"
      done
    ) &
    miner_pid=$!
    trap 'if [[ -n "'"$miner_pid"'" ]]; then kill '"$miner_pid"' 2>/dev/null || true; wait '"$miner_pid"' 2>/dev/null || true; fi' EXIT INT TERM
  fi

  "$blaster_bin" \
    --propagation-addr "$prop_addrs" \
    --rpc-url "$rpc_url" \
    ${passthrough[@]+"${passthrough[@]}"}
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
  help|-h)    usage ;;
  *)          echo "error: unknown command '$command'" >&2; usage ;;
esac

#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GEN_DIR="$SCRIPT_DIR/generated"
COMPOSE_FILE="$GEN_DIR/docker-compose-multinode.yml"

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
    [[ -x "$f" ]] && grep -oP 'http://localhost:\d+' "$f" | while read -r url; do
      echo "  $url"
    done
  done
  echo ""
  echo "run '$0 status' to check health"
}

cmd_down() {
  require_stack
  compose down -v --remove-orphans
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

  # Build node lines outside the herestring loop so curl/nsenter work
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
      local drop_rules
      drop_rules=$(nsenter_iptables "$ctr" -L INPUT --line-numbers 2>/dev/null | grep -c DROP || true)
      local has_netem
      has_netem=$(nsenter_tc "$ctr" qdisc show dev eth0 2>/dev/null | grep -c netem || true)
      if [[ "$drop_rules" -gt 0 ]]; then chaos_tag+=" ISOLATED"; fi
      if [[ "$has_netem" -gt 0 ]]; then
        local delay
        delay=$(nsenter_tc "$ctr" qdisc show dev eth0 2>/dev/null | grep -oP '\d+\.\d+ms|\d+ms' | head -1)
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

nsenter_iptables() {
  local ctr="$1"
  shift
  local pid
  pid=$(docker inspect --format '{{.State.Pid}}' "$ctr")
  sudo nsenter -t "$pid" -n iptables "$@"
}

chaos_isolate() {
  local node="${1:?usage: chaos isolate <node>}"
  local ctr
  ctr=$(container_name "$node")

  local blocked=0
  for other in $(docker ps --filter "name=-multinode" --format '{{.Names}}' | grep '^teranode[0-9]' | grep -v "^${ctr}$"); do
    local ip
    ip=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$other" 2>/dev/null)
    if [[ -n "$ip" ]]; then
      nsenter_iptables "$ctr" -A INPUT -s "$ip" -j DROP
      nsenter_iptables "$ctr" -A OUTPUT -d "$ip" -j DROP
      blocked=$((blocked + 1))
    fi
  done
  echo "teranode$node is isolated from $blocked peer(s) (RPC still accessible)"
}

chaos_heal() {
  if [[ -n "${1:-}" ]]; then
    local ctr
    ctr=$(container_name "$1")
    echo "restoring teranode$1 peer traffic..."
    nsenter_iptables "$ctr" -F 2>/dev/null || true
    echo "teranode$1 healed"
    return
  fi
  echo "restoring all nodes..."
  local healed=0
  for ctr in $(docker ps --filter "name=-multinode" --format '{{.Names}}' | grep '^teranode[0-9]'); do
    local count
    count=$(nsenter_iptables "$ctr" -L INPUT --line-numbers 2>/dev/null | grep -c DROP || true)
    if [[ "$count" -gt 0 ]]; then
      nsenter_iptables "$ctr" -F
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

nsenter_tc() {
  local ctr="$1"
  shift
  local pid
  pid=$(docker inspect --format '{{.State.Pid}}' "$ctr")
  sudo nsenter -t "$pid" -n tc "$@"
}

chaos_slow() {
  local node="${1:?usage: chaos slow <node> <ms>}"
  local ms="${2:?usage: chaos slow <node> <ms>}"
  local ctr
  ctr=$(container_name "$node")
  echo "adding ${ms}ms latency to teranode$node..."
  if ! nsenter_tc "$ctr" qdisc add dev eth0 root netem delay "${ms}ms" 2>/dev/null; then
    nsenter_tc "$ctr" qdisc change dev eth0 root netem delay "${ms}ms" 2>/dev/null \
      && echo "updated latency to ${ms}ms on teranode$node" \
      || { echo "error: failed to set latency (is iproute2 installed on host?)" >&2; return 1; }
  else
    echo "teranode$node now has ${ms}ms added latency"
  fi
}

chaos_unslow() {
  local node="${1:?usage: chaos unslow <node>}"
  local ctr
  ctr=$(container_name "$node")
  echo "removing latency from teranode$node..."
  nsenter_tc "$ctr" qdisc del dev eth0 root 2>/dev/null || true
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
  chaos)      cmd_chaos "$@" ;;
  help|-h)    usage ;;
  *)          echo "error: unknown command '$command'" >&2; usage ;;
esac

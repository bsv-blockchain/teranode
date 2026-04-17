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
  up <N>                   Generate and start an N-node network (3-10)
  down                     Stop and remove all containers and volumes
  restart                  Restart all containers (picks up config changes)
  status                   Show container status
  logs [node]              Tail logs (all nodes, or a specific node number)
  dashboards               Open all dashboards in the browser
  generate <n,count> ...   Generate blocks on specific nodes
                           e.g. generate 1,10 3,5

Chaos:
  chaos partition <node>   Disconnect a node from the network
  chaos heal [node]        Reconnect a node (or all nodes if omitted)
  chaos kill <node>        Stop a node container
  chaos start <node>       Start a stopped node container
  chaos pause <node>       Freeze a node (simulates hang/GC pause)
  chaos unpause <node>     Unfreeze a paused node
  chaos slow <node> <ms>   Add network latency to a node
  chaos unslow <node>      Remove added latency from a node

Examples:
  compose/multinode.sh up 5
  compose/multinode.sh generate 1,10 3,5
  compose/multinode.sh chaos partition 3
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

cmd_up() {
  local n="${1:-}"
  if [[ -z "$n" ]]; then
    echo "error: specify number of nodes, e.g. '$0 up 5'" >&2
    exit 2
  fi
  echo "generating $n-node stack..."
  (cd "$REPO_ROOT" && go run ./compose/cmd/gennodes -n "$n" -o compose/generated)
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
  compose down -v
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

  # Collect infra and node status from JSON lines
  local infra_lines=""
  local node_lines=""
  local node_count=0
  local nodes_ok=0

  while IFS= read -r line; do
    local service state status
    service=$(echo "$line" | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['Service'])" 2>/dev/null) || continue
    state=$(echo "$line" | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['State'])" 2>/dev/null)
    status=$(echo "$line" | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['Status'])" 2>/dev/null)

    case "$service" in
      teranode*)
        node_count=$((node_count + 1))
        local idx="${service#teranode}"
        local base=$((20000 + (idx - 1) * 2000))
        local dashboard=$((base + 90))
        local rpc=$((base + 1292))
        local health=$((base))

        local state_icon="x"
        if [[ "$state" == "running" ]]; then
          state_icon="+"
          nodes_ok=$((nodes_ok + 1))
        fi

        node_lines+=$(printf "\n  [%s] teranode%-3s %-24s dashboard=localhost:%d  rpc=localhost:%d  health=localhost:%d" \
          "$state_icon" "$idx" "$status" "$dashboard" "$rpc" "$health")
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

network_name() {
  docker compose -f "$COMPOSE_FILE" ps --format json 2>/dev/null \
    | head -1 \
    | python3 -c "
import sys, json
d = json.loads(sys.stdin.read())
for n in d.get('Networks', '').split(','):
    n = n.strip()
    if 'teranode-network' in n:
        print(n)
        break
" 2>/dev/null
}

cmd_chaos() {
  require_stack
  local action="${1:-}"
  shift 2>/dev/null || true

  case "$action" in
    partition) chaos_partition "$@" ;;
    heal)      chaos_heal "$@" ;;
    kill)      chaos_kill "$@" ;;
    start)     chaos_start "$@" ;;
    pause)     chaos_pause "$@" ;;
    unpause)   chaos_unpause "$@" ;;
    slow)      chaos_slow "$@" ;;
    unslow)    chaos_unslow "$@" ;;
    *)
      echo "error: unknown chaos action '$action'" >&2
      echo "actions: partition, heal, kill, start, pause, unpause, slow, unslow" >&2
      exit 2
      ;;
  esac
}

chaos_partition() {
  local node="${1:?usage: chaos partition <node>}"
  local net
  net=$(network_name)
  local ctr
  ctr=$(container_name "$node")
  echo "disconnecting $ctr from $net..."
  docker network disconnect "$net" "$ctr"
  echo "teranode$node is now isolated from all peers"
}

chaos_heal() {
  local net
  net=$(network_name)
  if [[ -n "${1:-}" ]]; then
    local ctr
    ctr=$(container_name "$1")
    echo "reconnecting $ctr to $net..."
    docker network connect "$net" "$ctr" 2>/dev/null && echo "teranode$1 reconnected" || echo "teranode$1 was already connected"
    return
  fi
  echo "reconnecting all nodes to $net..."
  local healed=0
  for ctr in $(docker ps -a --filter "name=-multinode" --format '{{.Names}}' | grep '^teranode[0-9]'); do
    if docker network connect "$net" "$ctr" 2>/dev/null; then
      echo "  reconnected $ctr"
      healed=$((healed + 1))
    fi
  done
  if [[ $healed -eq 0 ]]; then
    echo "  all nodes already connected"
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

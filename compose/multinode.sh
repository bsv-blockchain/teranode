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

Examples:
  compose/multinode.sh up 5
  compose/multinode.sh generate 1,10 3,5
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
  help|-h)    usage ;;
  *)          echo "error: unknown command '$command'" >&2; usage ;;
esac

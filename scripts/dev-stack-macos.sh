#!/usr/bin/env bash
set -euo pipefail

# Development Containers Script for macOS
# Manages Aerospike, Redpanda, and PostgreSQL containers for Teranode development
# Uses Apple Container runtime (macOS only)

# Check for macOS
if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "Error: This script is for macOS only" >&2
    echo "" >&2
    echo "For other platforms, use Docker Compose:" >&2
    echo "  - See deploy/docker/ for docker-compose configurations" >&2
    echo "" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Use DATADIR environment variable if set, otherwise default to ./data
DATADIR="${DATADIR:-${PROJECT_ROOT}/data}"
DATA_PATH=$(realpath "$DATADIR" 2>/dev/null || echo "$DATADIR")

# Container configuration
AEROSPIKE_CONTAINER="aerospike"
REDPANDA_CONTAINER="redpanda"
POSTGRES_CONTAINER="postgres"

CONTAINER_BINARY="container"

# Display usage
usage() {
    echo "Usage: $0 [up|down|restart|status|logs|follow]"
    echo ""
    echo "Commands:"
    echo "  up      - Start all containers"
    echo "  down    - Stop all containers"
    echo "  restart - Restart all containers"
    echo "  status  - Show container status"
    echo "  logs    - Show logs for all containers"
    echo "  follow  - Follow logs for all containers in real-time"
    echo ""
    echo "Options:"
    echo "  -h, --help    Show this help message"
    exit 1
}

# Check if container binary is installed and exit if not
ensure_container_system() {
    if ! command -v "$CONTAINER_BINARY" &>/dev/null; then
        echo "Error: '${CONTAINER_BINARY}' command not found" >&2
        echo "" >&2
        echo "Please install Apple Container first:" >&2
        echo "  - Download the latest signed installer package from:" >&2
        echo "    https://github.com/apple/container/releases" >&2
        echo "" >&2
        exit 1
    fi

    local version_output
    version_output=$($CONTAINER_BINARY --version 2>&1 || echo "")
    if ! echo "$version_output" | grep -q "container CLI version"; then
        echo "Error: '${CONTAINER_BINARY}' command found but does not appear to be Apple Container" >&2
        echo "" >&2
        echo "Expected 'container CLI version' but got: $version_output" >&2
        echo "" >&2
        echo "Please install Apple Container first:" >&2
        echo "  - Download the latest signed installer package from:" >&2
        echo "    https://github.com/apple/container/releases" >&2
        echo "" >&2
        exit 1
    fi

    echo "Starting container system..."
    if ! $CONTAINER_BINARY system start 2>/dev/null; then
        echo "⚠️  Container system may already be running or failed to start"
    else
        echo "✅ Container system started"
    fi

    sleep 2
}

# Initialize directories
init_directories() {
    mkdir -p "${DATA_PATH}/aerospike"
    mkdir -p "${DATA_PATH}/postgres"
    echo "📁 Data directories created: ${DATA_PATH}"
}

# Check if container is running
is_container_running() {
    local container_name=$1
    local inspect_output
    inspect_output=$($CONTAINER_BINARY inspect "$container_name" 2>/dev/null || echo "[]")
    if [ "$inspect_output" != "[]" ]; then
        local state
        state=$(echo "$inspect_output" | grep -o '"status":"[^"]*"' | cut -d'"' -f4 || echo "")
        [ "$state" = "running" ] && return 0
    fi
    return 1
}

# Start Aerospike container
start_aerospike() {
    if is_container_running "$AEROSPIKE_CONTAINER"; then
        echo "✅ Aerospike is already running"
        return 0
    fi

    # Remove existing container if it exists but is not running
    $CONTAINER_BINARY rm -f "$AEROSPIKE_CONTAINER" 2>/dev/null || true

    echo "Starting Aerospike..."
    if ! $CONTAINER_BINARY run -d \
        --name "$AEROSPIKE_CONTAINER" \
        -p 3000:3000 \
        -v "${SCRIPT_DIR}:/opt/aerospike/scripts" \
        -v "${DATA_PATH}/aerospike:/opt/aerospike/data" \
        aerospike/aerospike-server:8.1 \
        --config-file /opt/aerospike/scripts/aerospike.conf; then
        echo "❌ Failed to start Aerospike" >&2
        return 1
    fi

    sleep 2
    if is_container_running "$AEROSPIKE_CONTAINER"; then
        echo "✅ Aerospike started"
    else
        echo "❌ Aerospike failed to start" >&2
        return 1
    fi
}

# Start Redpanda container
start_redpanda() {
    if is_container_running "$REDPANDA_CONTAINER"; then
        echo "✅ Redpanda is already running"
        return 0
    fi

    # Remove existing container if it exists but is not running
    $CONTAINER_BINARY rm -f "$REDPANDA_CONTAINER" 2>/dev/null || true

    echo "Starting Redpanda..."
    if ! $CONTAINER_BINARY run -d \
        --name "$REDPANDA_CONTAINER" \
        -p 9092:9092 \
        redpandadata/redpanda:latest \
        redpanda start \
        --mode dev-container; then
        echo "❌ Failed to start Redpanda" >&2
        return 1
    fi

    sleep 2
    if is_container_running "$REDPANDA_CONTAINER"; then
        echo "✅ Redpanda started"
    else
        echo "❌ Redpanda failed to start" >&2
        return 1
    fi
}

# Start PostgreSQL container
start_postgres() {
    if is_container_running "$POSTGRES_CONTAINER"; then
        echo "✅ PostgreSQL is already running"
        return 0
    fi

    # Remove existing container if it exists but is not running
    $CONTAINER_BINARY rm -f "$POSTGRES_CONTAINER" 2>/dev/null || true

    # Clean up old volume if requested
    if [ "${CLEAN_POSTGRES:-false}" = "true" ]; then
        echo "Removing old PostgreSQL volume..."
        $CONTAINER_BINARY volume rm postgres-data 2>/dev/null || true
    fi

    echo "Starting PostgreSQL..."
    if ! $CONTAINER_BINARY run -d \
        --name "$POSTGRES_CONTAINER" \
        -p 5432:5432 \
        -e POSTGRES_USER=teranode \
        -e POSTGRES_PASSWORD=teranode \
        -e POSTGRES_DB=teranode \
        -e PGDATA=/var/lib/postgresql/data/pgdata \
        --volume postgres-data:/var/lib/postgresql/data \
        postgres:17; then
        echo "❌ Failed to start PostgreSQL" >&2
        return 1
    fi

    sleep 2
    if is_container_running "$POSTGRES_CONTAINER"; then
        echo "✅ PostgreSQL started"
    else
        echo "❌ PostgreSQL failed to start" >&2
        return 1
    fi
}

# Stop container
stop_container() {
    local container_name=$1
    local inspect_output
    inspect_output=$($CONTAINER_BINARY inspect "$container_name" 2>/dev/null || echo "[]")
    if [ "$inspect_output" = "[]" ]; then
        return 0
    fi

    echo "Stopping ${container_name}..."
    $CONTAINER_BINARY stop "$container_name" 2>/dev/null || true
    $CONTAINER_BINARY rm "$container_name" 2>/dev/null || true
}

# Start all containers
start_all() {
    echo "🚀 Starting all development containers..."
    ensure_container_system
    init_directories

    start_aerospike
    start_redpanda
    start_postgres

    echo ""
    echo "✅ All containers started successfully"
    echo ""
    echo "📋 Connection Information:"
    echo "  Aerospike:  localhost:3000"
    echo "  Redpanda:   localhost:9092"
    echo "  PostgreSQL: localhost:5432"
    echo "              User: teranode"
    echo "              Password: teranode"
    echo "              Database: teranode"
    echo ""
    echo "⚠️  WARNING: These are DEVELOPMENT-ONLY credentials!"
    echo "   Do NOT use these credentials in production environments."
    echo ""
    echo "View logs with: $0 logs"
    echo "Check status with: $0 status"
}

# Stop all containers
stop_all() {
    echo "🛑 Stopping all development containers..."
    stop_container "$AEROSPIKE_CONTAINER"
    stop_container "$REDPANDA_CONTAINER"
    stop_container "$POSTGRES_CONTAINER"
    echo "✅ All containers stopped"
}

# Restart all containers
restart_all() {
    echo "🔄 Restarting all development containers..."
    stop_all
    echo ""
    start_all
}

# Show container status
show_status() {
    echo "📊 Container Status:"
    echo ""

    local format="%-20s %-10s\n"
    printf "$format" "CONTAINER" "STATUS"
    printf "$format" "--------------------" "----------"

    for container in "$AEROSPIKE_CONTAINER" "$REDPANDA_CONTAINER" "$POSTGRES_CONTAINER"; do
        if is_container_running "$container"; then
            printf "$format" "$container" "running"
        else
            printf "$format" "$container" "stopped"
        fi
    done
}

# Show logs for all containers
show_logs() {
    local follow="${1:-false}"

    if [ "$follow" = "true" ]; then
        echo "📜 Following Container Logs (Ctrl+C to stop):"
        echo ""

        local pids=()

        cleanup_logs() {
            echo ""
            echo "Stopping log streams..."
            for pid in "${pids[@]}"; do
                kill "$pid" 2>/dev/null || true
            done
            exit 0
        }

        trap cleanup_logs SIGINT SIGTERM

        for container in "$AEROSPIKE_CONTAINER" "$REDPANDA_CONTAINER" "$POSTGRES_CONTAINER"; do
            if $CONTAINER_BINARY inspect "$container" &>/dev/null; then
                $CONTAINER_BINARY logs --follow "$container" 2>&1 | while IFS= read -r line; do
                    echo "[${container}] $line"
                done &
                pids+=($!)
            fi
        done

        if [ ${#pids[@]} -eq 0 ]; then
            echo "No running containers found"
            return 1
        fi

        wait
    else
        echo "📜 Container Logs:"
        echo ""

        for container in "$AEROSPIKE_CONTAINER" "$REDPANDA_CONTAINER" "$POSTGRES_CONTAINER"; do
            if $CONTAINER_BINARY inspect "$container" &>/dev/null; then
                echo "=== ${container} ==="
                $CONTAINER_BINARY logs -n 50 "$container" 2>&1 | tail -n 20
                echo ""
            fi
        done
    fi
}

# Main execution
ACTION="${1:-}"

if [ -z "$ACTION" ]; then
    usage
fi

case "$ACTION" in
    up|start)
        start_all
        ;;
    down|stop)
        stop_all
        ;;
    restart)
        restart_all
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    logs-follow|follow)
        show_logs true
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        echo "Error: Invalid action '$ACTION'" >&2
        usage
        ;;
esac

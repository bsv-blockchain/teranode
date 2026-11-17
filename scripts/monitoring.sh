#!/usr/bin/env bash
set -euo pipefail

# Use DATADIR environment variable if set, otherwise default to ./data
DATADIR="${DATADIR:-./data}"

# Default action is 'up' if no argument provided
ACTION="${1:-up}"

# Function to display usage
usage() {
    echo "Usage: $0 [up|down|restart]"
    echo "  up      - Start monitoring services (default)"
    echo "  down    - Stop monitoring services"
    echo "  restart - Restart monitoring services"
    exit 1
}

# Validate action
case "$ACTION" in
    up|down|restart)
        ;;
    *)
        echo "Error: Invalid action '$ACTION'" >&2
        usage
        ;;
esac

# Create data directories (only needed for 'up' and 'restart')
if [[ "$ACTION" == "up" || "$ACTION" == "restart" ]]; then
    mkdir -p "${DATADIR}/grafana"
    mkdir -p "${DATADIR}/prometheus"
fi

DATA_PATH=$(realpath "$DATADIR")
export DATA_PATH

cd deploy/docker/monitoring || exit

# Execute the requested action
case "$ACTION" in
    up)
        docker compose -f docker-compose.yml up -d
        echo "Monitoring services started"
        echo "Grafana: http://localhost:3005"
        echo "Prometheus: http://localhost:9090"
        echo "Note: Prometheus is configured to scrape metrics from host.docker.internal:9091"
        echo "This expects Teranode to be running on the host machine (not in Docker)"
        ;;
    down)
        docker compose -f docker-compose.yml down
        echo "Monitoring services stopped"
        ;;
    restart)
        docker compose -f docker-compose.yml restart
        echo "Monitoring services restarted"
        echo "Grafana: http://localhost:3005"
        echo "Prometheus: http://localhost:9090"
        ;;
esac

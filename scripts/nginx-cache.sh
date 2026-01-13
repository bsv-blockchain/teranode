#!/usr/bin/env bash
set -euo pipefail

# Script to run nginx cache proxy locally for development
# This connects to the asset server running on localhost:8090

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_SERVICES="${SCRIPT_DIR}/../deploy/docker/base/docker-services.yml"
NGINX_CONF="${SCRIPT_DIR}/../deploy/docker/base/asset-cache-nginx.conf"
CONTAINER_NAME="teranode-nginx-cache"
NGINX_PORT="${NGINX_PORT:-8000}"

# Extract nginx image from docker-services.yml (single source of truth)
NGINX_IMAGE=$(grep -A2 'asset-cache:' "$DOCKER_SERVICES" | grep 'image:' | awk '{print $2}')

# Default action is 'up' if no argument provided
ACTION="${1:-up}"

# Function to display usage
usage() {
    echo "Usage: $0 [up|down|restart]"
    echo "  up      - Start nginx cache proxy (default)"
    echo "  down    - Stop nginx cache proxy"
    echo "  restart - Restart nginx cache proxy"
    exit 1
}

# Validate action
case "$ACTION" in
    up|down|restart)
        ;;
    *)
        echo "Error: Invalid action '$ACTION'"
        usage
        ;;
esac

# Execute the requested action
case "$ACTION" in
    up)
        # Stop existing if running
        docker stop "$CONTAINER_NAME" 2>/dev/null || true
        docker rm "$CONTAINER_NAME" 2>/dev/null || true

        # Check if the nginx config exists
        if [ ! -f "${NGINX_CONF}" ]; then
            echo "Error: nginx config not found at ${NGINX_CONF}"
            exit 1
        fi

        # Create temp config with backend substitution for local dev
        # DNS resolver 127.0.0.11 works for both Docker and standalone containers
        # host.docker.internal is resolved via /etc/hosts (--add-host), not DNS
        TMP_CONF=$(mktemp)
        trap "rm -f ${TMP_CONF}" EXIT
        sed "s/asset:8090/host.docker.internal:8090/g" "$NGINX_CONF" > "$TMP_CONF"

        # Run container
        docker run -d \
            --name "$CONTAINER_NAME" \
            -p "${NGINX_PORT}:8000" \
            -v "${TMP_CONF}:/etc/nginx/nginx.conf:ro" \
            --add-host host.docker.internal:host-gateway \
            --tmpfs /tmp/nginx:rw,exec,size=512m \
            "$NGINX_IMAGE" sh -c "mkdir -p /tmp/nginx/cache && nginx -g 'daemon off;'"

        echo "Nginx cache proxy started"
        echo "  Image: ${NGINX_IMAGE}"
        echo "  Cache proxy: http://localhost:${NGINX_PORT}"
        echo "  Backend: localhost:8090 (via host.docker.internal)"
        echo ""
        echo "Test with:"
        echo "  curl -v http://localhost:${NGINX_PORT}/api/v1/block/<block_hash>"
        echo ""
        echo "View logs:"
        echo "  docker logs -f ${CONTAINER_NAME}"
        ;;
    down)
        docker stop "$CONTAINER_NAME" 2>/dev/null || true
        docker rm "$CONTAINER_NAME" 2>/dev/null || true
        echo "Nginx cache proxy stopped"
        ;;
    restart)
        "$0" down
        "$0" up
        ;;
esac

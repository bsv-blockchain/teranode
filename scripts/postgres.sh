#!/usr/bin/env bash
set -euo pipefail

# Source common helper functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/docker-service-helper.sh"

# Initialize and create postgres data directory
docker_service_init "postgres"

# Run docker compose with service-specific info
docker_service_run "${1:-up}" "deploy/docker/postgres" "PostgreSQL" \
    "Persistent data: ${DATA_PATH}/postgres" \
    "Connect with: postgresql://teranode:teranode@localhost:5432/teranode"

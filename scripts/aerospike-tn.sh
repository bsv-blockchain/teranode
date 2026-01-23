#!/usr/bin/env bash
set -euo pipefail

# Source common helper functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/docker-service-helper.sh"

# Initialize and create aerospike data directory
docker_service_init "aerospike"

# Run docker compose with custom compose file and service-specific info
docker_service_run "${1:-up}" "deploy/docker/aerospike" "Aerospike TN edition" "docker-compose-tn.yml" \
    "Persistent data: ${DATA_PATH}/aerospike" \
    "Connect with: aerospike://localhost:3000"

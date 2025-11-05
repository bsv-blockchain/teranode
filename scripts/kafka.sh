#!/usr/bin/env bash
set -euo pipefail

# Source common helper functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/docker-service-helper.sh"

# Initialize (DATA_PATH needed to suppress warnings from base docker-services.yml)
docker_service_init

# Run docker compose with service-specific info
docker_service_run "${1:-up}" "deploy/docker/kafka" "Kafka and Kafka Console (ephemeral - no persistent data)" \
    "Kafka broker: localhost:9092" \
    "Kafka Console UI: http://localhost:8080" \
    "" \
    "IMPORTANT: Add this to /etc/hosts (required for all developers):" \
    "127.0.0.1	kafka-shared" \
    "" \
    "Run this command to add it:" \
    "sudo sh -c 'echo \"127.0.0.1\tkafka-shared\" >> /etc/hosts'" \
    "" \
    "Why: Kafka advertises itself as 'kafka-shared:9092'. Docker containers can" \
    "resolve this via internal DNS, but your host machine needs this /etc/hosts" \
    "entry to connect. Without it, Kafka clients will fail to connect."

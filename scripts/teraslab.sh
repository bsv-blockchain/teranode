#!/usr/bin/env bash
set -euo pipefail

# Source common helper functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/docker-service-helper.sh"

# TeraSlab keeps two stores. The working dir (slab device + redo/tombstone WAL)
# is the hot path and the source the in-memory index rebuilds from on restart —
# keep it on fast local/internal disk (./data by default). The cold blob store
# (tx bodies > 8 KiB) is bulky but cold — put it on the roomy DATADIR volume
# (e.g. an external drive). Override either with TERASLAB_WORKDIR / TERASLAB_BLOBDIR.
workdir="${TERASLAB_WORKDIR:-$(pwd)/data/teraslab}"
blobdir="${TERASLAB_BLOBDIR:-${DATADIR:-$(pwd)/data}/teraslab-blob}"

# Create up front so the bind mounts are owned by the host user, not root.
mkdir -p "$workdir" "$blobdir"
TERASLAB_WORKDIR="$(cd "$workdir" && pwd)"
TERASLAB_BLOBDIR="$(cd "$blobdir" && pwd)"
export TERASLAB_WORKDIR TERASLAB_BLOBDIR

# Run docker compose with the teraslab compose file and service-specific info
docker_service_run "${1:-up}" "deploy/docker/teraslab" "TeraSlab UTXO store (single-node)" "docker-compose.yml" \
    "Working dir (device): ${TERASLAB_WORKDIR}" \
    "Blob store:           ${TERASLAB_BLOBDIR}" \
    "Health:               http://localhost:9100/health/live" \
    "Connect with:         teraslab://localhost:3300"

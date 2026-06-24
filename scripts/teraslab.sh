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

# Admin dashboard token. node.toml enables the admin endpoints (/admin/*, /ws/top)
# the web UI needs, which require a bearer token (>=16 bytes since the server
# binds non-loopback). Generate one on first run and persist it under the
# gitignored working dir — never commit it. Override by exporting
# TERASLAB_ADMIN_TOKEN before running.
token_file="${TERASLAB_WORKDIR}/.admin_token"
if [ -z "${TERASLAB_ADMIN_TOKEN:-}" ]; then
    if [ ! -s "$token_file" ]; then
        (openssl rand -hex 32 2>/dev/null || LC_ALL=C tr -dc 'a-f0-9' </dev/urandom | head -c 64) > "$token_file"
        chmod 600 "$token_file"
    fi
    TERASLAB_ADMIN_TOKEN="$(cat "$token_file")"
fi
export TERASLAB_ADMIN_TOKEN

# Run docker compose with the teraslab compose file and service-specific info
docker_service_run "${1:-up}" "deploy/docker/teraslab" "TeraSlab UTXO store (single-node)" "docker-compose.yml" \
    "Working dir (device): ${TERASLAB_WORKDIR}" \
    "Blob store:           ${TERASLAB_BLOBDIR}" \
    "Health:               http://localhost:9100/health/live" \
    "Admin web UI:         http://localhost:9100/ui  (token: ${TERASLAB_ADMIN_TOKEN})" \
    "Connect with:         teraslab://localhost:3300"

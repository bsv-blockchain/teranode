#!/bin/sh
# cold-bench-postgres.sh — memory-capped postgres for the cold-regime UTXO bench
# (stores/utxo/throughput_cold_test.go).
#
# The container's cgroup memory cap bounds BOTH shared_buffers and the Linux
# page cache inside the container, so a dataset larger than the cap genuinely
# reads from disk. NOTE (macOS): the host can still cache the Docker VM's disk
# image; treat macOS results as indicative and a Linux host as the reference.
#
# Usage:
#   ./scripts/cold-bench-postgres.sh          # start (or restart) the cluster
#   ./scripts/cold-bench-postgres.sh stop     # stop + remove container and data
#
# Then export THROUGHPUT_DSN as printed by the script on startup.

set -eu

NAME=teranode-cold-pg
PORT="${COLD_PG_PORT:-5440}"
MEM="${COLD_PG_MEM:-2g}"
# Local throwaway bench container only — override for anything shared.
PASSWORD="${COLD_PG_PASSWORD:-teranode}"
# Postgres-syntax view of the cap for the planner (docker's "2g" is not valid
# postgres units). Keep slightly below the cap: the planner should not assume
# more cache than the cgroup allows.
EFF_CACHE="${COLD_PG_EFF_CACHE:-1536MB}"
SHARED_BUFFERS="${COLD_PG_SHARED_BUFFERS:-512MB}"
IMAGE="${COLD_PG_IMAGE:-postgres:18}"

if [ "${1:-}" = "stop" ]; then
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    echo "stopped and removed $NAME"
    exit 0
fi

docker rm -f "$NAME" >/dev/null 2>&1 || true

# --memory + --memory-swap equal: hard cap, no swap escape hatch.
# Settings mirror the production-relevant knobs the throughput suite assumes;
# fsync stays ON (durability is a hard constraint of this store).
docker run -d --name "$NAME" \
    --memory="$MEM" --memory-swap="$MEM" \
    --shm-size=1g \
    -p "$PORT":5432 \
    -e POSTGRES_USER=teranode \
    -e POSTGRES_PASSWORD="$PASSWORD" \
    -e POSTGRES_DB=teranode_test \
    "$IMAGE" \
    -c shared_buffers="$SHARED_BUFFERS" \
    -c max_connections=400 \
    -c wal_compression=lz4 \
    -c max_wal_size=8GB \
    -c checkpoint_timeout=300s \
    -c effective_cache_size="$EFF_CACHE" \
    -c track_io_timing=on \
    >/dev/null

printf 'waiting for postgres'
i=0
until docker exec "$NAME" pg_isready -U teranode -d teranode_test >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -gt 60 ]; then
        echo ' FAILED (60s)'
        docker logs "$NAME" | tail -20
        exit 1
    fi
    printf '.'
    sleep 1
done
echo ' ready'

echo "cluster : $NAME ($IMAGE), memory cap $MEM, shared_buffers $SHARED_BUFFERS, port $PORT"
echo "export THROUGHPUT_DSN=postgres://teranode:$PASSWORD@localhost:$PORT/teranode_test"

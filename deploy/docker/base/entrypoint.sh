#!/bin/bash
set -e

# Function to wait for a service
wait_for_service() {
  local host=$1
  local port=$2
  local retries=$3
  shift 3
  /app/wait.sh "$host" "$port" "$retries" -- "$@"
}

# Conditionally wait for Aerospike if using local instance
if [ "$USE_LOCAL_AEROSPIKE" = "true" ]; then
  wait_for_service aerospike 3000 2
fi

if [ "$USE_LOCAL_POSTGRES" = "true" ]; then
  wait_for_service postgres 5432 1
fi

if [ "$USE_LOCAL_KAFKA" = "true" ]; then
  wait_for_service kafka-shared 9092 0
fi

# Execute the main application
exec /app/teranode.run "$@"

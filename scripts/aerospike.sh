#!/bin/bash

# Create a volume for persistent Aerospike data if it doesn't exist
docker volume create aerospike-data 2>/dev/null || true

docker run -d --rm \
  --name aerospike \
  -p 3000-3002:3000-3002 \
  -v ./scripts/aerospike.conf:/opt/aerospike/aerospike.conf \
  -v aerospike-data:/opt/aerospike/data \
  aerospike/aerospike-server \
  --config-file /opt/aerospike/aerospike.conf

echo "Aerospike started with persistent volume 'aerospike-data'"
echo "To remove the volume: docker volume rm aerospike-data"

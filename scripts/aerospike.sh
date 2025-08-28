#!/bin/bash

# Use DATADIR environment variable if set, otherwise default to ./data
DATADIR="${DATADIR:-./data}"

docker run -d \
  --name aerospike \
  -p 3000-3002:3000-3002 \
  -v ./scripts/aerospike.conf:/opt/aerospike/aerospike.conf \
  -v ${DATADIR}/aerospike:/opt/aerospike/data \
  aerospike/aerospike-server \
  --config-file /opt/aerospike/aerospike.conf

echo "Aerospike started with persistent data in '${DATADIR}/aerospike'"

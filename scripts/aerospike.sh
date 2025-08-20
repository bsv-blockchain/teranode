#!/bin/bash

docker run -d --rm \
  --name aerospike \
  -p 3000-3002:3000-3002 \
  -v ./scripts/aerospike.conf:/opt/aerospike/aerospike.conf \
  -v ./data/aerospike:/opt/aerospike/data \
  aerospike/aerospike-server \
  --config-file /opt/aerospike/aerospike.conf

echo "Aerospike started with persistent data in './data/aerospike'"

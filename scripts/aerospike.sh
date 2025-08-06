#!/bin/bash

docker run -d \
  --name aerospike \
  -p 3000-3002:3000-3002 \
  -v ./scripts/aerospike.conf:/opt/aerospike/aerospike.conf \
  aerospike/aerospike-server \
  --config-file /opt/aerospike/aerospike.conf

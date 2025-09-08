#!/bin/bash

DATA_PATH=$(realpath "./data")
export DATA_PATH

cd deploy/docker/aerospike-cluster || exit
docker compose up -d

echo "Aerospike cluster started with 3 nodes"
echo "Recommended to change your utxostore to use aerospike://localhost:3000,localhost:3010,localhost:3020..."

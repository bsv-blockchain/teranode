#!/bin/bash

DATA_PATH=$(realpath "./data")
export DATA_PATH

cd deploy/docker/aerospike || exit
docker compose -f docker-compose-ee.yml up -d

echo "Aerospike enterprise edition evaluation mode started"
echo "Connect with aerospike://localhost:3000"

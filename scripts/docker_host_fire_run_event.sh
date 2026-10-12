#!/bin/sh

# Function to wait for a service to be ready
wait_for_service() {
  host=$1
  port=$2
  while ! nc -z $host $port; do
    echo "Waiting for $host:$port to be ready..."
    sleep 1
  done
}

# Wait for teranode services to be ready
wait_for_service localhost 38087
wait_for_service localhost 28087
wait_for_service localhost 18087

# Send gRPC requests to each teranode container
echo "Sending gRPC requests..."

# Blockchain requires x-api-key on Run and serves no reflection by default, so
# pass the key and load the service definition from the proto files.
api_key=${grpc_admin_api_key:-docker-e2e-test-admin-key}
repo_root=$(cd "$(dirname "$0")/.." && pwd)

for port in 38087 28087 18087; do
  grpcurl -plaintext \
    -H "x-api-key: $api_key" \
    -import-path "$repo_root" \
    -proto services/blockchain/blockchain_api/blockchain_api.proto \
    localhost:$port blockchain_api.BlockchainAPI.Run
done

echo "gRPC requests sent."

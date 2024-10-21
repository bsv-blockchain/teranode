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

# Wait for ubsv services to be ready
wait_for_service localhost 38087
wait_for_service localhost 28087
wait_for_service localhost 18087

# Send gRPC requests to each ubsv container
echo "Sending gRPC requests..."

# Replace `your.package.Service/YourMethod` with the actual service and method names
grpcurl -plaintext localhost:38087 blockchain_api.BlockchainAPI.Run
grpcurl -plaintext localhost:28087 blockchain_api.BlockchainAPI.Run
grpcurl -plaintext localhost:18087 blockchain_api.BlockchainAPI.Run

echo "gRPC requests sent."

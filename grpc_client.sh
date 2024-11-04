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
wait_for_service ubsv1 8087
wait_for_service ubsv2 8087
wait_for_service ubsv3 8087

# Additional wait time of 20 seconds
echo "All ubsv services are up. Waiting for 20 seconds..."
sleep 20

# Send gRPC requests to each ubsv container
echo "Sending gRPC requests..."

# Replace `your.package.Service/YourMethod` with the actual service and method names
grpcurl -plaintext ubsv1:8087 blockchain_api.BlockchainAPI.Run
grpcurl -plaintext ubsv2:8087 blockchain_api.BlockchainAPI.Run
grpcurl -plaintext ubsv3:8087 blockchain_api.BlockchainAPI.Run

echo "gRPC requests sent."

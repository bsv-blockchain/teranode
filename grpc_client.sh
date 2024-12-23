#!/bin/sh

# Trap for cleanup on container shutdown or interrupt
cleanup() {
    echo "\nReceived shutdown signal - exiting gracefully..."
    exit 0
}

# Set up trap for SIGTERM and SIGINT
trap cleanup TERM INT

# Function to wait for a service to be ready using HTTP health check
wait_for_service() {
    host=$1
    port=$2
    max_attempts=60  # Add timeout after 60 seconds
    attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        # Use timeout command to prevent curl from hanging
        status=$(timeout 1 curl -s -o /dev/null -w "%{http_code}" "http://$host:$port/health/readiness" || echo " timeout")
        if [ "$status" = "200" ]; then
            echo "$host:$port is ready"
            return 0
        else
            echo "Waiting for $host:$port to be ready... (Status: $status)"
            sleep 1
            attempt=$((attempt + 1))
        fi
    done
    
    echo "Timeout waiting for $host:$port after $max_attempts seconds"
    exit 1
}

# Wait for teranode services to be ready
wait_for_service teranode3 8090
wait_for_service teranode2 8090
wait_for_service teranode1 8090

# Send gRPC requests to each teranode container
echo "Sending gRPC requests..."

# Replace `your.package.Service/YourMethod` with the actual service and method names
grpcurl -plaintext teranode3:8087 blockchain_api.BlockchainAPI.Run
grpcurl -plaintext teranode2:8087 blockchain_api.BlockchainAPI.Run
grpcurl -plaintext teranode1:8087 blockchain_api.BlockchainAPI.Run

echo "gRPC requests sent."

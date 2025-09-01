#!/bin/bash

# Script to build teranode and start nodes
# Usage: ./gstart.sh [1|2|3|all]
# No argument defaults to starting all nodes

set -e

# Default to all nodes if no argument provided
NODE_ARG="${1:-all}"

# Build teranode
echo "Building teranode..."
if ! make build-teranode; then
    echo "Build failed!"
    exit 1
fi
echo "Build successful!"

# Function to start a node
start_node() {
    local node_num=$1
    echo "Starting teranode${node_num}..."
    
    # Clean up any stale socket files before starting
    # Remove both the default TERANODE.sock and the numbered version
    if [ -e "/tmp/gocore/TERANODE.sock" ]; then
        echo "Removing stale default socket file..."
        rm -f /tmp/gocore/TERANODE.sock
    fi
    if [ -e "/tmp/gocore/TERANODE${node_num}.sock" ]; then
        echo "Removing stale socket file for teranode${node_num}..."
        rm -f /tmp/gocore/TERANODE${node_num}.sock
    fi
    
    # Ensure the socket directory exists
    mkdir -p /tmp/gocore
    
    # Use socketDIR config to put socket in a unique location per node
    socketDIR=/tmp/gocore/node${node_num} SETTINGS_CONTEXT=docker.host.teranode${node_num} logLevel=DEBUG ./teranode.run -localTestStartFromState=RUNNING > teranode${node_num}.log 2>&1 &
    echo "Teranode${node_num} started with PID $! (logging to teranode${node_num}.log)"
}

# Start nodes based on argument
case "$NODE_ARG" in
    1)
        start_node 1
        ;;
    2)
        start_node 2
        ;;
    3)
        start_node 3
        ;;
    all)
        start_node 1
        sleep 1
        start_node 2
        sleep 1
        start_node 3
        ;;
    *)
        echo "Usage: $0 [1|2|3|all]"
        echo "  1    - Start only teranode1"
        echo "  2    - Start only teranode2"
        echo "  3    - Start only teranode3"
        echo "  all  - Start all three nodes (default)"
        exit 1
        ;;
esac

echo "Done!"
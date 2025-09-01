#!/bin/bash

# Function to stop a specific teranode instance
stop_teranode() {
    local instance=$1
    echo "Stopping teranode${instance}..."
    
    # Find the PID by checking for the socketDIR in the environment
    local PID=$(ps eww | grep "socketDIR=/tmp/gocore/node${instance}" | grep -v grep | awk '{print $1}')
    
    if [ -n "$PID" ]; then
        echo "Found teranode${instance} with PID: $PID"
        
        # Try graceful kill first
        kill $PID 2>/dev/null
        
        # Wait up to 3 seconds for graceful shutdown
        for i in 1 2 3; do
            if ! kill -0 $PID 2>/dev/null; then
                break
            fi
            sleep 1
        done
        
        # Force kill if still running
        if kill -0 $PID 2>/dev/null; then
            echo "Force killing teranode${instance}..."
            kill -9 $PID 2>/dev/null
        fi
        
        echo "teranode${instance} stopped"
    else
        echo "teranode${instance} is not running"
    fi
    
    # Clean up the specific Unix socket (try both old and new locations)
    if [ -e "/tmp/gocore/TERANODE${instance}.sock" ]; then
        rm -f /tmp/gocore/TERANODE${instance}.sock
        echo "Cleaned up socket for teranode${instance}"
    fi
    if [ -d "/tmp/gocore/node${instance}" ]; then
        rm -rf /tmp/gocore/node${instance}
        echo "Cleaned up socket directory for teranode${instance}"
    fi
}

# Check if a specific instance was requested
if [ $# -eq 1 ]; then
    case $1 in
        1|2|3)
            stop_teranode $1
            ;;
        all)
            echo "Stopping all teranode instances..."
            for i in 1 2 3; do
                stop_teranode $i
            done
            ;;
        *)
            echo "Usage: $0 [1|2|3|all]"
            echo "  1/2/3 - Stop specific teranode instance"
            echo "  all   - Stop all instances (default)"
            echo "  no argument - Stop all instances"
            exit 1
            ;;
    esac
else
    # No argument provided, stop all
    echo "Stopping all teranode instances..."
    
    # Kill all teranode processes
    pkill -f "teranode.run"
    
    # Wait for processes to terminate
    sleep 2
    
    # Force kill if still running
    pkill -9 -f "teranode.run" 2>/dev/null
    
    # Clean up all Unix sockets (both old and new locations)
    echo "Cleaning up Unix sockets..."
    rm -f /tmp/gocore/TERANODE*.sock
    rm -rf /tmp/gocore/node*
    
    echo "All teranode instances stopped and cleaned up"
fi
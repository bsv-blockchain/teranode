#!/bin/bash

echo "Teranode Status:"
echo "================"

for i in 1 2 3; do
    # Find PID by checking environment for socket path
    PID=$(ps eww | grep "socketDIR=/tmp/gocore/node${i}" | grep -v grep | awk '{print $1}')
    
    if [ -n "$PID" ]; then
        echo "teranode${i}: RUNNING (PID: $PID)"
        
        # Check if socket exists
        if [ -e "/tmp/gocore/TERANODE${i}.sock" ]; then
            echo "  Socket: OK (/tmp/gocore/TERANODE${i}.sock)"
        else
            echo "  Socket: MISSING (may still be starting)"
        fi
        
        # Check log file
        if [ -f "teranode${i}.log" ]; then
            LAST_LOG=$(tail -1 teranode${i}.log | cut -c1-80)
            echo "  Last log: $LAST_LOG..."
        fi
    else
        echo "teranode${i}: STOPPED"
        
        # Check if socket exists (shouldn't if stopped properly)
        if [ -e "/tmp/gocore/TERANODE${i}.sock" ]; then
            echo "  Socket: EXISTS (stale, should be cleaned)"
        fi
    fi
    echo ""
done

# Check for any zombie teranode processes
ZOMBIES=$(pgrep -f "teranode.run" | wc -l)
EXPECTED=$(ps eww | grep -E "socketDIR=/tmp/gocore/node[123]" | grep -v grep | wc -l)

if [ $ZOMBIES -gt $EXPECTED ]; then
    echo "WARNING: Found $(($ZOMBIES - $EXPECTED)) unmanaged teranode process(es)"
    echo "Run './scripts/gdown.sh all' to clean up"
fi
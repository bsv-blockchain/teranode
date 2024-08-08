#!/bin/bash

# Check if logfile argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <logfile>"
    exit 1
fi

LOGFILE=$1

# Main loop to keep the process running
while true; do
    # Start the process and redirect both stdout and stderr to the log file
    logLevel=INFO startLegacy=true blockassembly_disabled=true legacy_verifyOnly=false ./ubsv.run -all=0 -blockchain=1 -legacy=1 -subtreevalidation=1 -blockvalidation=1 -validator=1 -blockpersister=1 -utxopersister=1 >> $LOGFILE 2>&1 &

    # Get the PID of the last process started in the background
    PID=$!

    # Wait for the process to exit
    wait $PID

    # Optional: Log the restart event
    echo "$(date -u +%Y-%m-%dT%H:%M:%S) - Process exited with status $?. Restarting..." >> $LOGFILE 2>&1

    # Sleep for a short period to avoid rapid restart in case of immediate failure
    sleep 10
done
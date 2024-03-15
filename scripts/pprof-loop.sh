#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <host> <port> <heap|profile>"
    exit 1
fi

HOST=$1
PORT=$2
COMMAND=$3
COUNTER=1

while true; do
    OUTPUT_FILE="$HOME/Downloads/${COMMAND}_${HOST}_${PORT}_${COUNTER}.pprof"

    curl http://$HOST:$PORT/debug/pprof/$COMMAND -o "$OUTPUT_FILE"

    if [ $? -eq 0 ]; then
        echo "Profile saved to $OUTPUT_FILE"
    else
        echo "Failed to download $COMMAND profile from $HOST:$PORT"
    fi

    ((COUNTER++))
    sleep 30
done

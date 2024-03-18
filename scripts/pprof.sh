#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <host> <port> <heap|profile>"
    exit 1
fi

HOST=$1
PORT=$2
COMMAND=$3

OUTPUT_FILE="$HOME/Downloads/$COMMAND.pprof"

curl http://$HOST:$PORT/debug/pprof/$COMMAND -o $OUTPUT_FILE

if [ $? -eq 0 ]; then
    echo "Heap profile saved to $OUTPUT_FILE"
    go tool pprof -http=:8080 $OUTPUT_FILE
else
    echo "Failed to download $COMMAND profile from $HOST:$PORT"
fi

#!/bin/sh

# Block generator script for chain integrity tests
# Generates blocks on teranode-1 with random delays

while true
do
  # Random delay between 1-5 seconds
  DELAY=$(( ( RANDOM % 5 ) + 1 ))
  echo "Generating block on teranode-1 (after $DELAY second delay)..."
  sleep $DELAY
  curl --user bitcoin:bitcoin -X POST http://localhost:19292 \
    -H "Content-Type: application/json" \
    -d '{"method": "generate", "params": [1]}'
done

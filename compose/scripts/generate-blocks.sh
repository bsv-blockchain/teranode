#!/bin/sh

# Function to handle signals for graceful shutdown
handle_signal() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Signal received, terminating script..."
  # Add any cleanup commands here if needed in the future
  exit 0
}

# Trap common termination signals
trap 'handle_signal' TERM INT QUIT HUP


# Block generator script for chain integrity tests
# Generates blocks on a specified teranode host with random delays

HOST="$1"
PORT="9292"

if [ -z "$HOST" ]; then
  echo "Usage: $0 <host>"
  exit 1
fi

echo "Waiting for service on ${HOST}:${PORT} to become available (max 30 retries)..."
RETRY_COUNT=0
MAX_RETRIES=30
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
  RETRY_COUNT=$((RETRY_COUNT + 1))
  echo "\nAttempt $RETRY_COUNT/$MAX_RETRIES: Checking service..."
  # Make curl verbose for debugging: removed --silent and --output /dev/null
  # Added command to capture exit code
  curl_output=$(curl --user bitcoin:bitcoin --fail -X POST -H "Content-Type: application/json" --data '{"method": "getblockchaininfo", "params": []}' "http://${HOST}:${PORT}" 2>&1)
  curl_exit_code=$?
  echo "Curl exit code: $curl_exit_code"
  if [ $curl_exit_code -eq 0 ]; then
    echo "Curl command succeeded."
    break # Exit loop if curl succeeds
  else
    echo "Curl command failed. Output:"
    echo "$curl_output"
    if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
      echo "\nMax retries reached. Service at ${HOST}:${PORT} did not become available. Exiting."
      exit 1
    fi
    echo "Retrying in 2 seconds..."
    sleep 2
  fi
done

# If loop exited due to success (not max retries)
if [ $curl_exit_code -ne 0 ]; then
  echo "\nFailed to connect to service at ${HOST}:${PORT} after $MAX_RETRIES attempts. Exiting."
  exit 1
fi
sleep 0.5
echo "\nService ${HOST}:${PORT} is available."

echo "Generating 100 blocks on teranode-1..."
curl --user bitcoin:bitcoin -X POST http://${HOST}:${PORT} \
    -H "Content-Type: application/json" \
    -d '{"method": "generate", "params": [100]}'
echo "Done."

while true
do
  # Random delay between 250ms-1000ms
  DELAY_MS=$(( (RANDOM % 751) + 250 ))
  DELAY_S=$(awk -v delay_ms="$DELAY_MS" 'BEGIN { printf "%.3f", delay_ms / 1000 }')
  echo "Generating block on teranode-1 (after $DELAY_S seconds delay)..."
  sleep $DELAY_S
  curl --user bitcoin:bitcoin -X POST http://${HOST}:${PORT} \
    -H "Content-Type: application/json" \
    -d '{"method": "generate", "params": [1]}'
done

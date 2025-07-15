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

# Default values
HOST=""
PORT="9292"
NUM_BLOCKS=-1 # -1 means infinite
GENERATE=false

# Parse command-line arguments
while [ "$#" -gt 0 ]; do
  case "$1" in
    --host)
      HOST="$2"
      shift 2
      ;;
    --numBlocks)
      NUM_BLOCKS="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --generate)
      GENERATE=true
      shift 1
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 --host <host> [--port <port>] [--numBlocks <number>] [--generate]"
      exit 1
      ;;
  esac
done

if [ -z "$HOST" ]; then
  echo "Error: --host is a required argument."
  echo "Usage: $0 --host <host> [--port <port>] [--numBlocks <number>] [--generate]"
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

if [ "$GENERATE" = true ]; then
  echo "Pre-generating 100 blocks due to --generate flag..."
  curl --user bitcoin:bitcoin -X POST http://${HOST}:${PORT} \
      -H "Content-Type: application/json" \
      -d '{"method": "generate", "params": [100]}'
  echo "Done pre-generating blocks."
fi

COUNT=0
while [ "$NUM_BLOCKS" -eq -1 ] || [ "$COUNT" -lt "$NUM_BLOCKS" ]; do
  # Random delay between 5s and 7s
  DELAY_MS=$(( (RANDOM % 2000) + 2000 ))
  DELAY_S=$(awk -v delay_ms="$DELAY_MS" 'BEGIN { printf "%.3f", delay_ms / 1000 }')
  
  if [ "$NUM_BLOCKS" -ne -1 ]; then
    echo "Generating block $((COUNT + 1))/$NUM_BLOCKS (after $DELAY_S seconds delay)..."
  else
    echo "Generating block (after $DELAY_S seconds delay)..."
  fi

  sleep $DELAY_S
  curl --user bitcoin:bitcoin -X POST http://${HOST}:${PORT} \
    -H "Content-Type: application/json" \
    -d '{"method": "generate", "params": [1]}'
  
  COUNT=$((COUNT + 1))
done

if [ "$NUM_BLOCKS" -ne -1 ]; then
    echo "Finished generating $NUM_BLOCKS blocks."
    exit 0
fi

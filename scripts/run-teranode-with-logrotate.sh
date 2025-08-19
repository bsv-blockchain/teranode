#!/bin/bash

# Check if logrotate is installed
if ! command -v logrotate &> /dev/null; then
    echo "Please install logrotate. On MacOS use \`brew install logrotate\`"
    exit 1
fi

# Check if SETTINGS_CONTEXT is set
if [ -z "$SETTINGS_CONTEXT" ]; then
    echo "Error: SETTINGS_CONTEXT environment variable is not set"
    exit 1
fi

# Configuration
LOG_DIR="./logs"
LOG_FILE="$LOG_DIR/teranode.log"
LOGROTATE_CONFIG="./teranode.logrotate"
LOGROTATE_STATE="$LOG_DIR/logrotate.state"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

echo "Starting teranode with logrotate..."
echo "Using SETTINGS_CONTEXT: $SETTINGS_CONTEXT"
echo "Logging to: $LOG_FILE"
echo "To monitor: tail -f $LOG_FILE"
echo "Press Ctrl+C to stop"

# Start logrotate in background (check every 60 seconds)
(
    while true; do
        logrotate -s "$LOGROTATE_STATE" "$LOGROTATE_CONFIG" 2>/dev/null
        sleep 60
    done
) &
LOGROTATE_PID=$!

# Cleanup function
cleanup() {
    echo "Stopping logrotate..."
    kill $LOGROTATE_PID 2>/dev/null
    exit
}

# Set trap for cleanup
trap cleanup INT TERM

# Run teranode with output to log file only
./teranode.run  >> "$LOG_FILE" 2>&1

# Cleanup when teranode exits
cleanup

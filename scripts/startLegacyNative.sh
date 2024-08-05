#!/bin/bash

# Determine the directory of the script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Navigate to the script's directory (optional, if make needs to run in the script directory)
cd "$SCRIPT_DIR" || exit 1

# Build the project
make build-ubsv

# Run startLooper.sh with nohup in the same directory
nohup "$SCRIPT_DIR/startLooper.sh" &
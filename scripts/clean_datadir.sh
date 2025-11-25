#!/bin/bash

set -e

# Parse arguments
FORCE=false
if [ "$1" == "--force" ] || [ "$1" == "-f" ]; then
    FORCE=true
fi

# Check if DATADIR is set
if [ -z "$DATADIR" ]; then
    echo "Error: DATADIR environment variable is not set"
    exit 1
fi

# Check if DATADIR exists
if [ ! -d "$DATADIR" ]; then
    echo "Error: DATADIR '$DATADIR' does not exist"
    exit 1
fi

echo "Cleaning folders in: $DATADIR"
echo "This will delete all subdirectories but preserve p2p.key in the root"

if [ "$FORCE" = false ]; then
    read -p "Are you sure you want to continue? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo "Operation cancelled"
        exit 0
    fi
fi

TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

echo "Creating temporary directory: $TEMP_DIR"

# Use rsync to delete everything and sync with temp directory
echo "Running rsync to clean directories..."
rsync -a --delete --exclude='p2p.key' "$TEMP_DIR/" "$DATADIR/"

echo "✅ Cleanup complete!"
echo "Contents of $DATADIR:"
ls -la "$DATADIR/"

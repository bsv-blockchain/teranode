#!/bin/bash

cd $(dirname "$0")/..

# Build the project
make build-ubsv

LOGFILE=$(date -u +%Y-%m-%dT%H:%M:%S).log

# Update the symlink to the current log file
rm -f current
ln -s $LOGFILE current

# Run startLooper.sh with nohup in the same directory
nohup ./scripts/startLooper.sh $LOGFILE >> $LOGFILE 2>&1 &
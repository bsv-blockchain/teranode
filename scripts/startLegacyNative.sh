#!/bin/bash

cd $(dirname "$0")/..

# Build the project
make build-ubsv

# Run startLooper.sh with nohup in the same directory
nohup ./scripts/startLooper.sh &
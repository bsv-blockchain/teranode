#!/bin/bash

# Chaos Test Scenario 08: Block Validation Memory Pressure
# This script runs the block validation and memory stress test

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}[INFO]${NC} Starting Scenario 08: Block Validation Memory Pressure"
echo -e "${GREEN}[INFO]${NC} This test validates block validation resilience under various conditions:"
echo -e "${GREEN}[INFO]${NC}   - Transient subtrees (many small blocks)"
echo -e "${GREEN}[INFO]${NC}   - Deep chains (stress caching)"
echo -e "${GREEN}[INFO]${NC}   - Mixed patterns (random characteristics)"
echo -e "${GREEN}[INFO]${NC}   - Concurrent validation"
echo -e "${GREEN}[INFO]${NC}   - Cache eviction under pressure"
echo ""

echo -e "${GREEN}[INFO]${NC} =================================================="
echo -e "${GREEN}[INFO]${NC} Running Scenario 8: Block Validation Memory"
echo -e "${GREEN}[INFO]${NC} =================================================="
echo ""

# Run the test with verbose output
go test -v ./test/chaos -run TestScenario08

TEST_EXIT_CODE=$?

echo ""
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}[INFO]${NC} =================================================="
    echo -e "${GREEN}[INFO]${NC} ✅ Test PASSED"
    echo -e "${GREEN}[INFO]${NC} =================================================="
else
    echo -e "${RED}[ERROR]${NC} =================================================="
    echo -e "${RED}[ERROR]${NC} ❌ Test FAILED"
    echo -e "${RED}[ERROR]${NC} =================================================="
fi
echo ""

exit $TEST_EXIT_CODE

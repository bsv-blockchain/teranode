#!/bin/bash
# Quick script to run Scenario 1: Database Latency chaos test

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

echo_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

echo_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if docker compose is running
echo_info "Checking if toxiproxy is running..."
if ! curl -s http://localhost:8474/version > /dev/null 2>&1; then
    echo_error "Toxiproxy is not running!"
    echo_info "Starting services with: docker compose -f compose/docker-compose-ss.yml up -d"
    cd "$PROJECT_ROOT"
    docker compose -f compose/docker-compose-ss.yml up -d

    echo_info "Waiting for services to be ready..."
    sleep 10
fi

# Verify toxiproxy is available
if ! curl -s http://localhost:8474/version > /dev/null 2>&1; then
    echo_error "Toxiproxy still not available after starting services"
    exit 1
fi

echo_info "Toxiproxy is available at http://localhost:8474"

# Check PostgreSQL is accessible
echo_info "Checking PostgreSQL..."
if ! nc -zv localhost 5432 2>&1 | grep -q succeeded; then
    echo_error "PostgreSQL is not accessible on localhost:5432"
    echo_info "Check with: docker ps | grep postgres"
    exit 1
fi

echo_info "PostgreSQL is accessible"

# Check PostgreSQL through toxiproxy
echo_info "Checking PostgreSQL through toxiproxy..."
if ! nc -zv localhost 15432 2>&1 | grep -q succeeded; then
    echo_error "PostgreSQL through toxiproxy is not accessible on localhost:15432"
    exit 1
fi

echo_info "PostgreSQL through toxiproxy is accessible"

# Reset toxiproxy to clean state
echo_info "Resetting toxiproxy to clean state..."
curl -s -X POST http://localhost:8474/reset > /dev/null

echo ""
echo_info "=========================================="
echo_info "Running Scenario 1: Database Latency Test"
echo_info "=========================================="
echo ""

# Run the test
cd "$PROJECT_ROOT"
go test -v -count=1 ./test/chaos -run TestScenario01_DatabaseLatency

TEST_EXIT_CODE=$?

echo ""
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo_info "=========================================="
    echo_info "✅ Test PASSED"
    echo_info "=========================================="
else
    echo_error "=========================================="
    echo_error "❌ Test FAILED (exit code: $TEST_EXIT_CODE)"
    echo_error "=========================================="
fi

# Cleanup
echo ""
echo_info "Cleaning up toxiproxy state..."
curl -s -X POST http://localhost:8474/reset > /dev/null
echo_info "Cleanup complete"

exit $TEST_EXIT_CODE

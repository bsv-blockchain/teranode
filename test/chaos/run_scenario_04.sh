#!/bin/bash

# Chaos Test Scenario 04: Intermittent Connection Drops
# This script runs the intermittent connection drops chaos test with proper setup validation

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
TOXIPROXY_POSTGRES_URL="http://localhost:8474"
TOXIPROXY_KAFKA_URL="http://localhost:8475"
POSTGRES_DIRECT_URL="localhost:5432"
POSTGRES_TOXI_URL="localhost:15432"
KAFKA_DIRECT_URL="localhost:9092"
KAFKA_TOXI_URL="localhost:19092"
COMPOSE_FILE="../compose/docker-compose-ss.yml"

echo -e "${GREEN}[INFO]${NC} Checking if toxiproxy services are running..."

# Check if toxiproxy-postgres is available
if curl -s -f "${TOXIPROXY_POSTGRES_URL}/version" > /dev/null 2>&1; then
    echo -e "${GREEN}[INFO]${NC} Toxiproxy for PostgreSQL is available at ${TOXIPROXY_POSTGRES_URL}"
else
    echo -e "${RED}[ERROR]${NC} Toxiproxy for PostgreSQL is not accessible at ${TOXIPROXY_POSTGRES_URL}"
    echo -e "${GREEN}[INFO]${NC} Check with: docker ps | grep toxiproxy-postgres"
    exit 1
fi

# Check if toxiproxy-kafka is available
if curl -s -f "${TOXIPROXY_KAFKA_URL}/version" > /dev/null 2>&1; then
    echo -e "${GREEN}[INFO]${NC} Toxiproxy for Kafka is available at ${TOXIPROXY_KAFKA_URL}"
else
    echo -e "${RED}[ERROR]${NC} Toxiproxy for Kafka is not accessible at ${TOXIPROXY_KAFKA_URL}"
    echo -e "${GREEN}[INFO]${NC} Check with: docker ps | grep toxiproxy-kafka"
    exit 1
fi

# Check if PostgreSQL is accessible
echo -e "${GREEN}[INFO]${NC} Checking PostgreSQL..."
if nc -z localhost 5432 2>/dev/null; then
    echo -e "${GREEN}[INFO]${NC} PostgreSQL is accessible"
else
    echo -e "${RED}[ERROR]${NC} PostgreSQL is not accessible on ${POSTGRES_DIRECT_URL}"
    echo -e "${GREEN}[INFO]${NC} Check with: docker ps | grep postgres"
    exit 1
fi

# Check if PostgreSQL through toxiproxy is accessible
echo -e "${GREEN}[INFO]${NC} Checking PostgreSQL through toxiproxy..."
if nc -z localhost 15432 2>/dev/null; then
    echo -e "${GREEN}[INFO]${NC} PostgreSQL through toxiproxy is accessible"
else
    echo -e "${RED}[ERROR]${NC} PostgreSQL through toxiproxy is not accessible on ${POSTGRES_TOXI_URL}"
    exit 1
fi

# Check if Kafka is accessible
echo -e "${GREEN}[INFO]${NC} Checking Kafka..."
if nc -z localhost 9092 2>/dev/null; then
    echo -e "${GREEN}[INFO]${NC} Kafka is accessible"
else
    echo -e "${RED}[ERROR]${NC} Kafka is not accessible on ${KAFKA_DIRECT_URL}"
    echo -e "${GREEN}[INFO]${NC} Check with: docker ps | grep kafka"
    exit 1
fi

# Check if Kafka through toxiproxy is accessible
echo -e "${GREEN}[INFO]${NC} Checking Kafka through toxiproxy..."
if nc -z localhost 19092 2>/dev/null; then
    echo -e "${GREEN}[INFO]${NC} Kafka through toxiproxy is accessible"
else
    echo -e "${RED}[ERROR]${NC} Kafka through toxiproxy is not accessible on ${KAFKA_TOXI_URL}"
    exit 1
fi

# Reset toxiproxy services to clean state
echo -e "${GREEN}[INFO]${NC} Resetting toxiproxy services to clean state..."
curl -s -X DELETE "${TOXIPROXY_POSTGRES_URL}/proxies/postgres/toxics" > /dev/null || true
curl -s -X DELETE "${TOXIPROXY_KAFKA_URL}/proxies/kafka/toxics" > /dev/null || true
curl -s -X POST "${TOXIPROXY_POSTGRES_URL}/proxies/postgres" -H "Content-Type: application/json" -d '{"enabled": true}' > /dev/null || true
curl -s -X POST "${TOXIPROXY_KAFKA_URL}/proxies/kafka" -H "Content-Type: application/json" -d '{"enabled": true}' > /dev/null || true

echo ""
echo -e "${GREEN}[INFO]${NC} =================================================="
echo -e "${GREEN}[INFO]${NC} Running Scenario 4: Intermittent Connection Drops"
echo -e "${GREEN}[INFO]${NC} =================================================="
echo ""

# Run the test
go test -v ./test/chaos -run TestScenario04_IntermittentDrops

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

# Cleanup: Reset toxiproxy to clean state
echo -e "${GREEN}[INFO]${NC} Cleaning up toxiproxy state..."
curl -s -X DELETE "${TOXIPROXY_POSTGRES_URL}/proxies/postgres/toxics" > /dev/null || true
curl -s -X DELETE "${TOXIPROXY_KAFKA_URL}/proxies/kafka/toxics" > /dev/null || true
curl -s -X POST "${TOXIPROXY_POSTGRES_URL}/proxies/postgres" -H "Content-Type: application/json" -d '{"enabled": true}' > /dev/null || true
curl -s -X POST "${TOXIPROXY_KAFKA_URL}/proxies/kafka" -H "Content-Type: application/json" -d '{"enabled": true}' > /dev/null || true
echo -e "${GREEN}[INFO]${NC} Cleanup complete"

exit $TEST_EXIT_CODE

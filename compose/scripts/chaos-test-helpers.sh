#!/bin/bash
# Toxiproxy Chaos Testing Helper Scripts
# These scripts help you inject various failure scenarios into your Teranode setup

TOXIPROXY_POSTGRES_API="http://localhost:8474"
TOXIPROXY_KAFKA_API="http://localhost:8475"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

echo_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

echo_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if toxiproxy is running
check_toxiproxy() {
    if ! curl -s "${TOXIPROXY_POSTGRES_API}/version" > /dev/null 2>&1; then
        echo_error "Toxiproxy is not running. Start it with: docker compose -f compose/docker-compose-ss.yml up -d toxiproxy-postgres toxiproxy-kafka"
        exit 1
    fi
    echo_info "Toxiproxy is running"
}

# ==============================================================================
# POSTGRES CHAOS SCENARIOS
# ==============================================================================

# Add latency to PostgreSQL connections
postgres_add_latency() {
    local latency=${1:-1000}  # Default 1000ms
    echo_info "Adding ${latency}ms latency to PostgreSQL connections..."
    curl -s -X POST "${TOXIPROXY_POSTGRES_API}/proxies/postgres/toxics" \
        -H "Content-Type: application/json" \
        -d "{\"name\":\"latency_downstream\",\"type\":\"latency\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"latency\":${latency}}}" | jq .
    echo_info "PostgreSQL latency added. All DB queries will be delayed by ${latency}ms"
}

# Slow down PostgreSQL connection (bandwidth limit)
postgres_limit_bandwidth() {
    local rate=${1:-100}  # Default 100 KB/s
    echo_info "Limiting PostgreSQL bandwidth to ${rate} KB/s..."
    curl -s -X POST "${TOXIPROXY_POSTGRES_API}/proxies/postgres/toxics" \
        -H "Content-Type: application/json" \
        -d "{\"name\":\"slow_bandwidth\",\"type\":\"bandwidth\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"rate\":${rate}}}" | jq .
    echo_info "PostgreSQL bandwidth limited to ${rate} KB/s"
}

# Simulate PostgreSQL connection drops
postgres_connection_drop() {
    local probability=${1:-0.5}  # Default 50% chance
    echo_info "Simulating PostgreSQL connection drops (${probability} probability)..."
    curl -s -X POST "${TOXIPROXY_POSTGRES_API}/proxies/postgres/toxics" \
        -H "Content-Type: application/json" \
        -d "{\"name\":\"connection_drop\",\"type\":\"timeout\",\"stream\":\"downstream\",\"toxicity\":${probability},\"attributes\":{\"timeout\":0}}" | jq .
    echo_info "PostgreSQL connections will randomly drop with ${probability} probability"
}

# Completely disable PostgreSQL proxy (simulate total DB failure)
postgres_disable() {
    echo_warn "Disabling PostgreSQL proxy (simulating total DB failure)..."
    curl -s -X POST "${TOXIPROXY_POSTGRES_API}/proxies/postgres" \
        -H "Content-Type: application/json" \
        -d '{"enabled":false}' | jq .
    echo_error "PostgreSQL is now OFFLINE. Services will fail to connect to the database."
}

# Re-enable PostgreSQL proxy
postgres_enable() {
    echo_info "Re-enabling PostgreSQL proxy..."
    curl -s -X POST "${TOXIPROXY_POSTGRES_API}/proxies/postgres" \
        -H "Content-Type: application/json" \
        -d '{"enabled":true}' | jq .
    echo_info "PostgreSQL is now ONLINE"
}

# Reset all PostgreSQL toxics
postgres_reset() {
    echo_info "Resetting all PostgreSQL toxics..."
    curl -s "${TOXIPROXY_POSTGRES_API}/proxies/postgres/toxics" | jq -r '.[].name' | while read toxic; do
        curl -s -X DELETE "${TOXIPROXY_POSTGRES_API}/proxies/postgres/toxics/${toxic}"
    done
    postgres_enable
    echo_info "PostgreSQL toxics cleared and proxy re-enabled"
}

# ==============================================================================
# KAFKA CHAOS SCENARIOS
# ==============================================================================

# Add latency to Kafka connections
kafka_add_latency() {
    local latency=${1:-1000}  # Default 1000ms
    echo_info "Adding ${latency}ms latency to Kafka connections..."
    curl -s -X POST "${TOXIPROXY_KAFKA_API}/proxies/kafka/toxics" \
        -H "Content-Type: application/json" \
        -d "{\"name\":\"latency_downstream\",\"type\":\"latency\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"latency\":${latency}}}" | jq .
    echo_info "Kafka latency added. All Kafka operations will be delayed by ${latency}ms"
}

# Slow down Kafka bandwidth
kafka_limit_bandwidth() {
    local rate=${1:-100}  # Default 100 KB/s
    echo_info "Limiting Kafka bandwidth to ${rate} KB/s..."
    curl -s -X POST "${TOXIPROXY_KAFKA_API}/proxies/kafka/toxics" \
        -H "Content-Type: application/json" \
        -d "{\"name\":\"slow_bandwidth\",\"type\":\"bandwidth\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"rate\":${rate}}}" | jq .
    echo_info "Kafka bandwidth limited to ${rate} KB/s"
}

# Simulate Kafka connection drops
kafka_connection_drop() {
    local probability=${1:-0.5}  # Default 50% chance
    echo_info "Simulating Kafka connection drops (${probability} probability)..."
    curl -s -X POST "${TOXIPROXY_KAFKA_API}/proxies/kafka/toxics" \
        -H "Content-Type: application/json" \
        -d "{\"name\":\"connection_drop\",\"type\":\"timeout\",\"stream\":\"downstream\",\"toxicity\":${probability},\"attributes\":{\"timeout\":0}}" | jq .
    echo_info "Kafka connections will randomly drop with ${probability} probability"
}

# Completely disable Kafka proxy (simulate total Kafka failure)
kafka_disable() {
    echo_warn "Disabling Kafka proxy (simulating total Kafka failure)..."
    curl -s -X POST "${TOXIPROXY_KAFKA_API}/proxies/kafka" \
        -H "Content-Type: application/json" \
        -d '{"enabled":false}' | jq .
    echo_error "Kafka is now OFFLINE. Services will fail to produce/consume messages."
}

# Re-enable Kafka proxy
kafka_enable() {
    echo_info "Re-enabling Kafka proxy..."
    curl -s -X POST "${TOXIPROXY_KAFKA_API}/proxies/kafka" \
        -H "Content-Type: application/json" \
        -d '{"enabled":true}' | jq .
    echo_info "Kafka is now ONLINE"
}

# Reset all Kafka toxics
kafka_reset() {
    echo_info "Resetting all Kafka toxics..."
    curl -s "${TOXIPROXY_KAFKA_API}/proxies/kafka/toxics" | jq -r '.[].name' | while read toxic; do
        curl -s -X DELETE "${TOXIPROXY_KAFKA_API}/proxies/kafka/toxics/${toxic}"
    done
    kafka_enable
    echo_info "Kafka toxics cleared and proxy re-enabled"
}

# ==============================================================================
# COMBINED SCENARIOS
# ==============================================================================

# Reset everything
reset_all() {
    echo_info "Resetting all toxiproxy configurations..."
    postgres_reset
    kafka_reset
    echo_info "All toxiproxy configurations reset"
}

# Simulate network partition (both services offline)
simulate_network_partition() {
    echo_warn "Simulating complete network partition..."
    postgres_disable
    kafka_disable
    echo_error "NETWORK PARTITION SIMULATED: Both PostgreSQL and Kafka are offline"
}

# List all active toxics
list_toxics() {
    echo_info "Active PostgreSQL toxics:"
    curl -s "${TOXIPROXY_POSTGRES_API}/proxies/postgres/toxics" | jq .
    echo ""
    echo_info "Active Kafka toxics:"
    curl -s "${TOXIPROXY_KAFKA_API}/proxies/kafka/toxics" | jq .
}

# ==============================================================================
# USAGE EXAMPLES
# ==============================================================================

show_usage() {
    cat << EOF
Toxiproxy Chaos Testing Helper

Usage: $0 [command] [args]

Commands:
  PostgreSQL:
    postgres_add_latency [ms]         Add latency to PostgreSQL (default: 1000ms)
    postgres_limit_bandwidth [KB/s]   Limit PostgreSQL bandwidth (default: 100 KB/s)
    postgres_connection_drop [prob]   Random connection drops (default: 0.5)
    postgres_disable                  Completely disable PostgreSQL
    postgres_enable                   Re-enable PostgreSQL
    postgres_reset                    Reset all PostgreSQL toxics

  Kafka:
    kafka_add_latency [ms]            Add latency to Kafka (default: 1000ms)
    kafka_limit_bandwidth [KB/s]      Limit Kafka bandwidth (default: 100 KB/s)
    kafka_connection_drop [prob]      Random connection drops (default: 0.5)
    kafka_disable                     Completely disable Kafka
    kafka_enable                      Re-enable Kafka
    kafka_reset                       Reset all Kafka toxics

  Combined:
    reset_all                         Reset all toxics and re-enable everything
    simulate_network_partition        Disable both PostgreSQL and Kafka
    list_toxics                       List all active toxics

Examples:
  # Add 2 second latency to PostgreSQL
  $0 postgres_add_latency 2000

  # Limit Kafka bandwidth to 50 KB/s
  $0 kafka_limit_bandwidth 50

  # Simulate 80% chance of connection drops for PostgreSQL
  $0 postgres_connection_drop 0.8

  # Simulate complete network partition
  $0 simulate_network_partition

  # Reset everything back to normal
  $0 reset_all

Note: To use these chaos scenarios with your services, uncomment the toxiproxy
      settings in settings_local.conf and restart your services.
EOF
}

# ==============================================================================
# MAIN
# ==============================================================================

if [ $# -eq 0 ]; then
    show_usage
    exit 0
fi

check_toxiproxy

# Execute the requested command
case "$1" in
    postgres_add_latency)
        postgres_add_latency "$2"
        ;;
    postgres_limit_bandwidth)
        postgres_limit_bandwidth "$2"
        ;;
    postgres_connection_drop)
        postgres_connection_drop "$2"
        ;;
    postgres_disable)
        postgres_disable
        ;;
    postgres_enable)
        postgres_enable
        ;;
    postgres_reset)
        postgres_reset
        ;;
    kafka_add_latency)
        kafka_add_latency "$2"
        ;;
    kafka_limit_bandwidth)
        kafka_limit_bandwidth "$2"
        ;;
    kafka_connection_drop)
        kafka_connection_drop "$2"
        ;;
    kafka_disable)
        kafka_disable
        ;;
    kafka_enable)
        kafka_enable
        ;;
    kafka_reset)
        kafka_reset
        ;;
    reset_all)
        reset_all
        ;;
    simulate_network_partition)
        simulate_network_partition
        ;;
    list_toxics)
        list_toxics
        ;;
    *)
        echo_error "Unknown command: $1"
        echo ""
        show_usage
        exit 1
        ;;
esac

# Toxiproxy Chaos Testing Guide

This guide explains how to use Toxiproxy for chaos testing with your Teranode deployment.

## What is Toxiproxy?

Toxiproxy is a framework for simulating network conditions. It's a TCP proxy that sits between your services and their dependencies (PostgreSQL, Kafka), allowing you to inject various failure scenarios:

- Network latency
- Connection timeouts
- Bandwidth limitations
- Random connection drops
- Complete service failures

## Quick Start

### 1. Start Services with Toxiproxy

```bash
# Start all services including toxiproxy
cd ubsv
docker compose -f compose/docker-compose-ss.yml up -d
```

This will start:
- `toxiproxy-postgres` - Proxy for PostgreSQL (API: port 8474, Proxy: port 15432)
- `toxiproxy-kafka` - Proxy for Kafka (API: port 8475, Proxy: port 19092)

### 2. Enable Toxiproxy Routing (Optional)

By default, services connect directly to PostgreSQL and Kafka. To route traffic through toxiproxy:

Edit `compose/settings_local.conf` and uncomment these lines:

```conf
# Route PostgreSQL traffic through toxiproxy
utxostore.docker.ss.teranode1 = postgres://miner1:miner1@toxiproxy-postgres:15432/teranode1
blockchain_store.docker.ss.teranode1 = postgres://miner1:miner1@toxiproxy-postgres:15432/teranode1

# Route Kafka traffic through toxiproxy
kafka_hosts.docker.ss.teranode1 = toxiproxy-kafka:19092
```

Then restart your services:

```bash
docker compose -f compose/docker-compose-ss.yml restart blockchain-1 validator-1 propagation-1
```

### 3. Run Chaos Scenarios

Use the helper script to inject failures:

```bash
cd compose

# View all available commands
./chaos-test-helpers.sh

# Add 2 second latency to PostgreSQL
./chaos-test-helpers.sh postgres_add_latency 2000

# Limit Kafka bandwidth to 50 KB/s
./chaos-test-helpers.sh kafka_limit_bandwidth 50

# Simulate 80% chance of connection drops
./chaos-test-helpers.sh postgres_connection_drop 0.8

# Reset everything back to normal
./chaos-test-helpers.sh reset_all
```

## Testing Scenarios

### Scenario 1: Database Latency

Test how your system handles slow database responses:

```bash
# Add 5 second latency to all PostgreSQL queries
./chaos-test-helpers.sh postgres_add_latency 5000

# Watch your services - they should handle timeouts gracefully
docker logs -f blockchain-1

# Reset when done
./chaos-test-helpers.sh postgres_reset
```

**Expected behavior:**
- Services should timeout gracefully
- Retry logic should kick in
- Circuit breakers should trigger if implemented

### Scenario 2: Kafka Broker Failure

Test resilience when Kafka goes down:

```bash
# Completely disable Kafka
./chaos-test-helpers.sh kafka_disable

# Services should handle this gracefully (queue messages, retry, etc.)
docker logs -f validator-1

# Re-enable Kafka
./chaos-test-helpers.sh kafka_enable
```

**Expected behavior:**
- Services should buffer messages or fail gracefully
- Once Kafka is back, services should reconnect and resume

### Scenario 3: Network Partition

Simulate a complete network failure:

```bash
# Disable both PostgreSQL and Kafka
./chaos-test-helpers.sh simulate_network_partition

# Watch how services handle total isolation
docker ps

# Restore everything
./chaos-test-helpers.sh reset_all
```

**Expected behavior:**
- Services should enter degraded mode
- Health checks should fail
- Services should not crash

### Scenario 4: Intermittent Failures

Test handling of flaky connections:

```bash
# 50% chance of connection drops for PostgreSQL
./chaos-test-helpers.sh postgres_connection_drop 0.5

# Services should retry failed operations
docker logs -f blockchain-1

# Reset
./chaos-test-helpers.sh postgres_reset
```

**Expected behavior:**
- Retry logic should handle intermittent failures
- Services should eventually succeed
- No data corruption

### Scenario 5: Bandwidth Constraints

Test behavior under network congestion:

```bash
# Limit PostgreSQL bandwidth to 10 KB/s
./chaos-test-helpers.sh postgres_limit_bandwidth 10

# Limit Kafka bandwidth to 10 KB/s
./chaos-test-helpers.sh kafka_limit_bandwidth 10

# Services should slow down but continue working
docker stats

# Reset
./chaos-test-helpers.sh reset_all
```

**Expected behavior:**
- Operations complete but more slowly
- No timeouts if within configured limits
- Graceful degradation

## Manual Toxiproxy API Usage

You can also use the Toxiproxy API directly:

### List all proxies
```bash
curl http://localhost:8474/proxies | jq .
```

### List active toxics for PostgreSQL
```bash
curl http://localhost:8474/proxies/postgres/toxics | jq .
```

### Add custom toxic
```bash
curl -X POST http://localhost:8474/proxies/postgres/toxics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my_latency",
    "type": "latency",
    "stream": "downstream",
    "toxicity": 1.0,
    "attributes": {
      "latency": 3000,
      "jitter": 500
    }
  }'
```

### Remove a toxic
```bash
curl -X DELETE http://localhost:8474/proxies/postgres/toxics/my_latency
```

## Available Toxic Types

### 1. Latency
Adds delay to connections
```json
{
  "type": "latency",
  "attributes": {
    "latency": 1000,  // milliseconds
    "jitter": 100     // random jitter ±100ms
  }
}
```

### 2. Bandwidth
Limits connection bandwidth
```json
{
  "type": "bandwidth",
  "attributes": {
    "rate": 100  // KB/s
  }
}
```

### 3. Slow Close
Delays connection closing
```json
{
  "type": "slow_close",
  "attributes": {
    "delay": 1000  // milliseconds
  }
}
```

### 4. Timeout
Stops connections after a timeout
```json
{
  "type": "timeout",
  "attributes": {
    "timeout": 1000  // milliseconds (0 = immediate)
  }
}
```

### 5. Slicer
Slices data into smaller packets
```json
{
  "type": "slicer",
  "attributes": {
    "average_size": 64,  // bytes
    "size_variation": 32,
    "delay": 10          // milliseconds between packets
  }
}
```

### 6. Limit Data
Closes connection after certain amount of data
```json
{
  "type": "limit_data",
  "attributes": {
    "bytes": 1024  // bytes before closing
  }
}
```

## Testing Workflow

### 1. Baseline Testing
Run your normal test suite without toxics to establish baseline metrics.

### 2. Inject Failures
Apply toxics one at a time and observe behavior:
- Check logs for errors
- Monitor metrics (Prometheus/Grafana)
- Verify data consistency
- Check service health endpoints

### 3. Recovery Testing
After injecting failures:
- Remove toxics
- Verify services recover
- Check that no data was lost
- Ensure system returns to healthy state

### 4. Document Findings
Record:
- Which failures were handled gracefully
- Which caused cascading failures
- Where retry logic helped
- Where circuit breakers triggered
- Any data consistency issues

## Integration with Tests

You can integrate toxiproxy into your automated tests:

```bash
# Example test script
#!/bin/bash

# Start services
docker compose -f compose/docker-compose-ss.yml up -d

# Run tests with normal conditions
go test ./test/e2e/...

# Inject database latency
./compose/chaos-test-helpers.sh postgres_add_latency 2000

# Run tests again - should still pass but slower
go test ./test/e2e/...

# Reset and verify recovery
./compose/chaos-test-helpers.sh reset_all
sleep 5

# Final tests should be fast again
go test ./test/e2e/...
```

## Monitoring During Chaos Tests

### Prometheus Metrics
View metrics at http://localhost:19090

Key metrics to watch:
- Request latency
- Error rates
- Connection pool stats
- Retry attempts

### Grafana Dashboards
View dashboards at http://localhost:3000

Monitor:
- Service health
- Database query times
- Kafka lag
- Resource usage

### Jaeger Tracing
View traces at http://localhost:16686

Look for:
- Failed spans
- Timeout errors
- Retry patterns

## Troubleshooting

### Toxiproxy not starting
```bash
# Check logs
docker logs toxiproxy-postgres
docker logs toxiproxy-kafka

# Verify config file
cat compose/toxiproxy-config.json
```

### Services not using toxiproxy
```bash
# Verify settings_local.conf is uncommented
cat compose/settings_local.conf | grep toxiproxy

# Restart services
docker compose -f compose/docker-compose-ss.yml restart
```

### Can't connect to toxiproxy API
```bash
# Check if ports are exposed
docker ps | grep toxiproxy

# Test connection
curl http://localhost:8474/version
curl http://localhost:8475/version
```

### Proxies not forwarding traffic
```bash
# Check proxy status
curl http://localhost:8474/proxies/postgres | jq .

# Verify upstream is reachable from toxiproxy container
docker exec toxiproxy-postgres nc -zv postgres 5432
```

## Best Practices

1. **Start Simple**: Begin with mild failures (small latency) before severe ones
2. **One at a Time**: Test one failure mode at a time to isolate effects
3. **Monitor Everything**: Use Prometheus, Grafana, and logs during tests
4. **Reset Between Tests**: Always reset toxics to avoid compounding effects
5. **Document Results**: Keep track of which scenarios cause issues
6. **Automate**: Create scripts for common test scenarios
7. **Test Recovery**: Always verify system recovers after removing toxics

## Advanced Usage

### Asymmetric Toxics
Apply different toxics to upstream vs downstream:

```bash
# Slow reads but fast writes
curl -X POST http://localhost:8474/proxies/postgres/toxics \
  -d '{"type":"latency","stream":"downstream","attributes":{"latency":2000}}'
```

### Probabilistic Toxics
Use toxicity parameter for random behavior:

```bash
# 30% chance of adding latency
curl -X POST http://localhost:8474/proxies/postgres/toxics \
  -d '{"type":"latency","toxicity":0.3,"attributes":{"latency":2000}}'
```

### Chaining Toxics
Apply multiple toxics to same proxy:

```bash
# Both latency AND bandwidth limit
./chaos-test-helpers.sh postgres_add_latency 1000
./chaos-test-helpers.sh postgres_limit_bandwidth 50
```

## References

- [Toxiproxy GitHub](https://github.com/Shopify/toxiproxy)
- [Toxiproxy API Documentation](https://github.com/Shopify/toxiproxy#http-api)
- [Chaos Engineering Principles](https://principlesofchaos.org/)

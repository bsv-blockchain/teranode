# Chaos Testing Suite

This directory contains chaos engineering tests for Teranode using Toxiproxy.

## Overview

Chaos tests verify system resilience by intentionally injecting failures and observing how the system responds. These tests use [Toxiproxy](https://github.com/Shopify/toxiproxy) to simulate various network conditions and infrastructure failures.

## Prerequisites

1. **Docker Compose with Toxiproxy**
   ```bash
   # Start services including toxiproxy
   docker compose -f compose/docker-compose-ss.yml up -d
   ```

2. **PostgreSQL accessible through toxiproxy**
   - Direct: `localhost:5432`
   - Via toxiproxy: `localhost:15432`
   - Toxiproxy API: `localhost:8474`

3. **Kafka accessible through toxiproxy**
   - Direct: `localhost:9092`
   - Via toxiproxy: `localhost:19092`
   - Toxiproxy API: `localhost:8475`

## Running Tests

### Run All Chaos Tests
```bash
cd /Users/etbdvaa/ubsv
go test -v ./test/chaos/...
```

### Run Specific Scenario

**Using helper scripts (recommended):**
```bash
# Scenario 1: Database Latency
./test/chaos/run_scenario_01.sh

# Scenario 2: Kafka Broker Failure
./test/chaos/run_scenario_02.sh
```

The helper scripts will:
- Check if required services are running
- Start docker compose if needed
- Verify connectivity through toxiproxy
- Reset toxiproxy to clean state
- Run the test
- Clean up after completion

**Using go test directly:**
```bash
# Scenario 1: Database Latency
go test -v ./test/chaos -run TestScenario01_DatabaseLatency

# Scenario 2: Kafka Broker Failure
go test -v ./test/chaos -run TestScenario02_KafkaBrokerFailure
```

### Run in Verbose Mode
```bash
go test -v -count=1 ./test/chaos/... -test.v
```

### Skip Chaos Tests (Short Mode)
```bash
go test -short ./test/chaos/...
```

## Test Scenarios

### Scenario 1: Database Latency
**File:** `scenario_01_database_latency_test.go`

**What it tests:**
- System behavior under slow database responses
- Timeout handling and retry logic
- Recovery after latency removal
- Data consistency during failures

**How to run:**
```bash
go test -v ./test/chaos -run TestScenario01_DatabaseLatency
```

**Test phases:**
1. Establish baseline performance
2. Inject 5-second latency via toxiproxy
3. Verify timeout behavior
4. Test retry mechanisms
5. Remove latency and verify recovery
6. Confirm data consistency

**Expected results:**
- ✅ Queries timeout gracefully when latency exceeds timeout
- ✅ Queries succeed (slowly) when timeout is sufficient
- ✅ Retry logic executes multiple attempts
- ✅ System recovers fully after latency removal
- ✅ No data corruption

### Scenario 2: Kafka Broker Failure
**File:** `scenario_02_kafka_broker_failure_test.go`

**What it tests:**
- System behavior when Kafka broker is unavailable
- Producer error handling and retry logic
- Consumer watchdog detection of stuck states
- Message delivery guarantees
- Recovery after broker restoration
- Offset management and message consistency

**How to run:**
```bash
go test -v ./test/chaos -run TestScenario02_KafkaBrokerFailure
```

**Test phases:**
1. Establish baseline Kafka operations (produce and consume)
2. Inject 3-second latency to simulate slow broker
3. Test sync producer with latency
4. Test async producer with latency
5. Inject 100% connection drops (simulate broker failure)
6. Verify producer failure handling
7. Verify consumer watchdog detects stuck state
8. Remove toxic and verify recovery
9. Verify message consistency and no data loss

**Expected results:**
- ✅ Producers handle latency gracefully (slow but successful)
- ✅ Async producers continue operating with latency
- ✅ Producers fail appropriately when broker is down
- ✅ Consumer watchdog detects stuck/failed connections
- ✅ System recovers fully after broker restoration
- ✅ No message loss (all published messages are retrievable)
- ✅ Consumer offsets maintained correctly

## Test Structure

Each chaos test follows this pattern:

```go
func TestScenarioXX_Name(t *testing.T) {
    // Skip in short mode
    if testing.Short() {
        t.Skip("Skipping chaos test in short mode")
    }

    // 1. Setup toxiproxy client
    toxiClient := NewToxiproxyClient(toxiproxyURL)

    // 2. Reset proxy to clean state
    toxiClient.ResetProxy(proxyName)
    defer toxiClient.ResetProxy(proxyName) // Cleanup

    // 3. Establish baseline
    t.Run("Baseline", func(t *testing.T) {
        // Test normal behavior
    })

    // 4. Inject failure
    t.Run("Inject_Failure", func(t *testing.T) {
        // Add toxic
        toxiClient.AddLatency(...)
    })

    // 5. Test under failure
    t.Run("Behavior_Under_Failure", func(t *testing.T) {
        // Verify graceful degradation
    })

    // 6. Remove failure and verify recovery
    t.Run("Recovery", func(t *testing.T) {
        toxiClient.RemoveToxic(...)
        // Verify system recovers
    })

    // 7. Verify consistency
    t.Run("Consistency", func(t *testing.T) {
        // Check no data corruption
    })
}
```

## Toxiproxy Client API

The `toxiproxy_client.go` provides a Go client for the Toxiproxy API:

### Available Methods

```go
// Connection
client := NewToxiproxyClient("http://localhost:8474")
client.WaitForProxy("postgres", 30*time.Second)

// Add toxics
client.AddLatency("postgres", 5000, "downstream")      // 5s latency
client.AddBandwidthLimit("kafka", 100, "downstream")  // 100 KB/s
client.AddTimeout("postgres", 0, 0.5, "downstream")   // 50% drop rate

// Remove toxics
client.RemoveToxic("postgres", "latency_downstream")
client.RemoveAllToxics("postgres")

// Control proxy
client.EnableProxy("postgres")
client.DisableProxy("postgres")
client.ResetProxy("postgres")  // Remove all toxics + enable

// Query state
toxics, _ := client.ListToxics("postgres")
proxy, _ := client.GetProxy("postgres")
```

### Toxic Types

1. **Latency** - Adds delay
   ```go
   client.AddLatency("postgres", 1000, "downstream") // 1 second
   ```

2. **Bandwidth** - Limits throughput
   ```go
   client.AddBandwidthLimit("kafka", 50, "downstream") // 50 KB/s
   ```

3. **Timeout** - Drops connections
   ```go
   client.AddTimeout("postgres", 0, 0.3, "downstream") // 30% drop
   ```

4. **Slicer** - Slows data transmission
   ```go
   client.AddSlicer("kafka", 64, 32, 10, "downstream")
   ```

## Writing New Chaos Tests

### 1. Create Test File
```bash
touch test/chaos/scenario_XX_name_test.go
```

### 2. Follow Template
```go
package chaos

import (
    "testing"
    "github.com/stretchr/testify/require"
)

func TestScenarioXX_Name(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping chaos test in short mode")
    }

    toxiClient := NewToxiproxyClient("http://localhost:8474")
    defer toxiClient.ResetProxy("postgres")

    // Your test phases here
}
```

### 3. Test Phases
- **Baseline**: Measure normal behavior
- **Inject**: Add toxic(s)
- **Observe**: Verify graceful degradation
- **Recover**: Remove toxic(s)
- **Verify**: Confirm full recovery + consistency

### 4. Assertions
Use `require` for critical checks:
```go
require.NoError(t, err, "operation should succeed")
require.Less(t, duration, timeout, "should complete within timeout")
require.Equal(t, expected, actual, "should match expected value")
```

Use `t.Logf` for progress updates:
```go
t.Logf("✓ Baseline completed in %v", duration)
t.Logf("⚠ Injected 5s latency")
t.Logf("✅ Test completed successfully")
```

## Continuous Integration

### In CI/CD Pipeline
```yaml
# .github/workflows/chaos-tests.yml
- name: Run Chaos Tests
  run: |
    docker compose -f compose/docker-compose-ss.yml up -d
    sleep 10  # Wait for services
    go test -v ./test/chaos/...
    docker compose -f compose/docker-compose-ss.yml down
```

### Test Duration
Chaos tests take longer than unit tests:
- Scenario 1 (Database Latency): ~30-45 seconds
- Scenario 2 (Kafka Broker Failure): ~40-60 seconds
- Full suite: ~2-3 minutes (grows with more scenarios)

## Troubleshooting

### Toxiproxy Not Available
```bash
# Check if toxiproxy containers are running
docker ps | grep toxiproxy

# Check logs
docker logs toxiproxy-postgres
docker logs toxiproxy-kafka

# Verify API is responding
curl http://localhost:8474/version
curl http://localhost:8475/version
```

### Tests Timing Out
```bash
# Increase test timeout
go test -v -timeout 10m ./test/chaos/...

# Check if services are responding
docker ps
docker logs blockchain-1
```

### Connection Refused
```bash
# Verify ports are exposed
docker compose -f compose/docker-compose-ss.yml ps

# Check firewall isn't blocking ports 8474, 8475, 15432, 19092
```

### Toxics Not Taking Effect
```bash
# Verify toxic was added
curl http://localhost:8474/proxies/postgres/toxics | jq .

# Check proxy is enabled
curl http://localhost:8474/proxies/postgres | jq .enabled

# Reset and try again
curl -X POST http://localhost:8474/reset
```

## Best Practices

1. **Always cleanup**: Use `defer toxiClient.ResetProxy(...)` to ensure cleanup
2. **Test isolation**: Each test should reset toxiproxy at start
3. **Descriptive names**: Use clear test names describing what's being tested
4. **Measure baselines**: Always establish normal behavior first
5. **Verify recovery**: Confirm system returns to healthy state
6. **Check consistency**: Verify no data corruption after failures
7. **Document expectations**: Comment what should happen under each failure
8. **Use subtests**: Break tests into logical phases with `t.Run()`

## Related Documentation

- [Toxiproxy Chaos Testing Guide](../../compose/scripts/toxiproxy-chaos-testing.md)
- [Toxiproxy GitHub](https://github.com/Shopify/toxiproxy)
- [Chaos Engineering Principles](https://principlesofchaos.org/)

## Future Scenarios (Planned)

- [x] Scenario 2: Kafka Broker Failure ✅ **Implemented**
- [ ] Scenario 3: Network Partition
- [ ] Scenario 4: Intermittent Connection Drops
- [ ] Scenario 5: Bandwidth Constraints
- [ ] Scenario 6: Slow Close Connections
- [ ] Scenario 7: Combined Failures (DB + Kafka)
- [ ] Scenario 8: Cascading Failures

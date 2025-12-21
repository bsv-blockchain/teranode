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

# Scenario 3: Network Partition
./test/chaos/run_scenario_03.sh

# Scenario 4: Intermittent Connection Drops
./test/chaos/run_scenario_04.sh

# Scenario 5: Bandwidth Constraints
./test/chaos/run_scenario_05.sh

# Scenario 6: Slow Close Connections (Slicer)
./test/chaos/run_scenario_06.sh

# Scenario 7: Combined Failures (DB + Kafka)
./test/chaos/run_scenario_07.sh

# Scenario 8: Block Validation Memory Pressure
./test/chaos/run_scenario_08.sh
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

# Scenario 3: Network Partition
go test -v ./test/chaos -run TestScenario03_NetworkPartition

# Scenario 4: Intermittent Connection Drops
go test -v ./test/chaos -run TestScenario04_IntermittentDrops

# Scenario 5: Bandwidth Constraints
go test -v ./test/chaos -run TestScenario05

# Scenario 6: Slow Close Connections (Slicer)
go test -v ./test/chaos -run TestScenario06

# Scenario 7: Combined Failures (DB + Kafka)
go test -v ./test/chaos -run TestScenario07

# Scenario 8: Block Validation Memory Pressure
go test -v ./test/chaos -run TestScenario08
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

### Scenario 3: Network Partition
**File:** `scenario_03_network_partition_test.go`

**What it tests:**
- System behavior when network connectivity is lost (partition)
- Both PostgreSQL and Kafka isolation simultaneously
- Service detection of network failures
- Application resilience during extended outages
- Recovery after partition healing
- Data consistency across services after partition

**How to run:**
```bash
# Using helper script (recommended)
./test/chaos/run_scenario_03.sh

# Using go test directly
go test -v ./test/chaos -run TestScenario03_NetworkPartition
```

**Test phases:**
1. Establish baseline connectivity to PostgreSQL and Kafka
2. Simulate network partition (disable both toxiproxy services)
3. Verify both services become unreachable
4. Test application resilience during partition
5. Heal network partition (re-enable proxies)
6. Verify both services recover
7. Confirm data consistency for both services

**Expected results:**
- ✅ Services correctly detected as unreachable during partition
- ✅ No panic or crashes during network failure
- ✅ Connection errors handled gracefully
- ✅ Retry attempts fail consistently during partition
- ✅ Services automatically recover when partition heals
- ✅ No data loss or corruption in either service
- ✅ Both PostgreSQL and Kafka fully operational after recovery

### Scenario 4: Intermittent Connection Drops (3 variants)
**File:** `scenario_04_intermittent_drops_test.go`

This scenario has three test variants that explore different aspects of intermittent network failures:

#### Variant A: Basic Intermittent Drops
**Test:** `TestScenario04_IntermittentDrops`

**What it tests:**
- System behavior under unstable network conditions with random connection drops
- Probabilistic failure handling (30% and 60% drop rates)
- Application retry logic effectiveness under intermittent failures
- Data consistency despite random connection failures
- Both PostgreSQL and Kafka resilience to intermittent drops

**How to run:**
```bash
# Using helper script (recommended)
./test/chaos/run_scenario_04.sh

# Using go test directly
go test -v ./test/chaos -run TestScenario04_IntermittentDrops
```

**Test phases:**
1. Establish baseline connectivity to both services
2. Inject 30% intermittent drops (low toxicity)
3. Test PostgreSQL operations with low drop rate
4. Test Kafka operations with low drop rate
5. Increase to 60% intermittent drops (high toxicity)
6. Validate retry logic under high drop rate
7. Remove drops and verify full recovery
8. Confirm data consistency after intermittent failures

**Expected results:**
- ✅ Some operations fail randomly, others succeed (probabilistic)
- ✅ Success rate correlates with drop rate (70% success at 30% drops)
- ✅ Retry logic improves eventual success rate significantly
- ✅ At least 40% eventual success even with 60% drop rate and retries
- ✅ 100% success rate after drops removed (full recovery)
- ✅ No data corruption despite intermittent failures
- ✅ Both services handle unstable networks gracefully

**Test duration:** ~8 minutes

#### Variant B: Cascading Effects
**Test:** `TestScenario04_CascadingEffects`

**What it tests:**
- How PostgreSQL failures cascade through the microservices mesh
- Impact on critical services (blockchain, block assembly, validator, RPC, asset server)
- Graceful failure handling without cascading crashes
- Circuit breaker and timeout behavior
- Recovery coordination across multiple services

**How to run:**
```bash
go test -v ./test/chaos -run TestScenario04_CascadingEffects
```

**Test phases:**
1. Establish baseline with 5 concurrent services
2. Inject complete PostgreSQL failure (disable proxy)
3. Test cascading failures across all services
4. Verify failure detection timing (fast fail)
5. Restore database and test recovery order
6. Verify data consistency after cascade

**Expected results:**
- ✅ All services fail gracefully when database unavailable
- ✅ No crashes or indefinite hangs
- ✅ Fast failure detection (< 10 seconds)
- ✅ All services recover after database restoration
- ✅ No data corruption after cascading failures

**Test duration:** ~2 seconds

#### Variant C: Load Under Failures
**Test:** `TestScenario04_LoadUnderFailures`

**What it tests:**
- System throughput under network instability with high concurrent load
- Transaction processing success rate with intermittent failures
- Latency distribution under load + failures
- Recovery time under sustained load
- Performance degradation patterns

**How to run:**
```bash
go test -v ./test/chaos -run TestScenario04_LoadUnderFailures
```

**Test phases:**
1. Establish baseline throughput (5 workers × 5 ops)
2. Inject 30% connection drops to both services
3. Measure performance degradation under baseline load
4. Increase load (10 workers) while maintaining failures
5. Remove failures and measure recovery speed
6. Compare performance metrics (throughput, latency, success rate)

**Expected results:**
- ✅ Baseline: 100% success rate, ~7ms avg latency
- ✅ Under 30% failures: 60-65% success rate, ~1.3s avg latency (187x slower)
- ✅ High load + failures: 60-65% success rate maintained
- ✅ Recovery: Returns to 100% success, ~9ms avg latency
- ✅ System maintains partial operation under failures
- ✅ Latency increases but remains bounded (~5s p95)

**Test duration:** ~28 seconds

### Scenario 5: Bandwidth Constraints (2 variants)
**File:** `scenario_05_bandwidth_constraints_test.go`

Tests system behavior under network bandwidth limitations using toxiproxy's bandwidth toxic.

#### Variant A: Database Bandwidth (`TestScenario05_DatabaseBandwidth`)
- Simulates datacenter network congestion (500 KB/s moderate, 100 KB/s heavy)
- Tests PostgreSQL query performance under bandwidth constraints
- Validates connection pooling behavior with slow data transfer
- **Duration:** ~2.1 seconds

#### Variant B: Kafka Bandwidth (`TestScenario05_KafkaBandwidth`)
- Tests Kafka producer/consumer under bandwidth constraints
- Validates message throughput and backpressure handling
- Ensures no message loss despite slow network
- **Duration:** ~2.1 seconds

**Combined duration:** ~4.4 seconds

### Scenario 6: Slow Close Connections (2 variants)
**File:** `scenario_06_slow_close_connections_test.go`

Tests system behavior with slicer toxic, which transmits data in small chunks with delays between each chunk. This simulates slow/unstable connections typical of congested networks or graceful connection draining.

#### Variant A: Database Slow Close
**Test:** `TestScenario06_DatabaseSlowClose`

**What it tests:**
- PostgreSQL queries with chunked result transmission
- Connection behavior during slow data transfer
- Database timeout handling with delayed responses
- System resilience to unstable/slow connections

**How to run:**
```bash
# Using helper script
./test/chaos/run_scenario_06.sh

# Using go test directly
go test -v ./test/chaos -run TestScenario06_DatabaseSlowClose
```

**Test phases:**
1. Establish baseline query performance
2. Apply moderate slicer (1KB chunks, 10ms delay)
3. Test database operations with moderate chunking
4. Apply aggressive slicer (256 byte chunks, 50ms delay)
5. Test database operations with aggressive chunking
6. Remove slicer and verify recovery
7. Verify data consistency

**Expected results:**
- ✅ Baseline queries complete quickly
- ✅ Moderate slicer: Queries slower but complete successfully
- ✅ Aggressive slicer: Significant slowdown but no failures
- ✅ Connection pool handles chunked transmission gracefully
- ✅ Full recovery after slicer removed
- ✅ No data corruption

**Test duration:** ~25 seconds

#### Variant B: Kafka Slow Close
**Test:** `TestScenario06_KafkaSlowClose`

**What it tests:**
- Kafka message transmission with chunked data transfer
- Producer behavior during slow sends
- Consumer behavior during slow fetches
- Message consistency with unstable connections

**How to run:**
```bash
# Using helper script
./test/chaos/run_scenario_06.sh

# Using go test directly
go test -v ./test/chaos -run TestScenario06_KafkaSlowClose
```

**Test phases:**
1. Establish baseline Kafka throughput
2. Apply moderate slicer (1KB chunks, 10ms delay)
3. Test message production/consumption with moderate chunking
4. Apply aggressive slicer (256 byte chunks, 50ms delay)
5. Test message production/consumption with aggressive chunking
6. Remove slicer and verify recovery
7. Verify message consistency (no loss)

**Expected results:**
- ✅ Baseline: Fast message throughput
- ✅ Moderate slicer: Reduced throughput but 80%+ success rate
- ✅ Aggressive slicer: Significant slowdown, at least 60% success
- ✅ No message loss (all published messages eventually consumed)
- ✅ Producer/consumer handle slow transmission gracefully
- ✅ Full recovery after slicer removed

**Test duration:** ~30 seconds

**Combined scenario duration:** ~55 seconds

### Scenario 7: Combined Failures (3 variants)
**File:** `scenario_07_combined_failures_test.go`

Tests system behavior when multiple dependencies fail simultaneously or in sequence. This simulates realistic infrastructure-wide issues like datacenter problems, network partitions, or cascading failures.

#### Variant A: Simultaneous Complete Failure
**Test:** `TestScenario07_SimultaneousFailure`

**What it tests:**
- System behavior when both PostgreSQL AND Kafka fail at the same time
- Failure detection when multiple dependencies down
- Graceful degradation (errors, not crashes)
- Simultaneous recovery of both services
- Data consistency after dual failure

**How to run:**
```bash
# Using helper script
./test/chaos/run_scenario_07.sh

# Using go test directly
go test -v ./test/chaos -run TestScenario07_SimultaneousFailure
```

**Test phases:**
1. Establish baseline with both services healthy
2. Disable both PostgreSQL and Kafka simultaneously (complete failure)
3. Test behavior during simultaneous outage
4. Restore both services simultaneously
5. Verify recovery and data consistency

**Expected results:**
- ✅ Baseline: Both services healthy and functional
- ✅ Simultaneous failure: Both fail quickly and cleanly (no hangs)
- ✅ During outage: Errors returned promptly (not timeouts or crashes)
- ✅ Recovery: Both services restored successfully
- ✅ Consistency: No data corruption from dual failure

**Test duration:** ~10 seconds

#### Variant B: Simultaneous Latency
**Test:** `TestScenario07_SimultaneousLatency`

**What it tests:**
- System behavior when both PostgreSQL AND Kafka become slow simultaneously
- Performance degradation when multiple dependencies affected
- System remains functional despite infrastructure-wide slowdown
- Recovery when latency removed from both

**How to run:**
```bash
# Using helper script
./test/chaos/run_scenario_07.sh

# Using go test directly
go test -v ./test/chaos -run TestScenario07_SimultaneousLatency
```

**Test phases:**
1. Measure baseline performance (both services fast)
2. Inject 500ms latency to both services simultaneously
3. Test performance under simultaneous latency
4. Remove latency and verify recovery

**Expected results:**
- ✅ Baseline: Fast operations on both services
- ✅ With latency: Both services slower but still functional
- ✅ Operations complete successfully despite 500ms delay
- ✅ No cascading timeouts or failures
- ✅ Recovery: Performance returns to baseline levels

**Test duration:** ~15 seconds

#### Variant C: Staggered Recovery
**Test:** `TestScenario07_StaggeredRecovery`

**What it tests:**
- System behavior when services recover at different times
- Partial functionality when one service up, one down
- No cascading failures during staggered recovery
- Data consistency with asynchronous recovery

**How to run:**
```bash
# Using helper script
./test/chaos/run_scenario_07.sh

# Using go test directly
go test -v ./test/chaos -run TestScenario07_StaggeredRecovery
```

**Test phases:**
1. Disable both PostgreSQL and Kafka simultaneously
2. Restore PostgreSQL first (Kafka still down)
3. Verify PostgreSQL works while Kafka remains down
4. Wait 3 seconds, then restore Kafka
5. Verify both services healthy
6. Confirm data consistency after staggered recovery

**Expected results:**
- ✅ Both services fail cleanly when disabled
- ✅ PostgreSQL recovers independently while Kafka down
- ✅ System handles partial recovery gracefully
- ✅ Kafka recovers after delay with no issues
- ✅ No data corruption from staggered recovery pattern

**Test duration:** ~10 seconds

**Combined scenario duration:** ~35 seconds

### Scenario 8: Block Validation Memory Pressure
**File:** `scenario_08_block_validation_memory_test.go`

Tests block validation resilience under various memory pressure scenarios and subtree patterns. This validates that the block validation system can handle extreme cases without panicking or leaking resources.

**What it tests:**
- Block validation with random subtree patterns (transient, deep chains, mixed)
- Memory usage and heap allocations during validation
- Goroutine lifecycle and cleanup
- Cache hit rates and eviction under pressure
- Concurrent validation under load
- `ValidateBlockWithOptions` never panics on any generated subtree
- `setTxMinedStatus` succeeds for all transactions

**How to run:**
```bash
# Using helper script
./test/chaos/run_scenario_08.sh

# Using go test directly
go test -v ./test/chaos -run TestScenario08
```

**Test phases:**

1. **Baseline Metrics**
   - Capture initial heap allocation
   - Record starting goroutine count
   - Force GC for clean baseline

2. **Transient Subtrees (50 blocks)**
   - Generate many small blocks (shallow depth, few TXs)
   - Validate each subtree
   - Monitor heap allocation growth
   - Verify no goroutine leaks
   - Target: < 500 MB heap, < 200 goroutines

3. **Deep Chains (100 blocks)**
   - Generate deep transaction chains (up to 10 levels)
   - Stress caching and eviction mechanisms
   - Monitor cache hit/miss rates
   - Verify all transactions marked as mined
   - Expected: > 50% cache hit rate

4. **Mixed Patterns (30 blocks)**
   - Random TX depth (1-10), TX count (1-1000), TX size (100-10000 bytes)
   - Validate with panic recovery
   - Verify no panics occur under any pattern
   - All validations must succeed

5. **Concurrent Validation (10 concurrent blocks)**
   - Generate and validate 10 subtrees concurrently
   - Monitor goroutine cleanup after completion
   - Verify no goroutine leaks (< 5 increase after cleanup)
   - All concurrent validations must succeed

6. **Cache Eviction Under Pressure (200 blocks)**
   - Generate many blocks to force cache eviction
   - Monitor cache size changes
   - Verify eviction mechanism works
   - Expected: Multiple cache evictions observed

**Expected results:**
- ✅ No panics under any subtree pattern
- ✅ All transactions successfully marked as mined
- ✅ Heap allocation stays under 500 MB
- ✅ Goroutine count stays under 200
- ✅ No goroutine leaks after concurrent validation
- ✅ Cache hit rate > 50% for deep chains
- ✅ Cache eviction works under memory pressure
- ✅ All validations complete within timeout (5s per block)

**Performance metrics tracked:**
- Heap allocations (MB)
- Goroutine count
- Cache hit/miss rates
- Validation times (average)
- Cache size and evictions

**Test duration:** ~45-60 seconds (depending on hardware)

**Note:** This test does NOT use Toxiproxy as it focuses on internal block validation logic rather than external service failures.

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
- Scenario 3 (Network Partition): ~35-50 seconds
- Scenario 4A (Intermittent Drops): ~8 minutes (includes retry logic with delays)
- Scenario 4B (Cascading Effects): ~2 seconds (fast failure detection test)
- Scenario 4C (Load Under Failures): ~28 seconds (load testing under failures)
- Scenario 5 (Bandwidth Constraints): ~4.4 seconds (database + Kafka bandwidth tests)
- Scenario 6 (Slow Close Connections): ~55 seconds (slicer toxic tests)
- Scenario 7 (Combined Failures): ~35 seconds (simultaneous and staggered failures)
- Scenario 8 (Block Validation Memory): ~45-60 seconds (memory pressure and validation tests)
- Full suite: ~14-15 minutes (with all scenarios)

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

## Implemented Scenarios

- [x] Scenario 1: Database Latency ✅ **Implemented**
- [x] Scenario 2: Kafka Broker Failure ✅ **Implemented**
- [x] Scenario 3: Network Partition ✅ **Implemented**
- [x] Scenario 4A: Intermittent Connection Drops ✅ **Implemented**
- [x] Scenario 4B: Cascading Effects ✅ **Implemented**
- [x] Scenario 4C: Load Under Failures ✅ **Implemented**
- [x] Scenario 5: Bandwidth Constraints ✅ **Implemented**
- [x] Scenario 6: Slow Close Connections (Slicer toxic) ✅ **Implemented**
- [x] Scenario 7: Combined Failures (DB + Kafka simultaneously) ✅ **Implemented**
- [x] Scenario 8: Block Validation Memory Pressure ✅ **Implemented**

All chaos test scenarios have been implemented! Total: **8 scenarios** covering network failures, infrastructure issues, and internal system resilience.

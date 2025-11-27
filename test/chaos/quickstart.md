# Chaos Testing Quick Start

Get started with chaos testing in 5 minutes!

## Prerequisites

1. Docker Compose running with toxiproxy:
   ```bash
   docker compose -f compose/docker-compose-ss.yml up -d
   ```

2. PostgreSQL accessible on:
   - Direct: `localhost:5432`
   - Via toxiproxy: `localhost:15432`

## Run Your First Chaos Test

### Option 1: Use the Quick Runner Script

```bash
cd /Users/etbdvaa/ubsv/test/chaos
./run_scenario_01.sh
```

This script will:
- ✅ Check if toxiproxy is running
- ✅ Verify PostgreSQL connectivity
- ✅ Reset toxiproxy to clean state
- ✅ Run Scenario 1 test
- ✅ Cleanup after test

### Option 2: Run Directly with Go

```bash
cd /Users/etbdvaa/ubsv

# Run the test
go test -v ./test/chaos -run TestScenario01_DatabaseLatency

# Or with more verbosity
go test -v -count=1 ./test/chaos -run TestScenario01_DatabaseLatency -test.v
```

## What Happens in Scenario 1?

**Test: Database Latency**

The test simulates slow database responses:

1. **Baseline** (5s)
   - Tests normal database performance
   - Query completes in < 100ms

2. **Inject Latency** (5s)
   - Adds 5000ms latency via toxiproxy
   - All database queries now take 5+ seconds

3. **Test With Latency** (20s)
   - Tests query timeouts (expects failure)
   - Tests slow success with long timeout
   - Tests retry behavior

4. **Recovery** (10s)
   - Removes latency toxic
   - Verifies system returns to normal
   - Confirms queries are fast again

5. **Consistency Check** (5s)
   - Verifies no data corruption
   - Checks database integrity

**Total Duration:** ~45 seconds

## Expected Output

```
=== RUN   TestScenario01_DatabaseLatency
=== RUN   TestScenario01_DatabaseLatency/Baseline_Performance
    ✓ Baseline query completed in 15ms
=== RUN   TestScenario01_DatabaseLatency/Inject_Latency
    ✓ Latency toxic injected successfully
=== RUN   TestScenario01_DatabaseLatency/Query_With_Latency
=== RUN   TestScenario01_DatabaseLatency/Query_With_Latency/Timeout_Failure
    ✓ Query correctly timed out after 3.002s
=== RUN   TestScenario01_DatabaseLatency/Query_With_Latency/Slow_Success
    ✓ Slow query completed successfully in 5.012s
=== RUN   TestScenario01_DatabaseLatency/Recovery
    ✓ System recovered: query completed in 12ms
=== RUN   TestScenario01_DatabaseLatency/Data_Consistency
    ✓ Data consistency verified: no corruption detected
    ✅ Scenario 1 (Database Latency) completed successfully
--- PASS: TestScenario01_DatabaseLatency (45.23s)
PASS
```

## Understanding Test Results

### ✅ Test Passes When:
- Baseline queries are fast (< 100ms)
- Queries timeout correctly when latency > timeout
- Queries succeed (slowly) when timeout is sufficient
- Retry logic executes multiple attempts
- System recovers fully after latency removal
- No data corruption detected

### ❌ Test Fails When:
- Toxiproxy is not running
- Database is not accessible
- Timeouts don't work correctly
- System doesn't recover after latency removal
- Data corruption is detected

## Troubleshooting

### "Toxiproxy is not available"
```bash
# Check if containers are running
docker ps | grep toxiproxy

# Check logs
docker logs toxiproxy-postgres

# Restart if needed
docker compose -f compose/docker-compose-ss.yml restart toxiproxy-postgres
```

### "Connection refused to PostgreSQL"
```bash
# Check PostgreSQL is running
docker ps | grep postgres

# Check logs
docker logs postgres

# Test direct connection
psql "postgres://postgres:really_strong_password_change_me@localhost:5432/postgres" -c "SELECT 1"
```

### "Test timeout"
```bash
# Increase test timeout
go test -v -timeout 10m ./test/chaos -run TestScenario01_DatabaseLatency
```

### "Context deadline exceeded"
This is expected! The test intentionally creates timeouts to verify timeout handling.

## Manual Testing

You can also inject failures manually and test your services:

```bash
# Add 5 second latency
curl -X POST http://localhost:8474/proxies/postgres/toxics \
  -H "Content-Type: application/json" \
  -d '{"name":"test_latency","type":"latency","stream":"downstream","toxicity":1.0,"attributes":{"latency":5000}}'

# Test your application
# ... run your app tests ...

# Remove latency
curl -X DELETE http://localhost:8474/proxies/postgres/toxics/test_latency
```

## Next Steps

1. **Read the full documentation**
   - [test/chaos/README.md](README.md) - Complete testing guide
   - [compose/TOXIPROXY_CHAOS_TESTING.md](../../compose/TOXIPROXY_CHAOS_TESTING.md) - Toxiproxy usage guide

2. **Try other scenarios**
   ```bash
   # When implemented
   ./run_scenario_02.sh  # Kafka failure
   ./run_scenario_03.sh  # Network partition
   ```

3. **Write your own tests**
   - Copy `scenario_01_database_latency_test.go`
   - Modify for your use case
   - Follow the test structure in README.md

4. **Integrate with CI/CD**
   ```yaml
   # .github/workflows/chaos-tests.yml
   - name: Chaos Tests
     run: |
       docker compose -f compose/docker-compose-ss.yml up -d
       sleep 10
       go test -v ./test/chaos/...
   ```

## Chaos Testing Philosophy

> "Chaos Engineering is the discipline of experimenting on a system in order to build confidence in the system's capability to withstand turbulent conditions in production."

Key principles:
1. **Build a Hypothesis** - What should happen when X fails?
2. **Define Steady State** - Measure normal behavior
3. **Inject Real-World Failures** - Latency, timeouts, crashes
4. **Observe** - Does the system behave as expected?
5. **Minimize Blast Radius** - Test safely in controlled environments

## Questions?

- Check [test/chaos/README.md](README.md) for detailed documentation
- Review [compose/scripts/chaos-test-helpers.sh](../../compose/scripts/chaos-test-helpers.sh) for more examples
- See [compose/scripts/toxiproxy-chaos-testing.md](../../compose/scripts/toxiproxy-chaos-testing.md) for manual testing

Happy Chaos Testing! 🔥

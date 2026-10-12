# ✔️ Running Tests

## Unit Tests

```shell
make test  # Executes unit tests, excluding the test/ directory.
```

## All Test Suites

```shell
make testall  # Runs all test suites: unit tests (make test), long-running tests (make longtest), and sequential tests (make sequentialtest).
```

## Long-Running Tests

```shell
make longtest  # Executes long-running tests in test/longtest/ with a 10-minute timeout.
```

## Soak Test

The soak test runs the in-process daemon under steady transaction load for a long wall-clock duration and fails if heap or goroutine counts trend upward, which is how slow leaks and unbounded caches show up. It samples `runtime.ReadMemStats` (after a forced GC) and `runtime.NumGoroutine()` at a fixed interval, discards a warm-up window, and fails a metric only when both its least-squares growth across the run and its last-quarter-minus-first-quarter mean exceed the tolerance. It is behind the `soak` build tag, so no other suite runs it. The nightly workflow runs it for two hours.

```shell
make soaktest                                          # 30 minute run (default)
make soaktest SOAK_DURATION=2h SOAK_TIMEOUT=150m       # nightly length; SOAK_TIMEOUT must exceed SOAK_DURATION by 10m+
make soaktest SOAK_DURATION=4m SOAK_TIMEOUT=20m SOAK_WARMUP=1m SOAK_SAMPLE_INTERVAL=10s SOAK_INJECT_LEAK=1  # confirm a leak fails it
```

| Variable | Default | Meaning |
|---|---|---|
| `SOAK_DURATION` | `30m` | How long to drive load |
| `SOAK_WARMUP` | larger of 5m and 20% of the duration, at most half of it | Samples before this are ignored |
| `SOAK_SAMPLE_INTERVAL` | `30s` | Time between samples |
| `SOAK_TXS_PER_CYCLE` | `500` | Transactions sent and mined per cycle |
| `SOAK_UTXO_STORE` | sqlite | `aerospike` or `postgres` run the store in a container, so its memory is not measured |
| `SOAK_HEAP_TOLERANCE` / `SOAK_GOROUTINE_TOLERANCE` | `0.10` / `0.05` | Allowed growth as a fraction of the mean (floors of 16 MiB and 20 goroutines) |
| `SOAK_OUTPUT_DIR` | a temp dir | Where `soak-samples.csv` and, on failure, heap and goroutine profiles are written |
| `SOAK_INJECT_LEAK` | `false` | Retain 1 MiB and one goroutine per cycle, to check that the test fails |

Only the Go heap is measured: memory held outside it, such as C allocations or containerised stores, is not.

## Smoke Tests

```shell
make smoketest  # Runs E2E smoke tests in test/e2e/daemon/ready/ focused on basic functionality.

# With retry support (TEST_RETRY_DELAY is seconds between retries):
make smoketest TEST_RETRY_COUNT=3
make smoketest TEST_RETRY_COUNT=3 TEST_RETRY_DELAY=5

# Disable retries:
make smoketest TEST_RETRY_COUNT=1
```

## Sequential Tests

```shell
make sequentialtest  # Executes tests in test/sequentialtest/ sequentially.

# With retry support:
make sequentialtest TEST_RETRY_COUNT=5 TEST_RETRY_DELAY=3

# Database-backend-specific variants:
make sequentialtest-sqlite
make sequentialtest-postgres
make sequentialtest-aerospike

# Database variants also support retry flags:
make sequentialtest-aerospike TEST_RETRY_COUNT=5
make sequentialtest-postgres TEST_RETRY_COUNT=3
make sequentialtest-sqlite TEST_RETRY_COUNT=3
```

## Single Test

```shell
go test -v -race -tags "testtxmetacache" -run TestNameHere ./path/to/package
```

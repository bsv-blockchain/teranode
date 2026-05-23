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

---
description: Testing conventions and patterns
globs: "**/*_test.go"
---

## Test Infrastructure

- Don't mock blockchain client/store — use sqlitememory store
- Don't mock kafka — use `in_memory_kafka.go`
- Use `require` from testify (not `assert`)
- Avoid `t.Parallel()` unless specifically testing concurrency
- Use TestContainers for integration tests requiring external services (Aerospike, PostgreSQL)

## Test Tags

- `testtxmetacache`: Small cache for testing
- `largetxmetacache`: Production cache size
- `aerospike`: Tests requiring Aerospike

## Running Tests

```bash
make test                 # Unit tests (no integration)
make smoketest            # E2E smoke tests
make sequentialtest       # Order-dependent tests
make testall              # Everything

# Retry support (smoketest and sequentialtest only)
make smoketest TEST_RETRY_COUNT=3
make sequentialtest TEST_RETRY_COUNT=5 TEST_RETRY_DELAY=3

# Single test
go test -v -race -tags "testtxmetacache" -run TestName ./path/to/package
```

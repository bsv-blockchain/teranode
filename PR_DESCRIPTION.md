# Add Exponential Backoff to PostgreSQL Retry Logic

## Problem
Current retry strategy uses fixed 300ms delays, causing workers to block for 1-3 seconds on each failure. At 30% failure rate, this creates massive latency (7ms → 1.3s = **187x slower**).

## Solution
Implemented exponential backoff with jitter for all PostgreSQL operations through the existing `usql.DB` wrapper.

**Retry behavior:**
- Attempts: 100ms → 200ms → 400ms (configurable)
- Jitter: ±25% to prevent thundering herd
- Smart error detection: Only retries connection issues, deadlocks, timeouts (not SQL syntax errors)

## Changes

**New Files:**
- `util/usql/retry.go` - Core retry logic with exponential backoff
- `util/usql/metrics.go` - Prometheus metrics for retry operations
- `util/usql/retry_test.go` - 16 unit tests
- `util/usql/db_test.go` - 9 integration tests

**Modified:**
- `util/usql/db.go` - Integrated retry into Query/Exec methods
- `settings/interface.go` + `settings/settings.go` - Added retry configuration
- `util/sql.go` - Configure retry settings on DB init

## Configuration

```bash
postgres_retryMaxAttempts=3      # default: 3
postgres_retryBaseDelay=100ms    # default: 100ms
postgres_retryEnabled=true       # default: true
```

## Metrics

- `teranode_db_query_retries_total{retry_attempt="N"}` - Total retry attempts by attempt number
- `teranode_db_query_retry_success` - Queries that succeeded after retry
- `teranode_db_query_retry_exhausted` - Queries that exhausted all retries

**Note:** Service-level labels not included to avoid coupling low-level DB wrapper with service context. Service-specific metrics should be added at the service layer if needed.

# Test Retry Mechanism

## Overview

The test retry mechanism automatically retries failed tests to handle flaky tests and improve CI reliability. This is particularly useful for tests that occasionally fail due to timing issues, race conditions, or external dependencies like Aerospike.

## Features

- **Automatic Retry**: Failed tests are automatically retried up to a configurable number of attempts
- **Flaky Test Detection**: Tracks and reports tests that fail initially but pass on retry
- **Configurable**: Customize retry count and delay between attempts
- **Selective**: Only retries failed tests, not passing tests
- **Detailed Reporting**: Clear output showing which tests were flaky and how many attempts were needed

## Configuration

### Environment Variables

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `TEST_RETRY_COUNT` | Maximum number of retry attempts | 3 | `TEST_RETRY_COUNT=5` |
| `TEST_RETRY_DELAY` | Delay between retries (seconds) | 2 | `TEST_RETRY_DELAY=3` |

### Makefile Variables

You can also set these as Makefile variables:

```bash
make sequentialtest TEST_RETRY_COUNT=5 TEST_RETRY_DELAY=3
```

## Usage

### Sequential Tests

The sequential test runner has built-in retry support:

```bash
# Use default retry settings (3 retries, 2s delay)
make sequentialtest

# Customize retry settings
make sequentialtest TEST_RETRY_COUNT=5 TEST_RETRY_DELAY=3

# Disable retries (set to 1 attempt)
make sequentialtest TEST_RETRY_COUNT=1

# Database-specific tests with retry
make sequentialtest-aerospike TEST_RETRY_COUNT=5
make sequentialtest-postgres TEST_RETRY_COUNT=5
make sequentialtest-sqlite TEST_RETRY_COUNT=5
```

You can also pass retry settings via command-line arguments:

```bash
test/scripts/run_tests_sequentially.sh --retry 5 --retry-delay 3
test/scripts/run_tests_sequentially.sh --db aerospike --retry 5
```

### Smoke Tests

Smoke tests support retry via the gotestsum wrapper:

```bash
# Use default retry settings
make smoketest

# Customize retry settings
make smoketest TEST_RETRY_COUNT=5 TEST_RETRY_DELAY=3

# Disable retries
make smoketest TEST_RETRY_COUNT=1
```

### Manual Test Retry

You can use the retry wrapper directly for any test command:

```bash
# Using the generic retry wrapper
TEST_RETRY_COUNT=3 test/scripts/retry_test.sh go test -v -race ./...

# Using the gotestsum retry wrapper
TEST_RETRY_COUNT=3 test/scripts/gotestsum_with_retry.sh --format pkgname -- -v -race ./test/e2e/...
```

## Output Examples

### Passing Test (First Attempt)

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📝 Attempt 1 of 3
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[test output...]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Test PASSED on first attempt
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### Flaky Test (Passed on Retry)

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📝 Attempt 1 of 3
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[test output...]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
❌ Test FAILED on attempt 1
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⏳ Waiting 2s before retry...

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📝 Attempt 2 of 3
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[test output...]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️  FLAKY TEST DETECTED!
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Test failed on attempt(s): 1
Test PASSED on attempt: 2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### Failed Test (All Retries Exhausted)

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
❌ Test FAILED on attempt 3
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💥 Test FAILED after 3 attempts
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### Test Summary with Flaky Tests

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 Test Summary
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Total Tests:    15
✅ Passed:      13
❌ Failed:      0
⏭️  Skipped:     0
⚠️  Flaky:       2

Retry Configuration:
  Max Retries:  3
  Retry Delay:  2s

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️  FLAKY TESTS DETECTED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
The following tests failed initially but passed on retry:
  - TestBlockAssemblyRestart (passed on attempt 2/3)
  - TestDeleteAtHeightHappyPath (passed on attempt 3/3)

⚠️  WARNING: Flaky tests may indicate race conditions or timing issues
   that should be investigated and fixed.
```

## Best Practices

### When to Use Retries

✅ **Good use cases:**
- CI/CD pipelines where occasional infrastructure issues occur
- Tests involving external dependencies (Aerospike, Docker containers)
- Tests with known timing sensitivities that are being worked on
- Temporary workaround while investigating root cause

❌ **Avoid using retries for:**
- Masking real bugs in production code
- Ignoring race conditions that should be fixed
- Long-term solution for poorly written tests

### Recommended Settings

| Scenario | `TEST_RETRY_COUNT` | `TEST_RETRY_DELAY` | Rationale |
|----------|-------------------|-------------------|-----------|
| **CI/CD Pipeline** | 3-5 | 2-3s | Balance between catching flakes and pipeline duration |
| **Local Development** | 1-2 | 1-2s | Fast feedback, developer investigates failures |
| **Nightly Tests** | 5-10 | 3-5s | More thorough retry for comprehensive testing |
| **Container-based Tests** | 5 | 5s | Extra time for container startup/teardown |

### Investigating Flaky Tests

When tests are reported as flaky:

1. **Review the logs** - Check which specific assertion/operation failed
2. **Check for race conditions** - Run tests with `-race` flag enabled
3. **Look for timing dependencies** - Check for hard-coded sleeps or timeouts
4. **Verify test isolation** - Ensure tests don't share state or resources
5. **Fix the root cause** - Don't rely on retries long-term

Example investigation workflow:

```bash
# Run specific flaky test multiple times to reproduce
go test -v -race -count=10 -run TestFlakyTest ./...

# Run with verbose race detector
GORACE="log_path=race.log halt_on_error=1" go test -race -run TestFlakyTest ./...

# Check test logs for patterns
grep "TestFlakyTest" /tmp/teranode-test-results/*.txt
```

## CI/CD Integration

### GitHub Actions

Add retry configuration to your workflow:

```yaml
- name: Run Sequential Tests
  env:
    TEST_RETRY_COUNT: 5
    TEST_RETRY_DELAY: 3
  run: make sequentialtest

- name: Run Smoke Tests
  env:
    TEST_RETRY_COUNT: 3
    TEST_RETRY_DELAY: 2
  run: make smoketest
```

### Reporting Flaky Tests

The retry mechanism outputs flaky test information that can be:
- Captured in CI logs
- Parsed for test quality metrics
- Used to create tickets for investigation

Example parsing script:

```bash
# Extract flaky tests from logs
grep "⚠️  FLAKY" /tmp/teranode-test-results/*.txt > flaky_tests.txt

# Count flaky tests
FLAKY_COUNT=$(grep -c "⚠️  FLAKY" /tmp/teranode-test-results/*.txt || echo 0)

if [ "$FLAKY_COUNT" -gt 0 ]; then
    echo "Warning: $FLAKY_COUNT flaky test(s) detected"
fi
```

## Implementation Details

### Scripts

1. **`test/scripts/retry_test.sh`**
   - Generic retry wrapper for any command
   - Simple shell script with configurable retries
   - Used for one-off test executions

2. **`test/scripts/gotestsum_with_retry.sh`**
   - Wrapper specifically for gotestsum
   - Parses failed tests and retries only those
   - More efficient for large test suites

3. **`test/scripts/run_tests_sequentially.sh`**
   - Sequential test runner with built-in retry logic
   - Tracks flaky tests separately
   - Provides detailed summary

### How It Works

1. **Initial Run**: All tests execute normally
2. **Failure Detection**: If tests fail, identify which specific tests failed
3. **Selective Retry**: Only retry the failed tests (not the entire suite)
4. **Flaky Detection**: Track tests that fail then pass
5. **Reporting**: Clearly distinguish between:
   - Tests that passed immediately
   - Tests that were flaky (failed then passed)
   - Tests that failed permanently

## Troubleshooting

### Retries Not Working

Check that:
- Environment variables are exported: `export TEST_RETRY_COUNT=3`
- Scripts have execute permissions: `chmod +x test/scripts/*.sh`
- Using the correct Makefile target (updated targets support retry)

### All Tests Still Failing

If tests fail after all retries:
- Increase `TEST_RETRY_COUNT` temporarily to diagnose
- Check test logs for consistent failure patterns
- Verify test environment (containers, databases, etc.)
- Run tests individually to isolate the issue

### Too Many Retries Slowing Down CI

- Reduce `TEST_RETRY_COUNT` to balance speed vs. reliability
- Investigate and fix flaky tests to reduce retry frequency
- Consider running retries only on specific test suites (e.g., Aerospike tests)

## Future Enhancements

Potential improvements to the retry mechanism:

- [ ] Per-test retry configuration (some tests need more retries than others)
- [ ] Exponential backoff for retry delays
- [ ] Retry budget (max total retries across all tests)
- [ ] Integration with test reporting tools (JUnit XML, CTRF)
- [ ] Automatic issue creation for consistently flaky tests
- [ ] Historical flaky test tracking database

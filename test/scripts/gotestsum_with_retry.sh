#!/bin/bash

# gotestsum_with_retry.sh - Wrapper for gotestsum with retry support for individual failed tests
#
# Usage: gotestsum_with_retry.sh [gotestsum_args...] -- [go_test_args...]
#
# Environment variables:
#   TEST_RETRY_COUNT - Number of retry attempts for failed tests (default: 3)
#   TEST_RETRY_DELAY - Delay between retries in seconds (default: 2)
#
# This script:
# 1. Runs gotestsum with all provided arguments
# 2. If tests fail, parses which specific tests failed
# 3. Retries only the failed tests up to TEST_RETRY_COUNT times
# 4. Reports flaky tests that passed on retry

set -eo pipefail

# Configuration
RETRY_COUNT=${TEST_RETRY_COUNT:-3}
RETRY_DELAY=${TEST_RETRY_DELAY:-2}

# Run initial test suite
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔄 Running tests with retry support (max retries: $RETRY_COUNT)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Store output to parse failed tests
TMPDIR=$(mktemp -d)
OUTPUT_FILE="$TMPDIR/test_output.txt"

# Run tests and capture output
if gotestsum "$@" 2>&1 | tee "$OUTPUT_FILE"; then
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "✅ All tests passed on first attempt"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    rm -rf "$TMPDIR"
    exit 0
fi

# Extract failed test names and packages
# Go test output format: "FAIL    package_path    duration"
# gotestsum output format: "FAIL TestName (duration)" or "FAIL package TestName"
# We need to extract both package and test name

# Try different patterns to extract failed tests
FAILED_TESTS_RAW=$(grep "^FAIL" "$OUTPUT_FILE" || true)

# Check if there was a race condition detected
RACE_DETECTED=$(grep -i "WARNING: DATA RACE" "$OUTPUT_FILE" || true)

if [ -n "$RACE_DETECTED" ]; then
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "⚠️  RACE CONDITION DETECTED"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "The test run detected a data race condition."
    echo "Race conditions are concurrency bugs that need to be fixed in the code."
    echo "Retrying will not help - the race needs to be fixed."
    echo ""
    echo "See race detector output in test logs for details."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    rm -rf "$TMPDIR"
    exit 1
fi

if [ -z "$FAILED_TESTS_RAW" ]; then
    echo "Could not parse failed tests from output. Exiting with failure."
    echo "Check $OUTPUT_FILE for details"
    rm -rf "$TMPDIR"
    exit 1
fi

# Parse failed tests - format can be:
# "FAIL TestName" or "FAIL package/path TestName"
declare -a FAILED_TEST_LIST=()
declare -a FAILED_PACKAGE_LIST=()

while IFS= read -r line; do
    # Extract test name from various gotestsum output formats
    # Pattern 1: "FAIL TestName (0.50s)" -> TestName
    # Pattern 2: "FAIL package TestName" -> TestName
    if echo "$line" | grep -q "FAIL.*Test"; then
        test_name=$(echo "$line" | sed -n 's/.*FAIL[[:space:]]*\([^[:space:]]*\)[[:space:]]*\(Test[^[:space:]]*\).*/\2/p')
        if [ -z "$test_name" ]; then
            # Try alternative pattern: FAIL TestName
            test_name=$(echo "$line" | sed -n 's/.*FAIL[[:space:]]*\(Test[^[:space:]]*\).*/\1/p')
        fi

        if [ -n "$test_name" ]; then
            FAILED_TEST_LIST+=("$test_name")
        fi
    fi
done <<< "$FAILED_TESTS_RAW"

if [ ${#FAILED_TEST_LIST[@]} -eq 0 ]; then
    echo "Could not extract test names from failed tests. Exiting with failure."
    echo "Failed test output:"
    echo "$FAILED_TESTS_RAW"
    rm -rf "$TMPDIR"
    exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "⚠️  ${#FAILED_TEST_LIST[@]} test(s) failed. Retrying only failed tests..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Show which tests will be retried
echo "Tests to retry:"
for test in "${FAILED_TEST_LIST[@]}"; do
    echo "  - $test"
done
echo ""

# Track flaky and permanently failed tests
declare -a FLAKY_TESTS=()
declare -a PERMANENTLY_FAILED_TESTS=()

# Extract package path from original command
PACKAGE_PATH="."
for arg in "$@"; do
    if [[ "$arg" =~ ^\./ ]] || [[ "$arg" =~ ^github\.com/ ]]; then
        PACKAGE_PATH="$arg"
        break
    fi
done

# If we couldn't find package in args, extract from go test output
if [ "$PACKAGE_PATH" = "." ]; then
    # Try to extract package from failed test output
    PACKAGE_PATH=$(grep "^FAIL" "$OUTPUT_FILE" | head -1 | awk '{print $2}' || echo ".")
fi

# Retry each failed test individually
for test_name in "${FAILED_TEST_LIST[@]}"; do
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Retrying: $test_name"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    attempt=1
    test_passed=false

    while [ $attempt -le $RETRY_COUNT ]; do
        if [ $attempt -gt 1 ]; then
            echo "  ⏳ Retry attempt $attempt/$RETRY_COUNT (after ${RETRY_DELAY}s delay)..."
            sleep $RETRY_DELAY
        fi

        # Run only this specific failed test with same args as original
        # Extract args after -- separator
        GO_TEST_ARGS=""
        FOUND_SEPARATOR=false
        for arg in "$@"; do
            if [ "$FOUND_SEPARATOR" = true ]; then
                GO_TEST_ARGS="$GO_TEST_ARGS $arg"
            elif [ "$arg" = "--" ]; then
                FOUND_SEPARATOR=true
            fi
        done

        # Replace -run argument if it exists, or add it
        if echo "$GO_TEST_ARGS" | grep -q "\-run"; then
            # Replace existing -run pattern
            GO_TEST_ARGS=$(echo "$GO_TEST_ARGS" | sed "s/-run[[:space:]]*[^[:space:]]*/-run $test_name/")
        else
            # Add -run argument
            GO_TEST_ARGS="$GO_TEST_ARGS -run $test_name"
        fi

        # Run just this test
        if eval "gotestsum --format testname -- $GO_TEST_ARGS" 2>&1; then
            if [ $attempt -eq 1 ]; then
                echo "  ✅ Test passed on first retry"
            else
                FLAKY_TESTS+=("$test_name (passed on attempt $attempt/$RETRY_COUNT)")
                echo "  ⚠️  FLAKY - Test passed on attempt $attempt/$RETRY_COUNT"
            fi
            test_passed=true
            break
        fi

        attempt=$((attempt + 1))
    done

    if [ "$test_passed" = false ]; then
        PERMANENTLY_FAILED_TESTS+=("$test_name")
        echo "  ❌ Test failed after $RETRY_COUNT retries"
    fi
    echo ""
done

# Print summary
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 Retry Summary"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ ${#FLAKY_TESTS[@]} -gt 0 ]; then
    echo "⚠️  Flaky tests (${#FLAKY_TESTS[@]}):"
    for test in "${FLAKY_TESTS[@]}"; do
        echo "  - $test"
    done
    echo ""
fi

if [ ${#PERMANENTLY_FAILED_TESTS[@]} -gt 0 ]; then
    echo "❌ Failed tests after retries (${#PERMANENTLY_FAILED_TESTS[@]}):"
    for test in "${PERMANENTLY_FAILED_TESTS[@]}"; do
        echo "  - $test"
    done
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    rm -rf "$TMPDIR"
    exit 1
fi

echo "✅ All previously failed tests passed on retry"
if [ ${#FLAKY_TESTS[@]} -gt 0 ]; then
    echo "⚠️  WARNING: ${#FLAKY_TESTS[@]} flaky test(s) detected - consider investigating"
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

rm -rf "$TMPDIR"
exit 0

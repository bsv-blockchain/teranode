#!/bin/bash

# retry_test.sh - Retry wrapper for flaky tests
#
# Usage: retry_test.sh <test_command> [args...]
#
# Environment variables:
#   TEST_RETRY_COUNT - Number of retry attempts (default: 3)
#   TEST_RETRY_DELAY - Delay between retries in seconds (default: 2)
#
# Exit codes:
#   0 - Test passed (on first or subsequent attempt)
#   1 - Test failed after all retries
#
# Example:
#   TEST_RETRY_COUNT=3 ./retry_test.sh go test -v -race ./...

set -euo pipefail

# Configuration
RETRY_COUNT=${TEST_RETRY_COUNT:-3}
RETRY_DELAY=${TEST_RETRY_DELAY:-2}

# Check if command is provided
if [ $# -eq 0 ]; then
    echo "Error: No test command provided"
    echo "Usage: $0 <test_command> [args...]"
    exit 1
fi

# Store the command and arguments
TEST_COMMAND="$@"

# Counters
attempt=1
test_passed=false

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔄 Test Retry Wrapper (max attempts: $RETRY_COUNT)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Command: $TEST_COMMAND"
echo ""

# Run test with retries
while [ $attempt -le $RETRY_COUNT ]; do
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📝 Attempt $attempt of $RETRY_COUNT"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""

    # Run the test command
    if eval "$TEST_COMMAND"; then
        test_passed=true

        if [ $attempt -eq 1 ]; then
            echo ""
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "✅ Test PASSED on first attempt"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        else
            echo ""
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "⚠️  FLAKY TEST DETECTED!"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "Test failed on attempt(s): $(seq 1 $((attempt - 1)) | tr '\n' ',' | sed 's/,$//')"
            echo "Test PASSED on attempt: $attempt"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        fi

        exit 0
    else
        echo ""
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "❌ Test FAILED on attempt $attempt"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

        if [ $attempt -lt $RETRY_COUNT ]; then
            echo "⏳ Waiting ${RETRY_DELAY}s before retry..."
            sleep $RETRY_DELAY
            echo ""
        fi

        attempt=$((attempt + 1))
    fi
done

# All retries exhausted
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "💥 Test FAILED after $RETRY_COUNT attempts"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
exit 1

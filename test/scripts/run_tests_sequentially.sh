#!/bin/bash

set -euo pipefail

# Parse command line arguments
DB_FILTER=""
RETRY_COUNT=${TEST_RETRY_COUNT:-3}
RETRY_DELAY=${TEST_RETRY_DELAY:-2}
SHARD=""
TOTAL=""
LIST_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --db)
            DB_FILTER="$2"
            shift 2
            ;;
        --retry)
            RETRY_COUNT="$2"
            shift 2
            ;;
        --retry-delay)
            RETRY_DELAY="$2"
            shift 2
            ;;
        --shard)
            SHARD="$2"; shift 2 ;;
        --total)
            TOTAL="$2"; shift 2 ;;
        --list-only)
            LIST_ONLY=true; shift ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--db sqlite|postgres|aerospike] [--retry COUNT] [--retry-delay SECONDS] [--shard N] [--total M] [--list-only]"
            exit 1
            ;;
    esac
done

# --shard and --total are only meaningful together, and shard must be in range.
if { [ -n "$SHARD" ] && [ -z "$TOTAL" ]; } || { [ -n "$TOTAL" ] && [ -z "$SHARD" ]; }; then
    echo "Error: --shard and --total must be used together" >&2
    exit 1
fi
if [ -n "$TOTAL" ]; then
    if ! [ "$TOTAL" -gt 0 ] 2>/dev/null; then
        echo "Error: --total must be a positive integer (got '$TOTAL')" >&2
        exit 1
    fi
    if ! { [ "$SHARD" -ge 0 ] && [ "$SHARD" -lt "$TOTAL" ]; } 2>/dev/null; then
        echo "Error: --shard must be an integer in [0, $TOTAL) (got '$SHARD')" >&2
        exit 1
    fi
fi

# --db (legacy name-substring filter) and --shard must not be combined: the shard
# index spans the full list and --db is applied afterward, which would leave shards
# uneven. They are mutually exclusive.
if [ -n "$DB_FILTER" ] && [ -n "$TOTAL" ]; then
    echo "Error: --db cannot be combined with --shard/--total" >&2
    exit 1
fi

# Common test flags
TEST_FLAGS="-timeout 120 -tags aerospike,native,functional,test_sequentially,test_all,memory,postgres,sqlite -count=1"

# Store the original directory
ORIGINAL_DIR=$(pwd)

# Find all test files that have "test_sequentially" in their first line
test_files=$(find ./test/sequentialtest -name "*_test.go" -type f -print)

if [ -z "$test_files" ]; then
    echo "No test files found with 'test_sequentially' in their first line"
    exit 1
fi

# Build the full ordered spec list by parsing every test file once.
# Each element is: test_dir<TAB>test_binary<TAB>testspec
# where testspec is either "Func" (no subtests) or "Func/Subtest".
declare -a ALL_SPECS=()
for test_file in $test_files; do
    test_dir=$(dirname "$test_file")
    test_filename=$(basename "$test_file")
    package_name=$(basename "$test_dir")
    test_binary="${package_name}.test"

    # Read all Test functions (excluding TestMain), keeping line numbers.
    # || true: when grep finds no Test functions, printf '\0' never runs and read -d '' exits non-zero; prevent set -e from aborting.
    IFS=$'\n' read -r -d '' -a test_functions < <(grep -n "^func Test" "$test_file" | grep -v "^[0-9]*:func TestMain" && printf '\0') || true

    total_lines=$(wc -l < "$test_file")

    for ((fi=0; fi<${#test_functions[@]}; fi++)); do
        line_info="${test_functions[fi]}"
        line_num=$(echo "$line_info" | cut -d: -f1)
        line=$(echo "$line_info" | cut -d: -f2-)

        test_func=$(echo "$line" | awk '{print $2}' | cut -d'(' -f1)

        # Find the end boundary (next function or EOF)
        if [ $((fi + 1)) -lt "${#test_functions[@]}" ]; then
            next_line_num=$(echo "${test_functions[$((fi + 1))]}" | cut -d: -f1)
        else
            next_line_num=$total_lines
        fi

        # Collect subtests in this function's body
        has_subtests=false
        while IFS= read -r subtest_line; do
            if echo "$subtest_line" | grep -q 't\.Run('; then
                subtest_name=$(echo "$subtest_line" | sed -n 's/.*t\.Run("\([^"]*\)".*/\1/p')
                if [ -n "$subtest_name" ]; then
                    has_subtests=true
                    ALL_SPECS+=("${test_dir}"$'\t'"${test_binary}"$'\t'"${test_func}/${subtest_name}")
                fi
            fi
        done < <(sed -n "${line_num},${next_line_num}p" "$test_file")

        if [ "$has_subtests" = false ]; then
            ALL_SPECS+=("${test_dir}"$'\t'"${test_binary}"$'\t'"${test_func}")
        fi
    done
done

# Apply even shard partitioning (on the full list index) then optional --db filter.
# The shard index runs over ALL specs regardless of --db, so the disjoint/exhaustive
# property holds for the self-test even when --db is not set.
declare -a RUN_SPECS=()
idx=0
for spec in "${ALL_SPECS[@]}"; do
    testspec="${spec##*$'\t'}"

    # Shard filter: only keep entries whose position mod TOTAL == SHARD
    if [ -n "${TOTAL}" ]; then
        if [ $(( idx % TOTAL )) -ne "${SHARD:-0}" ]; then
            idx=$((idx+1))
            continue
        fi
    fi

    # DB name-substring filter (case-insensitive, orthogonal to sharding)
    if [ -n "$DB_FILTER" ]; then
        ts_lower=$(echo "$testspec" | tr '[:upper:]' '[:lower:]')
        db_lower=$(echo "$DB_FILTER" | tr '[:upper:]' '[:lower:]')
        if [[ ! "$ts_lower" =~ $db_lower ]]; then
            idx=$((idx+1))
            continue
        fi
    fi

    RUN_SPECS+=("$spec")
    idx=$((idx+1))
done

# --list-only: print specs and exit before any compilation or execution
if [ "$LIST_ONLY" = true ]; then
    for spec in "${RUN_SPECS[@]}"; do
        echo "${spec##*$'\t'}"
    done
    exit 0
fi

# Store start time
start_time=$(date +%s)
echo -e "\nStarting test execution at $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "----------------------------------------------------------"

# Compile all test packages
echo "Compiling test packages..."
for test_file in $test_files; do
    test_dir=$(dirname "$test_file")
    cd "$test_dir"
    echo "Compiling tests in ${test_dir}..."
    if ! go test -c -tags aerospike,native,memory,postgres,sqlite -race; then
        echo "Failed to compile tests in ${test_dir}"
        cd "$ORIGINAL_DIR"
        exit 1
    fi
    cd "$ORIGINAL_DIR"
done
echo "----------------------------------------------------------"

# Initialize counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
SKIPPED_TESTS=0
FLAKY_TESTS=0

# Arrays to store test results for summary
declare -a FAILED_TEST_NAMES
declare -a PASSED_TEST_NAMES
declare -a SKIPPED_TEST_NAMES
declare -a FLAKY_TEST_NAMES

# Function to run a test with retry logic and update counters
run_test() {
    local test_name=$1
    local test_binary=$2
    local attempt=1
    local max_attempts=$RETRY_COUNT

    while [ $attempt -le $max_attempts ]; do
        if [ $attempt -gt 1 ]; then
            echo "  ⏳ Retry attempt $attempt/$max_attempts (after ${RETRY_DELAY}s delay)..."
            sleep $RETRY_DELAY
        fi

        local output
        output=$("./${test_binary}" -test.run "^${test_name}$" -test.timeout 120s -test.count=1 2>&1)
        local result=$?

        # Only show output on first attempt or final failure
        if [ $attempt -eq 1 ] || [ $attempt -eq $max_attempts ]; then
            echo "$output" | sed '$d'
        fi

        if echo "$output" | grep -q "warning: no tests to run"; then
            SKIPPED_TESTS=$((SKIPPED_TESTS + 1))
            SKIPPED_TEST_NAMES+=("$test_name")
            echo "SKIPPED"
            return 0
        elif [ $result -eq 0 ]; then
            if [ $attempt -eq 1 ]; then
                PASSED_TESTS=$((PASSED_TESTS + 1))
                PASSED_TEST_NAMES+=("$test_name")
                echo "✅ PASSED"
            else
                FLAKY_TESTS=$((FLAKY_TESTS + 1))
                FLAKY_TEST_NAMES+=("$test_name (passed on attempt $attempt/$max_attempts)")
                PASSED_TESTS=$((PASSED_TESTS + 1))
                echo "⚠️  FLAKY - PASSED on attempt $attempt/$max_attempts"
            fi
            return 0
        elif [ $attempt -eq $max_attempts ]; then
            # Final failure after all retries
            FAILED_TESTS=$((FAILED_TESTS + 1))
            FAILED_TEST_NAMES+=("$test_name")
            echo "❌ FAILED after $max_attempts attempts"
            return 1
        fi

        attempt=$((attempt + 1))
    done
}

any_test_failed=0

# Run each spec from the pre-built list
for spec in "${RUN_SPECS[@]}"; do
    # Parse the three tab-separated fields
    IFS=$'\t' read -r test_dir test_binary testspec <<< "$spec"

    cd "$test_dir"

    echo -e "\nRunning: $testspec"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if ! run_test "${testspec}" "${test_binary}"; then
        any_test_failed=1
    fi

    echo -e "\n"
    cd "$ORIGINAL_DIR"
done

# Clean up test binaries
find . -name "*.test" -type f -delete

# Print summary
echo -e "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 Test Summary"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Total Tests:    $TOTAL_TESTS"
echo "✅ Passed:      $PASSED_TESTS"
echo "❌ Failed:      $FAILED_TESTS"
echo "⏭️  Skipped:     $SKIPPED_TESTS"
if [ "$FLAKY_TESTS" -gt 0 ]; then
    echo "⚠️  Flaky:       $FLAKY_TESTS"
fi
echo ""
echo "Retry Configuration:"
echo "  Max Retries:  $RETRY_COUNT"
echo "  Retry Delay:  ${RETRY_DELAY}s"

# Print flaky tests if any
if [ "$FLAKY_TESTS" -gt 0 ]; then
    echo -e "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "⚠️  FLAKY TESTS DETECTED"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "The following tests failed initially but passed on retry:"
    for test in "${FLAKY_TEST_NAMES[@]}"; do
        echo "  - $test"
    done
    echo ""
    echo "⚠️  WARNING: Flaky tests may indicate race conditions or timing issues"
    echo "   that should be investigated and fixed."
fi

# Print failed tests if any
if [ "$FAILED_TESTS" -gt 0 ]; then
    echo -e "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "❌ FAILED TESTS"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "The following tests failed after $RETRY_COUNT attempts:"
    for test in "${FAILED_TEST_NAMES[@]}"; do
        echo "  - $test"
    done
fi

# Print end time and duration
end_time=$(date +%s)
duration=$((end_time - start_time))
echo -e "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Test execution completed at $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "Total duration: $((duration / 60)) minutes and $((duration % 60)) seconds"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ "$FAILED_TESTS" -gt 0 ]; then
    echo -e "\n❌ Some tests failed after retries!"
    exit 1
fi

exit $any_test_failed

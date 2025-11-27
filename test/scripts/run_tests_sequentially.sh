#!/bin/bash

set -euo pipefail

# Parse command line arguments
DB_FILTER=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --db)
            DB_FILTER="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--db sqlite|postgres|aerospike]"
            exit 1
            ;;
    esac
done

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

# Store start time
start_time=$(date +%s)
echo -e "\nStarting test execution at $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "----------------------------------------------------------"

# First compile all test packages
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

# Arrays to store test results for summary
declare -a FAILED_TEST_NAMES
declare -a PASSED_TEST_NAMES
declare -a SKIPPED_TEST_NAMES

# Function to run a test and update counters
run_test() {
    local test_name=$1
    local test_binary=$2
    local output
    output=$("./${test_binary}" -test.run "^${test_name}$" -test.timeout 120s -test.count=1 2>&1)
    local result=$?

    echo "$output" | sed '$d'
    
    if echo "$output" | grep -q "warning: no tests to run"; then
        SKIPPED_TESTS=$((SKIPPED_TESTS + 1))
        SKIPPED_TEST_NAMES+=("$test_name")
        echo "SKIPPED"
        return 0
    elif [ $result -eq 0 ]; then
        PASSED_TESTS=$((PASSED_TESTS + 1))
        PASSED_TEST_NAMES+=("$test_name")
        echo "PASSED"
        return 0
    else
        FAILED_TESTS=$((FAILED_TESTS + 1))
        FAILED_TEST_NAMES+=("$test_name")
        echo "FAILED"
        return 1
    fi
}

any_test_failed=0
# Process each test file
for test_file in $test_files; do
    # Change to the directory containing the test file
    test_dir=$(dirname "$test_file")
    cd "$test_dir"
    test_filename=$(basename "$test_file")
    package_name=$(basename "$test_dir")
    test_binary="${package_name}.test"

    # Store all test functions in an array
    IFS=$'\n' read -r -d '' -a test_functions < <(grep -n "^func Test" "$test_filename" | grep -v "^func TestMain" && printf '\0')
    
    for ((i=0; i<${#test_functions[@]}; i++)); do
        line_info="${test_functions[i]}"
        line_num=$(echo "$line_info" | cut -d: -f1)
        line=$(echo "$line_info" | cut -d: -f2-)
        
        # Extract the test function name
        test_func=$(echo "$line" | awk '{print $2}' | cut -d'(' -f1)

        # Skip test if DB filter is set and test doesn't match
        if [ ! -z "$DB_FILTER" ]; then
            # Normalize both to lowercase for case-insensitive matching
            test_func_lower=$(echo "$test_func" | tr '[:upper:]' '[:lower:]')
            db_filter_lower=$(echo "$DB_FILTER" | tr '[:upper:]' '[:lower:]')
            if [[ ! "$test_func_lower" =~ $db_filter_lower ]]; then
                continue
            fi
        fi

        # Find the next test function to get the end line
        next_line_num=
        if [ $((i + 1)) -lt "${#test_functions[@]}" ]; then
            next_line_num=$(echo "${test_functions[$((i + 1))]}" | cut -d: -f1)
        else
            next_line_num=$(wc -l < "$test_filename")
        fi

        # Look for t.Run calls between the current function and the next
        has_subtests=false
        while IFS= read -r subtest_line; do
            if echo "$subtest_line" | grep -q "t.Run("; then
                has_subtests=true
                subtest_name=$(echo "$subtest_line" | sed -n 's/.*t.Run("\([^"]*\)".*/\1/p')
                if [ ! -z "$subtest_name" ]; then
                    echo -e "\nRunning $test_func / $subtest_name"
                    
                    TOTAL_TESTS=$((TOTAL_TESTS + 1))
                    if ! run_test "${test_func}/${subtest_name}" "${test_binary}"; then
                        any_test_failed=1
                    fi
                fi
            fi
        done < <(sed -n "${line_num},${next_line_num}p" "$test_filename")
        
        # If no subtests found, run the main test function
        if [ "$has_subtests" = false ]; then
            echo "Running: $test_func"
            TOTAL_TESTS=$((TOTAL_TESTS + 1))
            if ! run_test "${test_func}" "${test_binary}"; then
                any_test_failed=1
            fi
        fi

        echo -e "\n"
    done

    # Return to the original directory after processing each test file
    cd "$ORIGINAL_DIR"
done

# Clean up test binaries
find . -name "*.test" -type f -delete

# Print summary
echo -e "\nTest Summary:"
echo "Total Tests: $TOTAL_TESTS"
echo "Passed: $PASSED_TESTS"
echo "Failed: $FAILED_TESTS"
echo "Skipped: $SKIPPED_TESTS"

# Print end time and duration
end_time=$(date +%s)
duration=$((end_time - start_time))
echo -e "\nTest execution completed at $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "Total duration: $((duration / 60)) minutes and $((duration % 60)) seconds"

if [ "$FAILED_TESTS" -gt 0 ]; then
    echo -e "\nSome tests failed!"
    exit 1
fi

exit $any_test_failed

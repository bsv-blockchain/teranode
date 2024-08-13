#!/bin/bash

# Directory to start searching from (can be set to project root)
SEARCH_DIR="./"

# Output JSON report file
REPORT_FILE="sonar_filename_report.json"

# Initialize the report JSON structure
echo '{ "issues": [' > $REPORT_FILE

# Function to check if the filename is snake_case.go
is_snake_case() {
    local filename=$1
    if [[ $filename =~ ^[a-z0-9_]+\.go$ ]]; then
        return 0
    else
        return 1
    fi
}

# Function to normalize file paths
normalize_path() {
    local path=$1
    # Remove leading './' or '/' and replace any '//' with '/'
    normalized_path=$(echo "$path" | sed 's|^\./||' | sed 's|^/||' | sed 's|//|/|g')
    echo "$normalized_path"
}

# Initialize a flag to handle comma placement in JSON
first_issue=true

# Iterate over all .go files in the directory
find $SEARCH_DIR -type f -name "*.go" | while read -r file; do
    basename=$(basename "$file")
    if ! is_snake_case "$basename"; then
        # echo "ERROR: File $file does not follow snake_case.go convention"

        if [ "$first_issue" = false ]; then
            echo ',' >> $REPORT_FILE
        fi

        # Normalize the file path
        normalized_file=$(normalize_path "$file")

        # Add the issue to the JSON report
        cat <<EOL >> $REPORT_FILE
{
    "engineId": "custom-filename-check",
    "ruleId": "filename-convention",
    "primaryLocation": {
        "message": "Filename does not follow snake_case.go convention",
        "filePath": "$normalized_file"
    },
    "severity":"MAJOR",
    "type":"BUG",
    "textRange": {
      "startLine": 1
    }
}
EOL

        # After the first issue, we need to add a comma before subsequent issues
        first_issue=false
    fi
done

# Close the JSON structure
echo ']}' >> $REPORT_FILE

echo "Filename validation completed. See $REPORT_FILE for details."

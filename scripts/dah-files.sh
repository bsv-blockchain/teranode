#!/bin/bash

# Temporary file for sorting
tmpfile=$(mktemp)

while IFS= read -r -d '' file; do
    if [[ $file =~ \.dah$ ]]; then
        number=$(cat "$file" 2>/dev/null)
        if [[ $number =~ ^[0-9]+$ ]]; then
            # Store number and file path in tmpfile
            echo "$number|$file" >> "$tmpfile"
        fi
    fi
done < <(find . -type f -name "*.dah" -print0)

if [ ! -s "$tmpfile" ]; then
    echo "No valid .dah files found"
    rm "$tmpfile"
    exit 0
fi

# Sort by number and print
sort -n "$tmpfile" | while IFS='|' read -r number file; do
    echo "$number (from $file)"
done

# Clean up
rm "$tmpfile"
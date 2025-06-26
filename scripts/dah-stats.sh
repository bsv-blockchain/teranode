#!/bin/bash

files=()
numbers=()
while IFS= read -r -d '' file; do
    if [[ $file =~ \.dah$ ]]; then
        number=$(cat "$file" 2>/dev/null)
        if [[ $number =~ ^[0-9]+$ ]]; then
            files+=("$file")
            numbers+=("$number")
        fi
    fi
done < <(find . -type f -name "*.dah" -print0)

if [ ${#numbers[@]} -eq 0 ]; then
    echo "No valid .dah files found"
else
    min_val=${numbers[0]}
    max_val=${numbers[0]}
    min_file=${files[0]}
    max_file=${files[0]}
    for ((i=0; i<${#numbers[@]}; i++)); do
        num=${numbers[$i]}
        file=${files[$i]}
        if ((num < min_val)); then
            min_val=$num
            min_file=$file
        fi
        if ((num > max_val)); then
            max_val=$num
            max_file=$file
        fi
    done
    echo "Minimum: $min_val (from $min_file)"
    echo "Maximum: $max_val (from $max_file)"
fi
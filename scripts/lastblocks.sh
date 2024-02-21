#!/bin/bash

function get_lastblocks() {
  local node=$1
  local n=$2
  local url="https://$node.scaling.ubsv.dev/api/v1/lastblocks?n=$n"
  curl -s "$url" | jq -r '.[] | "\(.height) \(.height): \(.hash)"' > "$3"
}

n=10
if [ -n "$1" ]; then
  n=$1
fi

# Prepare temporary files
tmp1=$(mktemp)
tmp2=$(mktemp)
sorted1=$(mktemp)
sorted2=$(mktemp)
joined=$(mktemp)
joined.coloured=$(mktemp)

# Fetch data in parallel
get_lastblocks m1 "$n" "$tmp1" &
pid1=$!
get_lastblocks m2 "$n" "$tmp2" &
pid2=$!

wait $pid1 $pid2

# Sort files by height
sort -n -r -k1,1 "$tmp1" > "$sorted1"
sort -n -r -k1,1 "$tmp2" > "$sorted2"

# Join the sorted files on height, handling unpaired lines with placeholders
join -a1 -a2 -e 'MISSING' -o 1.1,1.3,2.3 "$sorted1" "$sorted2" > "$joined"

# Process the joined file for matching lines and apply color
awk '{
  if ($2 == $3)
    print "\033[32m" $0 "\033[0m";
  else
    print "\033[31m" $0 "\033[0m";
}' "$joined" > "$joined.coloured"

# Cleanup
rm "$tmp1" "$tmp2" "$sorted1" "$sorted2" "$joined"

# Display the results with colors
echo $(date -u)
echo ""
cat "$joined.coloured" | column -t
echo ""

# Remove the colored joined file
rm "$joined.coloured"

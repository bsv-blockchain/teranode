#!/bin/bash

function get_block_header() {
  local node=$1
  local url="http://$node/api/v1/bestblockheader/json"
  curl -s "$url" | jq -r '. | "\(.height): \(.hash)"' > "$2" # Write output to temp file
}

while true; do
  tmp1=$(mktemp)

  get_block_header localhost:8090 "$tmp1" &
  pid1=$!

  # Wait for both background processes to finish
  wait $pid1

  # Clear the screen and display results
  echo -ne "\033c"
  echo $(date -u)
  echo ""
  echo -e "\033[32m  ubsv-1: $(cat $tmp1)  \033[0m"
  echo ""

  # Clean up temporary files
  rm "$tmp1"

  # Countdown before the next update
  for (( i=5; i>0; i-- )); do
    echo -ne "  Refreshing in $i seconds  \r"
    sleep 1
  done
  echo -ne "  Refreshing...                  \r"
done


#!/bin/bash

function get_block_header() {
  local node=$1
  local url="https://$node.scaling.ubsv.dev/api/v1/bestblockheader/json"
  curl -s "$url" | jq -r '. | "\(.height): \(.hash)"' > "$2" # Write output to temp file
}

while true; do
  tmp1=$(mktemp)
  tmp2=$(mktemp)

  get_block_header m1 "$tmp1" &
  pid1=$!
  get_block_header m2 "$tmp2" &
  pid2=$!

  # Wait for both background processes to finish
  wait $pid1
  wait $pid2

  # Clear the screen and display results
  echo -ne "\033c"
  echo $(date -u)
  echo ""
  
  if [[ $(cat $tmp1) == $(cat $tmp2) ]]; then
    echo -e "\033[32m  m1: $(cat $tmp1)  \033[0m"
    echo -e "\033[32m  m2: $(cat $tmp2)  \033[0m"
  else
    echo -e "\033[31m  m1: $(cat $tmp1)  \033[0m"
    echo -e "\033[31m  m2: $(cat $tmp2)  \033[0m"
  fi
  echo ""

  # Clean up temporary files
  rm "$tmp1" "$tmp2"

  # Countdown before the next update
  for (( i=10; i>0; i-- )); do
    echo -ne "  Refreshing in $i seconds  \r"
    sleep 1
  done
  echo -ne "  Refreshing...                  \r"
done


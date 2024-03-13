#!/bin/bash

function get_block_header() {
  local node=$1
  local url="http://$node/api/v1/bestblockheader/json"
  curl -s "$url" | jq -r '. | "\(.height): \(.hash)"' > "$2" # Write output to temp file
}

while true; do
  tmp1=$(mktemp)
  tmp2=$(mktemp)
  tmp3=$(mktemp)

  get_block_header localhost:18090 "$tmp1" &
  pid1=$!
  get_block_header localhost:28090 "$tmp2" &
  pid2=$!
  get_block_header localhost:38090 "$tmp3" &
  pid3=$!

  # Wait for both background processes to finish
  wait $pid1
  wait $pid2
  wait $pid3

  # Clear the screen and display results
  echo -ne "\033c"
  echo $(date -u)
  echo ""
  
  if [[ $(cat $tmp1) == $(cat $tmp2) ]] && [[ $(cat $tmp2) == $(cat $tmp3) ]]; then
    echo -e "\033[32m  ubsv-1: $(cat $tmp1)  \033[0m"
    echo -e "\033[32m  ubsv-2: $(cat $tmp2)  \033[0m"
    echo -e "\033[32m  ubsv-3: $(cat $tmp3)  \033[0m"
  else
    echo -e "\033[31m  ubsv-1: $(cat $tmp1)  \033[0m"
    echo -e "\033[31m  ubsv-2: $(cat $tmp2)  \033[0m"
    echo -e "\033[31m  ubsv-3: $(cat $tmp3)  \033[0m"
  fi
  echo ""

  # Clean up temporary files
  rm "$tmp1" "$tmp2" "$tmp3"

  # Countdown before the next update
  for (( i=5; i>0; i-- )); do
    echo -ne "  Refreshing in $i seconds  \r"
    sleep 1
  done
  echo -ne "  Refreshing...                  \r"
done


#!/bin/bash

function get_block_header() {
  local node=$1
  https://eu-central-1-teranode-teranet-prod-1.teranode.dev/
  local url="https://$node.teranode.dev/api/v1/bestblockheader/json"
  local output=$(curl -s "$url")

  if [[ -z $output ]]; then
    echo "n/a" > "$2" # Write n/a to temp file
  else
    # Check if the output is valid JSON
    echo $output | jq -e . > /dev/null 2>&1
    if [[ $? -ne 0 ]]; then
      echo "n/a" > "$2" # Write n/a to temp file
    else
      echo $output | jq -r '. | "\(.height): \(.hash)"' > "$2" # Write output to temp file
    fi
  fi
}

while true; do
  tmp1=$(mktemp)
  tmp2=$(mktemp)
  tmp3=$(mktemp)

  get_block_header eu-central-1-teranode-teranet-prod-1 "$tmp1" &
  pid1=$!
  get_block_header eu-central-1-teranode-teranet-prod-2 "$tmp2" &
  pid2=$!
  get_block_header eu-central-1-teranode-teranet-prod-3 "$tmp3" &
  pid3=$!

  # Wait for both background processes to finish
  wait $pid1
  wait $pid2
  wait $pid3

  # Clear the screen and display results
  echo -ne "\033c"
  echo $(date -u)
  echo ""
  
  if [[ $(cat $tmp1) == $(cat $tmp2) ]] && [[ $(cat $tmp1) == $(cat $tmp3) ]]; then
    echo -e "\033[32m  t1: $(cat $tmp1)  \033[0m"
    echo -e "\033[32m  t2: $(cat $tmp2)  \033[0m"
    echo -e "\033[32m  t3: $(cat $tmp3)  \033[0m"
  else
    echo -e "\033[31m  t1: $(cat $tmp1)  \033[0m"
    echo -e "\033[31m  t2: $(cat $tmp2)  \033[0m"
    echo -e "\033[31m  t3: $(cat $tmp3)  \033[0m"
  fi
  echo ""

  # Clean up temporary files
  rm "$tmp1" "$tmp2" "$tmp3"

  # Countdown before the next update
  for (( i=10; i>0; i-- )); do
    echo -ne "  Refreshing in $i seconds (or press 'r' key) \r"
    # Set a timeout for read command
    read -t 1 -n 1 key
    if [ "$key" == "r" ]; then
        key="" # Clear the key
        echo -ne "  Refreshing immediately...                   \r"
        break # Exit the loop on key press
    fi
  done

  echo -ne "  Refreshing...                             \n" # Ensure newline at the end
done


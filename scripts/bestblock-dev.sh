#!/bin/bash

function get_block_header() {
  local node=$1
  local url="http://$node/api/v1/bestblockheader/json"
  curl -s "$url" | jq -r '. | "\(.height): \(.hash)"' > "$2" # Write output to temp file
}

function get_fsm_state() {
  local node=$1
  local url="http://$node/api/v1/fsm/state"
  curl -s "$url" | jq -r '.state' > "$2" # Write output to temp file
}

function get_service_heights() {
  local node=$1
  local url="http://$node/api/v1/service/heights"
  curl -s "$url" | jq -r '"Persister: \(.block_persister_height // "N/A") Assembly: \(.block_assembly_height // "N/A")"' > "$2"
}

while true; do
  tmp0=$(mktemp)
  tmp_fsm=$(mktemp)
  tmp_heights=$(mktemp)
  tmp1=$(mktemp)
  tmp2=$(mktemp)
  tmp3=$(mktemp)
  tmp4=$(mktemp)
  tmp5=$(mktemp)
  tmp6=$(mktemp)
  tmp_fsm1=$(mktemp)
  tmp_fsm2=$(mktemp)
  tmp_fsm3=$(mktemp)
  tmp_fsm4=$(mktemp)
  tmp_fsm5=$(mktemp)
  tmp_fsm6=$(mktemp)
  tmp_heights1=$(mktemp)
  tmp_heights2=$(mktemp)
  tmp_heights3=$(mktemp)
  tmp_heights4=$(mktemp)
  tmp_heights5=$(mktemp)
  tmp_heights6=$(mktemp)

  # dev
  get_block_header localhost:8090 "$tmp0" &
  pid0=$!
  get_fsm_state localhost:8090 "$tmp_fsm" &
  pid_fsm=$!
  get_service_heights localhost:8090 "$tmp_heights" &
  pid_heights=$!

  # docker host network
  get_block_header localhost:18090 "$tmp1" &
  pid1=$!
  get_fsm_state localhost:18090 "$tmp_fsm1" &
  pid_fsm1=$!
  get_service_heights localhost:18090 "$tmp_heights1" &
  pid_heights1=$!

  get_block_header localhost:28090 "$tmp2" &
  pid2=$!
  get_fsm_state localhost:28090 "$tmp_fsm2" &
  pid_fsm2=$!
  get_service_heights localhost:28090 "$tmp_heights2" &
  pid_heights2=$!

  get_block_header localhost:38090 "$tmp3" &
  pid3=$!
  get_fsm_state localhost:38090 "$tmp_fsm3" &
  pid_fsm3=$!
  get_service_heights localhost:38090 "$tmp_heights3" &
  pid_heights3=$!

  # smoke tests
  get_block_header localhost:10090 "$tmp4" &
  pid4=$!
  get_fsm_state localhost:10090 "$tmp_fsm4" &
  pid_fsm4=$!
  get_service_heights localhost:10090 "$tmp_heights4" &
  pid_heights4=$!

  get_block_header localhost:12090 "$tmp5" &
  pid5=$!
  get_fsm_state localhost:12090 "$tmp_fsm5" &
  pid_fsm5=$!
  get_service_heights localhost:12090 "$tmp_heights5" &
  pid_heights5=$!

  get_block_header localhost:14090 "$tmp6" &
  pid6=$!
  get_fsm_state localhost:14090 "$tmp_fsm6" &
  pid_fsm6=$!
  get_service_heights localhost:14090 "$tmp_heights6" &
  pid_heights6=$!

  # Wait for all background processes to finish
  wait $pid0
  wait $pid_fsm
  wait $pid_heights
  wait $pid1
  wait $pid_fsm1
  wait $pid_heights1
  wait $pid2
  wait $pid_fsm2
  wait $pid_heights2
  wait $pid3
  wait $pid_fsm3
  wait $pid_heights3
  wait $pid4
  wait $pid_fsm4
  wait $pid_heights4
  wait $pid5
  wait $pid_fsm5
  wait $pid_heights5
  wait $pid6
  wait $pid_fsm6
  wait $pid_heights6

  # Clear the screen and display results
  echo -ne "\033c"
  echo $(date -u)
  echo ""

  echo -e "\033[32m  localhost:8090: $(cat $tmp0)  \033[0m"
  echo -e "\033[32m  FSM State: $(cat $tmp_fsm) $(cat $tmp_heights)  \033[0m"
  echo ""

  if [[ $(cat $tmp1) == $(cat $tmp2) ]] && [[ $(cat $tmp2) == $(cat $tmp3) ]]; then
    echo -e "\033[32m  teranode1: $(cat $tmp1)  \033[0m"
    echo -e "\033[32m  FSM State: $(cat $tmp_fsm1) $(cat $tmp_heights1)  \033[0m"
    echo -e "\033[32m  teranode2: $(cat $tmp2)  \033[0m"
    echo -e "\033[32m  FSM State: $(cat $tmp_fsm2) $(cat $tmp_heights2)  \033[0m"
    echo -e "\033[32m  teranode3: $(cat $tmp3)  \033[0m"
    echo -e "\033[32m  FSM State: $(cat $tmp_fsm3) $(cat $tmp_heights3)  \033[0m"
  else
    echo -e "\033[31m  teranode1: $(cat $tmp1)  \033[0m"
    echo -e "\033[31m  FSM State: $(cat $tmp_fsm1) $(cat $tmp_heights1)  \033[0m"
    echo -e "\033[31m  teranode2: $(cat $tmp2)  \033[0m"
    echo -e "\033[31m  FSM State: $(cat $tmp_fsm2) $(cat $tmp_heights2)  \033[0m"
    echo -e "\033[31m  teranode3: $(cat $tmp3)  \033[0m"
    echo -e "\033[31m  FSM State: $(cat $tmp_fsm3) $(cat $tmp_heights3)  \033[0m"
  fi
  echo ""

  if [[ $(cat $tmp4) == $(cat $tmp5) ]] && [[ $(cat $tmp5) == $(cat $tmp6) ]]; then
    echo -e "\033[32m  teranode1-test: $(cat $tmp4)  \033[0m"
    echo -e "\033[32m  FSM State: $(cat $tmp_fsm4) $(cat $tmp_heights4)  \033[0m"
    echo -e "\033[32m  teranode2-test: $(cat $tmp5)  \033[0m"
    echo -e "\033[32m  FSM State: $(cat $tmp_fsm5) $(cat $tmp_heights5)  \033[0m"
    echo -e "\033[32m  teranode3-test: $(cat $tmp6)  \033[0m"
    echo -e "\033[32m  FSM State: $(cat $tmp_fsm6) $(cat $tmp_heights6)  \033[0m"
  else
    echo -e "\033[31m  teranode1-test: $(cat $tmp4)  \033[0m"
    echo -e "\033[31m  FSM State: $(cat $tmp_fsm4) $(cat $tmp_heights4)  \033[0m"
    echo -e "\033[31m  teranode2-test: $(cat $tmp5)  \033[0m"
    echo -e "\033[31m  FSM State: $(cat $tmp_fsm5) $(cat $tmp_heights5)  \033[0m"
    echo -e "\033[31m  teranode3-test: $(cat $tmp6)  \033[0m"
    echo -e "\033[31m  FSM State: $(cat $tmp_fsm6) $(cat $tmp_heights6)  \033[0m"
  fi
  echo ""

  # Clean up temporary files
  rm "$tmp0" "$tmp_fsm" "$tmp_heights"
  rm "$tmp1" "$tmp2" "$tmp3"
  rm "$tmp4" "$tmp5" "$tmp6"
  rm "$tmp_fsm1" "$tmp_fsm2" "$tmp_fsm3"
  rm "$tmp_fsm4" "$tmp_fsm5" "$tmp_fsm6"
  rm "$tmp_heights1" "$tmp_heights2" "$tmp_heights3"
  rm "$tmp_heights4" "$tmp_heights5" "$tmp_heights6"

  # Countdown before the next update
  for (( i=5; i>0; i-- )); do
    echo -ne "  Refreshing in $i seconds  \r"
    sleep 1
  done
  echo -ne "  Refreshing...                  \r"
done

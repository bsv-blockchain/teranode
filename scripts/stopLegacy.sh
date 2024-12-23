#!/bin/bash

# Kill the startLooper.sh process
start_looper_pid=$(ps -ef | grep './scripts/startLooper.sh' | grep -v grep | awk '{print $2}')
if [ -n "$start_looper_pid" ]; then
    kill $start_looper_pid
    echo "Killed startLooper.sh process with PID $start_looper_pid"
else
    echo "No startLooper.sh process found"
fi

# Kill the teranode.run process
teranode_run_pid=$(ps -ef | grep './teranode.run' | grep -v grep | awk '{print $2}')
if [ -n "$teranode_run_pid" ]; then
    kill $teranode_run_pid
    echo "Killed teranode.run process with PID $teranode_run_pid"
else
    echo "No teranode.run process found"
fi
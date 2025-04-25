#!/bin/bash
# wait-for-port.sh

set -e

host="$1"
port="$2"
waitfor="$3"
shift 3
cmd="$@"

until nc -z "$host" "$port"; do
  >&2 echo "$host:$port is unavailable - sleeping"
  sleep 1
done

>&2 echo "$host:$port is up - waiting for $waitfor seconds before executing command"
sleep $waitfor

>&2 echo "$host:$port is up - executing command"
exec $cmd

if [ "$host" = "postgres" ]; then
  sleep 10
fi

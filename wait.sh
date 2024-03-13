#!/bin/bash
# wait-for-port.sh

set -e

host="$1"
port="$2"
waitfor="$3"
shift 3
cmd="$@"

until nc -z "$host" "$port"; do
  >&2 echo "$host is unavailable - sleeping"
  sleep 1
done

>&2 echo "$host is up - waiting for $waitfor seconds before executing command"
sleep $waitfor

>&2 echo "$host is up - executing command"
exec $cmd

#!/bin/bash
host_port=$1
shift

IFS=':' read host port <<< "$host_port"

echo "Connecting to $host:$port..."

until nc -z "$host" "$port"; do
  echo "Waiting for $host:$port..."
  sleep 2
done

echo "Connection successful!"

exec "$@"
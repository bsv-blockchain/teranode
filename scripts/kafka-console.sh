#!/bin/sh

docker run --rm -d --name kafka-console \
  --network app-tier \
  -p 8080:8080 \
  -e KAFKA_BROKERS=kafka-server:9092 \
  docker.redpanda.com/redpandadata/console:latest

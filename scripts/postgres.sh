#!/bin/bash

# Use DATADIR environment variable if set, otherwise default to ./data
DATADIR="${DATADIR:-./data}"

docker run -d \
  --name postgres \
  -p 5432:5432 \
  -e POSTGRES_USER=teranode \
  -e POSTGRES_PASSWORD=teranode \
  -e PGDATA=/var/lib/postgresql/data/pgdata \
  -v ${DATADIR}/postgres:/var/lib/postgresql/data \
  postgres:latest

echo "PostgreSQL started with persistent data folder in '${DATADIR}/postgres'"

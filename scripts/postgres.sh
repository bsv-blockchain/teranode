#!/bin/bash

docker run --rm -d \
  --name postgres \
  -p 5432:5432 \
  -e POSTGRES_USER=teranode \
  -e POSTGRES_PASSWORD=teranode \
  -e PGDATA=/var/lib/postgresql/data/pgdata \
  -v ./data/postgres:/var/lib/postgresql/data \
  postgres:latest

echo "PostgreSQL started with persistent data folder in './data/postgres'"

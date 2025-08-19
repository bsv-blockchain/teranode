#!/bin/bash

# Create a volume for persistent PostgreSQL data if it doesn't exist
docker volume create postgres-data 2>/dev/null || true

docker run --rm -d \
  --name postgres \
  -p 5432:5432 \
  -e POSTGRES_USER=teranode \
  -e POSTGRES_PASSWORD=teranode \
  -e PGDATA=/var/lib/postgresql/data/pgdata \
  -v postgres-data:/var/lib/postgresql/data \
  postgres:latest

echo "PostgreSQL started with persistent volume 'postgres-data'"
echo "To remove the volume: docker volume rm postgres-data"

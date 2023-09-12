#!/bin/sh

docker network create app-tier --driver bridge

docker run -d --name minio-server \
  --publish 9000:9000 \
  --publish 9001:9001 \
  --env MINIO_ROOT_USER="minio-root-user" \
  --env MINIO_ROOT_PASSWORD="minminio-root-passwordio" \
  --env MINIO_SERVER_ACCESS_KEY="minio-access-key" \
  --env MINIO_SERVER_SECRET_KEY="minio-secret-key" \
  --network app-tier \
  bitnami/minio:latest

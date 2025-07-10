#!/bin/bash

docker run --rm -d --name postgres -p 5432:5432 -e POSTGRES_USER=teranode -e POSTGRES_PASSWORD=teranode -d postgres postgres

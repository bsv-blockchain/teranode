#!/bin/bash

docker run -d --rm --name aerospike -p 3000-3002:3000-3002 aerospike/aerospike-server

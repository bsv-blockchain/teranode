#!/bin/bash

# Remove the data folder...
rm -rf "$(dirname "$0")/../data"

# Drop all postgres tables
psql postgres://ubsv:ubsv@localhost:5432/ubsv -c "SET client_min_messages TO WARNING; drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists blocks;"
psql postgres://ubsv:ubsv@localhost:5432/coinbase -c "SET client_min_messages TO WARNING; drop table if exists coinbase_utxos; drop table if exists spendable_utxos; drop table if exists blocks; drop table if exists state;"

# Flush redis
redis-cli FLUSHALL

# Flush aerospike
# aql -c "truncate test.utxo;"

asinfo -h localhost -v "truncate-namespace:namespace=ubsv-store"
asinfo -h localhost -v "truncate-namespace:namespace=test"

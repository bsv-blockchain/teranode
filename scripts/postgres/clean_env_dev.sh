#!/bin/bash

# Source the utilities
source "$(dirname "$0")/db_utils.sh"

# Get credentials
get_credentials

# drop all tables for local postgres dbs
psql "$(get_pg_url teranode)" -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists blocks;"
psql "$(get_pg_url coinbase)" -c "drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists blocks;"
# How to Reset Teranode (Docker Compose)

Last modified: 29-October-2025

## Index

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Simple Method: Delete Data Folder](#simple-method-delete-data-folder)
- [Granular Method: Per-Service Cleanup](#granular-method-per-service-cleanup)
    - [Aerospike Cleanup](#aerospike-cleanup)
    - [PostgreSQL Cleanup](#postgresql-cleanup)
    - [Filesystem Cleanup](#filesystem-cleanup)
- [Post-Reset Steps](#post-reset-steps)
    - [Verify Services Are Running](#verify-services-are-running)
    - [Set FSM State](#set-fsm-state)
    - [Monitor Synchronization](#monitor-synchronization)
    - [Consider Data Seeding](#consider-data-seeding)
- [Related Documentation](#related-documentation)

## Introduction

If you need to sync Teranode from scratch, restore from a backup, or clean up after testing, you'll need to reset the blockchain data. This guide provides reset procedures for Docker Compose deployments.

**When to reset:**

- Starting a fresh synchronization from genesis
- Switching between networks (mainnet/testnet)
- Recovering from data corruption
- Cleaning up after testing or development
- Preparing for a data seeding operation

## Prerequisites

- **Backup important data** before proceeding (if applicable)
- **Stop all Teranode services** before reset
- **Root or sudo access** for file operations
- **Docker Compose environment** properly configured

> **⚠️ Warning:** These operations are **irreversible** and will delete all blockchain data. Ensure you have backups if needed.

## Simple Method: Delete Data Folder

This is the **fastest and easiest method** for Docker Compose. It removes all data including Aerospike, PostgreSQL, Kafka, and filesystem data.

```bash
# Stop all services
docker compose down

# Navigate to your deployment directory
cd $YOUR_WORKING_DIR/teranode/deploy/docker/testnet
# Or for mainnet:
# cd $YOUR_WORKING_DIR/teranode/deploy/docker/mainnet

# Delete all data
sudo rm -rf ./data/*

# Restart services
docker compose up -d
```

**What this deletes:**

- Aerospike UTXO data (`./data/aerospike/`)
- PostgreSQL blockchain data (`./data/postgres/`)
- Kafka message data (`./data/kafka/`)
- Teranode filesystem data (`./data/teranode/`)

## Granular Method: Per-Service Cleanup

For more control or when debugging specific components, you can clean up each service individually.

### Aerospike Cleanup

```bash
# Access Aerospike container
docker exec -it aerospike /bin/bash

# Truncate the UTXO set
asadm --enable -e "manage truncate ns utxo-store"

# Verify the total records count (should slowly decrease to 0)
asadm -e "info"
```

See the [Aerospike documentation](https://aerospike.com/docs/server/operations/manage/sets#truncating-a-set-in-a-namespace) for more information.

### PostgreSQL Cleanup

```bash
# Access Postgres container
docker exec -it postgres psql -U postgres

# Connect to the database used by Teranode
postgres=> \c teranode

# Sanity count check
teranode=> SELECT COUNT(*) FROM blocks;
 count
--------
 123123
(1 row)

# Truncate the blocks and state tables
TRUNCATE TABLE blocks RESTART IDENTITY CASCADE;
TRUNCATE TABLE state RESTART IDENTITY CASCADE;
TRUNCATE TABLE bans RESTART IDENTITY CASCADE;


# Verify the table was truncated
SELECT COUNT(*) FROM blocks;
 count
-------
     0
(1 row)

# (Optional) Drop indexes for faster seeding
# The blockchain service will recreate them automatically
DROP INDEX IF EXISTS idx_chain_work_id;
DROP INDEX IF EXISTS idx_chain_work_peer_id;

# Exit psql
\q
```

> **💡 Performance Tip:** Dropping the indexes before seeding significantly speeds up the initial data load. The blockchain service will recreate them automatically on startup.

### Filesystem Cleanup

We can also remove the Teranode filesystem data only, preserving the Aerospike, PostgreSQL, Nginx, Grafana and Prometheus data.

```bash
# Stop Teranode services (keep databases running if using granular cleanup)
docker compose stop blockchain asset blockassembly blockvalidation subtreevalidation legacy rpc peer propagation miner

# Clean up Teranode filesystem data only
sudo rm -rf ./data/teranode/*

# Restart Teranode services
docker compose up -d
```

## Post-Reset Steps

After resetting Teranode, follow these steps to resume operations:

### Verify Services Are Running

```bash
docker compose ps
```

### Set FSM State

Teranode starts in IDLE state. You need to transition to the appropriate state:

```bash
# Via Admin Dashboard (easiest)
# Open http://localhost:8090/admin

# Or via teranode-cli
docker exec -it blockchain teranode-cli setfsmstate --fsmstate LEGACYSYNCING
# Or for direct operation:
docker exec -it blockchain teranode-cli setfsmstate --fsmstate RUNNING
```

### Monitor Synchronization

```bash
# View blockchain logs
docker compose logs -f blockchain

# View blockchain info in dashboard
# Open http://localhost:8090/viewer
```

### Consider Data Seeding

For faster synchronization, consider using data seeding instead of syncing from genesis. See the [How to Sync the Node](minersHowToSyncTheNode.md) guide for seeding procedures.

## Related Documentation

- [How to Sync the Node](minersHowToSyncTheNode.md) - Blockchain synchronization methods
- [Docker Installation Guide](minersHowToInstallation.md) - Docker Compose setup
- [How to Interact with FSM](../minersHowToInteractWithFSM.md) - FSM state management

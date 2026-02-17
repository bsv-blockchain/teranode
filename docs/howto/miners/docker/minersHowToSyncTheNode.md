# Syncing the Blockchain (Docker Compose)

Last modified: 29-October-2025

## Table of Contents

- [Overview](#overview)
- [Synchronization Methods Comparison](#synchronization-methods-comparison)
- [Method 1: Default Network Sync (P2P)](#method-1-default-network-sync-p2p)
- [Method 2: Seeding from Legacy SV Node](#method-2-seeding-from-legacy-sv-node)
- [Method 3: Seeding from Existing Teranode](#method-3-seeding-from-existing-teranode)
- [Recovery and Troubleshooting](#recovery-and-troubleshooting)
- [Monitoring and Verification](#monitoring-and-verification)
- [Additional Resources](#additional-resources)

## Overview

This guide covers the different methods available for synchronizing a Teranode instance with the Bitcoin SV blockchain using Docker Compose. Whether you're setting up a fresh node or recovering from downtime, this document will help you choose the most appropriate synchronization method for your situation.

> **📋 Network Scope:** This guide applies to **both mainnet and testnet** deployments. All command examples use mainnet paths and settings for illustration - adjust the `network` parameter and data paths according to your target network (e.g., replace `mainnet` with `testnet` in paths and environment variables).

---

## Synchronization Methods Comparison

Choose the synchronization method that best fits your situation:

| Method | Use Case | Advantages | Disadvantages | Time Required |
|--------|----------|------------|---------------|---------------|
| **Default Network Sync** | Fresh install, no existing data | • Simple setup<br>• No additional requirements<br>• Complete validation | • Slowest method<br>• High bandwidth usage | 5-8 days |
| **Legacy SV Node Seeding** | Have existing BSV node | • Faster than P2P<br>• Proven data source<br>• Reduced bandwidth | • Requires SV Node setup<br>• Additional export steps | 1 Hour<br>(assumes SV node<br>already in sync) |
| **Teranode Data Seeding** | Have existing Teranode | • Fastest method<br>• Direct data transfer<br>• Minimal processing | • Requires access to existing data<br>• Version compatibility needed | 1 Hour |

![seedingOptions.svg](../img/mermaid/seedingOptions.svg)

> **💡 Recommendation:** For production deployments, we recommend using the Legacy SV Node seeding method when possible, as it provides the best balance of speed and data integrity verification.

---

## Method 1: Default Network Sync (P2P)

This is the standard synchronization method where Teranode downloads the complete blockchain from other nodes in the network via peer-to-peer connections.

### Prerequisites

- ✅ Teranode instance deployed and running
- ✅ Network connectivity to BSV peers
- ✅ Sufficient storage space (minimum 4TB with default prune settings of 288 blocks)
- ✅ Stable internet connection with adequate bandwidth

> **⚠️ Important:** This method can take around 14 days depending on your network connection and hardware specifications.

### Process Overview

![syncStateFlow.svg](../img/mermaid/syncStateFlow.svg)

### Step 1: Initialize Sync Process

Upon startup, Teranode begins in IDLE state. You must explicitly set the state to `legacysyncing` to begin synchronization.

```bash
# Set FSM state to begin legacy syncing
docker exec -it blockchain teranode-cli setfsmstate --fsmstate legacysyncing
```

### Step 2: Peer Discovery and Block Download

- **Peer Connection**: Teranode automatically discovers and connects to BSV network peers
- **Block Requests**: Downloads blocks sequentially from genesis, starting with the first available peer
- **Legacy Mode**: In `legacysyncing` state, connects to traditional BSV nodes for compatibility

### Step 3: Validation and Storage

As blocks are received, multiple Teranode services work in parallel:

- **Block Validation**: `block-validator` service validates block headers and structure
- **Subtree Validation**: `subtree-validator` service validates transaction subtrees
- **Storage**: `blockchain` service stores validated blocks in the database
- **UTXO Updates**: `asset` service maintains the UTXO set

### Step 4: Monitor Progress

```bash
# View real-time sync logs
docker compose logs -f blockchain

# Check service health
docker compose ps

# View blockchain info in the blockchain viewer
# Access http://localhost:8090/viewer in your browser
```

### Expected Timeline

| Phase | Duration | Description |
|-------|----------|-------------|
| **Initial Setup** | 5-10 minutes | Peer discovery and connection establishment |
| **Early Blocks** | 1-2 days | Genesis to block ~500,000 (smaller blocks, faster processing) |
| **Recent Blocks** | 3-6 days | Block ~500,000 to current tip (larger blocks, slower processing) |
| **Catch-up** | Ongoing | Maintaining sync with new blocks |

> **📊 Performance Tip:** Monitor your system resources during sync. CPU and I/O intensive operations are normal during this process.

---

## Method 2: Seeding from Legacy SV Node

This method allows you to bootstrap a Teranode instance using data exported from an existing Bitcoin SV node (bitcoind). This significantly reduces synchronization time compared to P2P sync.

### Prerequisites

- ✅ Access to a fully synchronized Bitcoin SV node (bitcoind)
- ✅ SV Node gracefully shut down (using `bitcoin-cli stop`)
- ✅ Fresh Teranode instance with no existing blockchain data (see [reset guide](minersHowToResetTeranode.md) to clear existing data if needed)
- ✅ Sufficient disk space for export files (~1TB recommended, temporary during process)
- ✅ Sufficient disk space for Teranode data (~10TB recommended, permanent)
- ✅ Docker Compose environment set up

> **⚠️ Critical:** Only perform this operation on a gracefully shut down SV Node to ensure data consistency.

### Overview

This process involves two main phases:

1. **Export Phase**: Convert SV Node database data (UTXO set and headers) into a Teranode-compatible format
2. **Seeding Phase**: Import the converted data files into Teranode

> **💡 What's happening:** The `bitcointoutxoset` tool reads the SV Node's LevelDB database files (`chainstate` and `blocks`) and converts them into Teranode's native format for fast import.

---

### Phase 1: Convert SV Node Data to Teranode Format

#### Step 1: Verify SV Node Requirements

Before proceeding with the export, ensure your Bitcoin SV node meets these critical requirements:

!!! info "Data Location Requirements"
    - For this guide, we assume SV node data is located at `/mnt/bitcoin-sv-data`
    - Replace this path with your actual SV node data directory in all commands
    - Verify the directory contains both `blocks` and `chainstate` subdirectories

!!! warning "Node State Requirements"
    - **✅ SV Node must be completely stopped** - Use `bitcoin-cli stop` for graceful shutdown
    - **❌ Node cannot be actively syncing** - Export will fail if the node is processing blocks
    - **❌ Node cannot have active network connections** during export

#### Optional: Ensure Data Consistency

If you failed to stop gracefully, and you want to guarantee data consistency before export, you can start the SV node one final time in isolated mode:

```bash
# Start SV node in isolated mode (no network activity)
bitcoind -listen=0 -connect=0 -daemon

# Wait for any pending operations to complete (check logs)
tail -f ~/.bitcoin/debug.log

# Once stable, stop gracefully
bitcoin-cli stop
```

!!! danger "Critical Warning"
    The SV node must be completely stopped before proceeding. Active syncing or network activity during export will cause data corruption or export failure.

#### Step 2: Prepare Export Directory

```bash
# Create export directory
sudo mkdir -p /mnt/teranode/seed/export
sudo chown $USER:$(id -gn $USER) /mnt/teranode/seed/export
```

#### Step 3: Export UTXO Data

```bash
# Export UTXO set from Bitcoin SV node
# Replace /mnt/bitcoin-sv-data with your actual SV node data directory
# Instead of latest, you could use a specific version of teranode. 
# Check for tagged versions here ghcr.io/bsv-blockchain/teranode
docker run -it \
    -v /mnt/bitcoin-sv-data:/home/ubuntu/bitcoin-data:ro \
    -v /mnt/teranode/seed:/mnt/teranode/seed \
    --entrypoint="" \
    ghcr.io/bsv-blockchain/teranode:latest \
    /app/teranode-cli bitcointoutxoset \
        -bitcoinDir=/home/ubuntu/bitcoin-data \
        -outputDir=/mnt/teranode/seed/export
```

**Expected Output Files:**

- `{blockhash}.utxo-headers` - Block headers data
- `{blockhash}.utxo-set` - UTXO set data

#### Step 4: Verify Export

```bash
# Check exported files
ls -la /mnt/teranode/seed/export/
# You should see .utxo-headers and .utxo-set files
```

> **🔧 Troubleshooting:** If you encounter a `Block hash mismatch between last block and chainstate` error, restart your SV Node once more with `bitcoin-cli stop` followed by a clean restart.

#### ⚠️ Important: Cleanup for Retries

If you encounter issues during export or need to retry the process:

1. **Clear the export directory completely:**

    ```bash
    sudo rm -rf /mnt/teranode/seed/export/*
    ```

2. **Reset the target Teranode instance** (see [reset guide](minersHowToResetTeranode.md))

3. **Verify SV node was gracefully shutdown** and repeat from Step 1

!!! note "Important for Retries"
    Partial or corrupted export files can cause seeding failures. Always start with a clean export directory and fresh Teranode instance when retrying.

---

### Phase 2: Seed Teranode with Exported Data

!!! warning "Critical Service Requirements"
    During the seeding process, only the following services should be running for Teranode:

    - **Aerospike** (database)
    - **Postgres** (database)
    - **Kafka** (messaging)

    **All other Teranode services must be stopped** (blockchain, asset, blockvalidation, etc.) to prevent conflicts during data import.

!!! danger "Network Configuration Alignment"
    The export and import target networks must be consistent:

    - If SV node was running **mainnet**, Teranode must be configured for **mainnet**
    - If SV node was running **testnet**, Teranode must be configured for **testnet**
    - **Check development configurations** - Some setups may be set to "regtest" mode
    - **Verify network settings** in Teranode configuration before proceeding with Step 1

    **Configuration Examples:**

    - **Mainnet:** Use `network=mainnet` and `docker/mainnet/data/teranode` path
    - **Testnet:** Use `network=testnet` and `docker/testnet/data/teranode` path

    **Mismatched networks will cause seeding failure or data corruption.**

#### Step 1: Prepare Teranode Environment

```bash
# Start required services (adjust service names as needed)
docker compose up -d aerospike postgres kafka-shared

# Verify services are running
docker compose ps

# CRITICAL: Ensure Teranode services are NOT running
docker compose stop blockchain asset blockvalidation # Add other services as needed
```

#### Step 2: Identify the Block Hash

Before running the seeder, you need to identify the correct block hash from your exported files:

```bash
# List the exported files to see the block hash
ls -la /mnt/teranode/seed/export/

# You should see files like:
# 0000000000013b8ab2cd513b0261a14096412195a72a0c4827d229dcc7e0f7af.utxo-headers
# 0000000000013b8ab2cd513b0261a14096412195a72a0c4827d229dcc7e0f7af.utxo-set
```

!!! info "Understanding the Hash Parameter"
    The `-hash` parameter specifies the **block hash of the last block** in the exported UTXO set. This hash:

    - **Identifies the exported files** - Files are named `{blockhash}.utxo-headers` and `{blockhash}.utxo-set`
    - **Represents the blockchain tip** at the time of export from the SV node
    - **Must match exactly** with the filenames in your export directory

    **How to find it:** Extract the hash from your exported filenames (the part before `.utxo-headers` or `.utxo-set`)

#### Step 3: Run Seeder

```bash
# Run the seeder (replace the hash with your actual block hash from Step 2)
# Make sure to add any environment variables you have defined in your docker-compose.yml
# NOTE: This example uses mainnet - for testnet, change 'network=testnet' and 'docker/testnet/data/teranode'
docker run -it \
    -e SETTINGS_CONTEXT=docker.m \
    -e network=mainnet \
    -v ${PWD}/docker/mainnet/data/teranode:/app/data \
    -v /mnt/teranode/seed:/mnt/teranode/seed \
    --network my-teranode-network \
    --entrypoint="" \
    ghcr.io/bsv-blockchain/teranode:latest \
    /app/teranode-cli seeder \
        -inputDir /mnt/teranode/seed/export \
        -hash 0000000000013b8ab2cd513b0261a14096412195a72a0c4827d229dcc7e0f7af
```

#### Step 4: Monitor Seeding Progress

```bash
# Monitor seeder logs
docker logs -f <seeder-container-id>
```

#### Step 5: Start Teranode Services

After successful seeding:

```bash
# Start all Teranode services
docker compose up -d
```

### Expected Timeline

| Phase | Duration | Description |
|-------|----------|-------------|
| **Export** | 2-4 hours | Extracting UTXO set from SV Node |
| **Seeding** | 4-8 hours | Importing data into Teranode |
| **Verification** | 30 minutes | Starting services and verifying sync |
| **Total** | 1-12 hours | Complete process |

> **💾 Storage Note:** The seeder writes directly to Aerospike, PostgreSQL, and the filesystem. Ensure your environment variables and volume mounts are correctly configured.

---

## Method 3: Seeding from Existing Teranode

This is the fastest synchronization method, using UTXO set and header files from an existing Teranode instance. This method is ideal when you have access to another synchronized Teranode node.

### Prerequisites

- ✅ Access to a synchronized Teranode instance
- ✅ UTXO set files from Block/UTXO Persister
- ✅ Fresh target Teranode instance with no existing blockchain data (see [reset guide](minersHowToResetTeranode.md) to clear existing data if needed)
- ✅ Network access between source and target systems
- ✅ Sufficient storage for data transfer

### Overview

This method uses the same seeder tool as Method 2, but with data files generated by Teranode's Block Persister and UTXO Persister services instead of exporting from a legacy SV node.

### Step 1: Locate Source Data

On your source Teranode instance, locate the persisted data files:

```bash
# Typical locations for persisted data in Docker deployments
ls -la /app/data/blockstore/
ls -la /app/data/utxo-persister/
```

**Required Files:**

- `{blockhash}.utxo-headers` - Block headers
- `{blockhash}.utxo-set` - UTXO set data

### Step 2: Make Data Available

Ensure the required UTXO files are available in your target Teranode's export directory:

```bash
# Target location for the files
/mnt/teranode/seed/export/{blockhash}.utxo-headers
/mnt/teranode/seed/export/{blockhash}.utxo-set
```

!!! note "Data Transfer"
    Use your preferred method to transfer the files from the source Teranode to the target location (scp, rsync, container volumes, shared storage, etc.).

### Step 3: Run Seeder

Use the same seeder process as described in Method 2:

```bash
# Prepare environment
docker compose up -d aerospike aerospike-2 postgres kafka-shared
docker compose stop blockchain asset blockvalidation

# Run seeder
# Instead of latest, you could use a specific version of teranode.
# Check for tagged versions here ghcr.io/bsv-blockchain/teranode
# NOTE: This example uses mainnet - for testnet, change to 'docker/testnet/data/teranode'
docker run -it \
    -e SETTINGS_CONTEXT=docker.m \
    -v ${PWD}/docker/mainnet/data/teranode:/app/data \
    -v /mnt/teranode/seed:/mnt/teranode/seed \
    --network my-teranode-network \
    --entrypoint="" \
    ghcr.io/bsv-blockchain/teranode:latest \
    /app/teranode-cli seeder \
        -inputDir /mnt/teranode/seed/export \
        -hash <blockhash-from-filename>
```

### Expected Timeline

| Phase | Duration | Description |
|-------|----------|-------------|
| **Data Transfer** | 1-3 hours | Copying files between systems |
| **Seeding** | 2-4 hours | Importing data into target Teranode |
| **Verification** | 15 minutes | Starting services and verification |
| **Total** | 1-6 hours | Complete process |

> **⚡ Speed Advantage:** This method is typically 2-3x faster than Method 2 since the data is already in Teranode's optimized format.

---

## Recovery and Troubleshooting

Teranode is designed for resilience and can recover from various types of downtime or disconnections. This section covers different recovery scenarios and troubleshooting approaches.

### Normal Recovery (Short Downtime)

For brief interruptions (minutes to hours), Teranode typically recovers automatically.

#### Automatic Recovery Process

1. **Service Restart**
    - Docker Compose can be configured with restart policies

2. **Peer Reconnection**
    - The `peer` service re-establishes network connections
    - Automatic discovery of available BSV network peers

3. **Block Catch-up**
    - Node determines last processed block height
    - Requests missing blocks from peers automatically
    - Processes blocks sequentially to catch up

#### Monitor Normal Recovery

```bash
# Check container status
docker compose ps

# View recovery logs
docker compose logs -f blockchain --tail=100

# View sync progress in blockchain viewer
# Access http://localhost:8090/viewer in your browser
```

### Extended Downtime Recovery

For longer outages (days to weeks), additional considerations apply.

#### Assessment Phase

Check current sync status:

```bash
# Check current block height vs network tip using the blockchain viewer
# Access http://localhost:8090/viewer in your browser
# Compare the displayed height with a block explorer

# If >10,000 blocks behind, consider reseeding
```

#### Recovery Options

| Blocks Behind | Recommended Action | Expected Time |
|---------------|-------------------|---------------|
| **< 1,000** | Normal catch-up | 1-4 hours |
| **1,000 - 10,000** | Monitor catch-up progress | 4-24 hours |
| **> 10,000** | Consider reseeding (Method 2 or 3) | 6-12 hours |

#### Manual Intervention Steps

##### Option 1: Force Catch-up

```bash
# Reset FSM state and restart sync
docker exec -it blockchain teranode-cli setfsmstate --fsmstate legacysyncing

# Monitor progress closely
docker compose logs -f blockchain
```

##### Option 2: Reseed from Recent Data

If catch-up is too slow, use Method 2 or Method 3 from this guide with recent data.

### Troubleshooting Common Issues

#### Issue: Sync Stalled

**Symptoms:**

- Block height not increasing
- No new blocks being processed
- Peer connections established but inactive

**Solutions:**

```bash
# Check peer connections
docker exec -it blockchain teranode-cli getpeerinfo

# Restart peer service
docker compose restart peer

# Reset FSM state
docker exec -it blockchain teranode-cli setfsmstate --fsmstate legacysyncing
```

#### Issue: Database Connection Errors

**Symptoms:**

- Connection timeouts to PostgreSQL/Aerospike
- "Database unavailable" errors in logs

**Solutions:**

```bash
# Check database container status
docker compose ps | grep -E "postgres\|aerospike"

# Test database connectivity
docker exec -it postgres psql -U postgres -d teranode -c "SELECT 1;"

# Restart database containers if needed
docker compose restart postgres aerospike
```

#### Issue: Storage Space Exhausted

**Symptoms:**

- "No space left on device" errors
- Containers crashing

**Solutions:**

```bash
# Check storage usage
df -h

# Clean up old logs (if applicable)
docker compose logs --tail=0

# Ensure log rotation is configured
```

### Performance Monitoring During Recovery

#### Key Metrics to Watch

- **Block Processing Rate**: Blocks per minute
- **Peer Connection Count**: Active peer connections
- **Database Performance**: Query response times
- **Storage I/O**: Disk read/write rates
- **Memory Usage**: RAM consumption patterns

#### Monitoring Commands

```bash
# Monitor block height progress via blockchain viewer
# Access http://localhost:8090/viewer in your browser

# Monitor resource usage
docker stats

# Check service health endpoints
docker exec -it blockchain curl -s http://localhost:8000/health
```

> **📈 Tip:** Use monitoring tools like Prometheus and Grafana to track recovery progress and estimate completion times. Set up alerts for stalled sync or resource exhaustion.

---

## Monitoring and Verification

After completing synchronization using any method, it's important to verify that your Teranode instance is properly synchronized and functioning correctly.

### Verification Checklist

#### ✅ Basic Connectivity

```bash
# Check if services are running
docker compose ps
# All services should show "Up" status

# Test CLI connectivity
docker exec -it blockchain teranode-cli getfsmstate
```

#### ✅ Synchronization Status

```bash
# Check current block height using blockchain viewer
# Access http://localhost:8090/viewer in your browser

# Compare with network tip (use block explorer or other nodes)
# Heights should match within 1-2 blocks
```

#### ✅ Peer Connections

```bash
# Verify peer connections
docker exec -it blockchain teranode-cli getpeerinfo | grep -E "addr|version"

# Should have 8+ active peer connections
```

#### ✅ Database Health

```bash
# Check PostgreSQL connectivity
docker exec -it postgres psql -U postgres -d teranode -c "SELECT COUNT(*) FROM blocks;"

# Check Aerospike connectivity
docker exec -it aerospike asinfo -v "statistics" | grep "objects"
```

#### ✅ FSM State

```bash
# Verify FSM is in correct state
docker exec -it blockchain teranode-cli getfsmstate

# Should show "RUNNING" for a fully synchronized node
```

### Ongoing Monitoring

#### Daily Health Checks

```bash
#!/bin/bash
# Save as teranode-health-check.sh

echo "=== Teranode Health Check ==="
echo "Timestamp: $(date)"

# Check container status
echo "\n--- Container Status ---"
docker compose ps

# Check block height via blockchain viewer
echo "\n--- Block Height ---"
echo "View at http://localhost:8090/viewer"

# Check peer count
echo "\n--- Peer Count ---"
docker exec -it blockchain teranode-cli getpeerinfo | wc -l

# Check FSM state
echo "\n--- FSM State ---"
docker exec -it blockchain teranode-cli getfsmstate
```

#### Performance Metrics

```bash
# Monitor resource usage
docker stats --no-stream

# Check storage usage
df -h

# Monitor network traffic
docker exec -it blockchain netstat -i
```

#### Log Analysis

```bash
# Check for errors in recent logs
docker compose logs blockchain --tail=1000 | grep -i error

# Monitor sync progress
docker compose logs -f blockchain | grep -E "height|block|sync"

# Check for warnings
docker compose logs blockchain --tail=1000 | grep -i warn
```

---

## Additional Resources

### Documentation

- **[Teranode CLI Reference](../minersHowToTeranodeCLI.md)** - Complete CLI command reference
- **[Reset Teranode (Docker)](minersHowToResetTeranode.md)** - How to reset Docker Compose deployments
- **[Seeder Command Details](../../../topics/commands/seeder.md)** - Advanced seeder configuration
- **[UTXO Persister Service](../../../topics/services/utxoPersister.md)** - UTXO persistence configuration
- **[UTXO Store Documentation](../../../topics/stores/utxo.md)** - UTXO storage mechanisms

### Installation and Configuration

- **[Docker Installation](minersHowToInstallation.md)** - Docker-based setup
- **[Docker Configuration](minersHowToConfigureTheNode.md)** - Docker configuration guide
- **[Third Party Requirements](../../../references/thirdPartySoftwareRequirements.md)** - External dependencies

### Support and Community

For additional support:

1. **Check Logs**: Always start with examining service logs
2. **Documentation**: Review related documentation sections
3. **Community Forums**: Search existing discussions and solutions
4. **Issue Reporting**: Report bugs with detailed logs and reproduction steps

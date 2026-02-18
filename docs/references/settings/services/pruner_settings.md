# Pruner Service Settings Reference

## Overview

This document provides comprehensive reference for all Pruner service configuration settings in Teranode.

## Service Control Settings

### startPruner

**Type**: Boolean

**Default**: `true`

**Description**: Enable or disable the Pruner service

**Context-Specific Values:**

```conf
# Global default
startPruner = true

# Disable for specific nodes in multi-node docker setup
startPruner.docker.host.teranode1.coinbase = false
startPruner.docker.host.teranode2.coinbase = false
startPruner.docker.host.teranode3.coinbase = false

# Development
startPruner.dev = true

# Operator/Kubernetes
startPruner.operator = true
```

**Impact:**

- `true`: Pruner service starts and performs UTXO pruning
- `false`: Pruner service disabled, UTXO database will grow unbounded

**When to Disable:**

- Testing scenarios requiring full UTXO history
- Debugging transaction issues
- Temporary workaround for pruning errors

**Warning**: Disabling pruning will cause the UTXO database to grow continuously. Only disable temporarily.

## Network Settings

### pruner_grpcPort

**Type**: Integer

**Default**: `8096`

**Description**: gRPC server port for Pruner service

**Example:**

```conf
PRUNER_GRPC_PORT = 8096
```

**Port Conflicts:**

If port 8096 is already in use, change to an available port:

```conf
PRUNER_GRPC_PORT = 8097
pruner_grpcAddress = localhost:8097
pruner_grpcListenAddress = :8097
```

### pruner_grpcAddress

**Type**: String

**Default**: `localhost:8096`

**Description**: gRPC client address for connecting to Pruner service

**Context-Specific Values:**

```conf
# Development (default)
pruner_grpcAddress = localhost:${PRUNER_GRPC_PORT}

# Docker multi-node (single machine)
pruner_grpcAddress.docker.m = pruner:${PRUNER_GRPC_PORT}

# Docker (per-service containers)
pruner_grpcAddress.docker = ${clientName}:${PRUNER_GRPC_PORT}

# Docker host access
pruner_grpcAddress.docker.host = localhost:${PORT_PREFIX}${PRUNER_GRPC_PORT}

# Kubernetes/Operator
pruner_grpcAddress.operator = k8s:///pruner.${clientName}.svc.cluster.local:${PRUNER_GRPC_PORT}
```

**Usage**: Other services use this address to connect to Pruner's gRPC API

### pruner_grpcListenAddress

**Type**: String

**Default**: `:8096`

**Description**: gRPC server listen address (bind address)

**Context-Specific Values:**

```conf
# Default (all interfaces)
pruner_grpcListenAddress = :${PRUNER_GRPC_PORT}

# Development (localhost only)
pruner_grpcListenAddress.dev = localhost:${PRUNER_GRPC_PORT}

# Docker host (localhost with port prefix)
pruner_grpcListenAddress.docker.host = localhost:${PORT_PREFIX}${PRUNER_GRPC_PORT}
```

**Bind Addresses:**

- `:8096` - Listen on all interfaces
- `localhost:8096` - Listen only on localhost (more secure)
- `0.0.0.0:8096` - Explicitly listen on all IPv4 interfaces

## Behavior Control Settings

### pruner_skipDuringCatchup

**Type**: Boolean

**Default**: `false`

**Environment Variable**: `pruner_skipDuringCatchup`

**Description**: Skip pruning during blockchain catchup

When enabled, the pruner checks FSM state and skips all deletion operations during catchup. This prevents race conditions where block validation marks transactions as mined faster than the pruner can preserve their parents.

**Values:**

- `false` (default): Normal pruning during catchup (safe with retention >= 288 blocks)
- `true`: Skip all pruning during catchup state

### pruner_blockAssemblyWaitTimeout

**Type**: Duration

**Default**: `10m`

**Environment Variable**: `pruner_blockAssemblyWaitTimeout`

**Description**: Maximum wait for Block Assembly service to be ready before pruning

Sets the maximum time to wait for Block Assembly to be ready before proceeding with pruning operations. Prevents pruning data that could be needed for block construction.

### pruner_connectionPoolWarningThreshold

**Type**: Float64

**Default**: `0.7`

**Environment Variable**: `pruner_connectionPoolWarningThreshold`

**Description**: Connection pool utilization warning threshold (0.0-1.0)

When Aerospike connection pool utilization exceeds this threshold, the pruner auto-reduces chunk group limit to prevent pool exhaustion.

### pruner_block_trigger

**Type**: String

**Default**: `OnBlockPersisted`

**Environment Variable**: `pruner_block_trigger`

**Description**: When to trigger pruning operations

**Values:**

- `OnBlockPersisted` (default): Triggers on BlockPersisted notifications (coordinated with Block Persister)
- `OnBlockMined`: Triggers on Block notifications with mined_set=true

### pruner_force_ignore_block_persister_height

**Type**: Boolean

**Default**: `false`

**Environment Variable**: `pruner_force_ignore_block_persister_height`

**Description**: Force ignore block persister height tracking

When enabled, uses Block notifications with mined_set=true instead of BlockPersisted notifications from Block Persister for determining safe prune height.

### pruner_utxoSetTTL

**Type**: Boolean

**Default**: `false`

**Environment Variable**: `pruner_utxoSetTTL`

**Description**: Use TTL expiration instead of hard delete for UTXO records

When enabled, sets Aerospike record TTL to 1 second instead of hard deleting. This produces optimized tombstones and reduces write amplification.

### pruner_skipBlobDeletion

**Type**: Boolean

**Default**: `false`

**Environment Variable**: `pruner_skipBlobDeletion`

**Description**: Skip blob deletion scheduling

When enabled, skips scheduled deletion of blob store data (transactions and subtrees) based on Delete-At-Height values.

### pruner_blobDeletionSafetyWindow

**Type**: uint32

**Default**: `10`

**Environment Variable**: `pruner_blobDeletionSafetyWindow`

**Description**: Number of blocks to wait after Block Persister height before deleting blobs

Provides a safety margin to ensure persisted blocks are stable before deleting their associated blobs.

### pruner_blobDeletionBatchSize

**Type**: Integer

**Default**: `1000`

**Environment Variable**: `pruner_blobDeletionBatchSize`

**Description**: Maximum number of blob deletions to process per pruning trigger

Limits deletions per cycle to prevent overwhelming the blob store. Remaining deletions are processed in subsequent triggers.

### pruner_blobDeletionMaxRetries

**Type**: Integer

**Default**: `3`

**Environment Variable**: `pruner_blobDeletionMaxRetries`

**Description**: Maximum retry attempts for failed blob deletions

### pruner_skipPreserveParents

**Type**: Boolean

**Default**: `false`

**Environment Variable**: `pruner_skipPreserveParents`

**Description**: Skip Phase 1 - preserve parents of unmined transactions

When enabled, parent transactions will not be protected from deletion even if they have unmined children.

### pruner_skipParentUpdates

**Type**: Boolean

**Default**: `false`

**Environment Variable**: `pruner_skipParentUpdates`

**Description**: Skip parent update operations during pruning

### pruner_skipDeletions

**Type**: Boolean

**Default**: `false`

**Environment Variable**: `pruner_skipDeletions`

**Description**: Skip deletion operations during pruning

## Operation Timeout Settings

### pruner_jobTimeout

**Type**: Duration

**Default**: `10m` (10 minutes)

**Description**: Timeout for overall pruning operation completion

**Format**: Duration string (e.g., `5m`, `30s`, `1h`)

**Example:**

```conf
pruner_jobTimeout = 10m
```

**Behavior:**

- Pruning operation runs synchronously up to this timeout
- On timeout: Operation is logged as timed out but continues in background

**Tuning Guidelines:**

- **Small databases**: `5m` may be sufficient
- **Large databases**: Increase to `15m` or `20m`
- **Slow storage**: Increase timeout accordingly
- **High latency networks**: Add buffer for network delays

**Impact:**

- Too short: Frequent timeouts logged
- Too long: Blocks other operations unnecessarily

**Metrics**: Monitor `pruner_duration_seconds` to determine appropriate timeout

## UTXO Store Settings

These settings control the pruning behavior at the UTXO store level.

### utxostore_unminedTxRetention

**Type**: `uint32` (block height)

**Default**: `globalBlockHeightRetention / 2`

**Typical Value**: ~7200 blocks (≈50 days)

**Description**: Number of blocks to retain unmined transactions before considering them for parent preservation

**Calculation:**

```conf
globalBlockHeightRetention = 14400  # ~100 days
utxostore_unminedTxRetention = 7200  # 50 days
```

**Purpose:**

Unmined transactions older than this are considered "old" and their parent transactions are marked for preservation during Phase 1 pruning.

**Tuning:**

- **Increase**: Retain unmined transactions longer, slower pruning
- **Decrease**: Prune unmined transactions sooner, faster pruning, higher risk

**Warning**: Setting too low may cause valid resubmitted transactions to fail if their parent UTXOs were already pruned.

### utxostore_parentPreservationBlocks

**Type**: `uint32` (block height)

**Default**: `blocksInADayOnAverage * 10`

**Typical Value**: ~14400 blocks (≈100 days, assuming 144 blocks/day)

**Description**: Number of blocks to preserve parent transactions of old unmined transactions

**Calculation:**

```conf
blocksInADayOnAverage = 144  # Typical Bitcoin block time
utxostore_parentPreservationBlocks = 1440  # 10 days
```

**Purpose:**

When Phase 1 preservation runs, parent transactions get their `PreserveUntil` flag set to:

```go
PreserveUntil = currentHeight + parentPreservationBlocks
```

This prevents parent UTXOs from being deleted for the specified number of blocks, ensuring resubmitted transactions can validate.

**Tuning:**

- **Increase**: Preserve parents longer, safer for resubmissions, slower pruning
- **Decrease**: Prune parents sooner, faster pruning, higher risk for resubmissions

**Recommendation**: Keep default value unless specific use case requires change.

### utxostore_prunerMaxConcurrentOperations

**Type**: Integer

**Default**: UTXO store connection pool size

**Description**: Maximum number of concurrent pruning operations for Aerospike

**Applies To**: Aerospike store only (SQL uses fixed worker count)

**Example:**

```conf
utxostore_prunerMaxConcurrentOperations = 8
```

**Impact:**

- **Higher values**: Faster pruning, higher load on database
- **Lower values**: Slower pruning, lower load on database

**Tuning Guidelines:**

- **Small databases**: 4-8 workers sufficient
- **Large databases**: 8-16 workers for faster pruning
- **Limited resources**: Reduce to 2-4 workers
- **High throughput nodes**: Increase to 16-32 workers

**Constraint**: Limited by Aerospike connection pool size. Exceeding pool size causes contention.

**Aerospike Worker Count:**

Default: 4 workers (hardcoded in `/stores/utxo/aerospike/pruner/pruner_service.go`)

To change, modify code:

```go
// In pruner_service.go
workerCount := 4  // Change this value
```

### utxostore_disableDAHCleaner

**Type**: Boolean

**Default**: `false`

**Description**: Disable Delete-At-Height (DAH) pruning (Phase 2)

**Example:**

```conf
utxostore_disableDAHCleaner = true
```

**Impact:**

- `false`: Normal operation, Phase 2 pruning runs
- `true`: Phase 2 disabled, only Phase 1 (parent preservation) runs

**When to Enable:**

- **Testing**: Debugging pruning issues
- **Investigation**: Analyzing DAH pruning behavior
- **Temporary workaround**: If Phase 2 causing issues

**Warning**: Enabling this setting prevents UTXO record deletion, causing database growth. Only for testing/debugging.

## Aerospike-Specific Settings

### pruner_IndexName

**Type**: String

**Default**: `pruner_dah_index`

**Description**: Name of the Aerospike secondary index on `DeleteAtHeight` field

**Example:**

```conf
pruner_IndexName = pruner_dah_index
```

**Purpose:**

DAH pruning (Phase 2) queries Aerospike using this secondary index for efficient record filtering:

```sql
SELECT * FROM utxos WHERE deleteAtHeight <= safeHeight
```

**Index Creation:**

- **Automatic**: Created on service start via index waiter
- **Wait Time**: Service waits for index to be ready before starting pruning

**Manual Index Creation (if needed):**

```bash
asadm -e "asinfo -v 'sindex-create:ns=teranode;set=utxos;indexname=pruner_dah_index;indextype=NUMERIC;binname=deleteAtHeight'"
```

**Index Verification:**

```bash
asadm -e "show indexes"
```

Expected output:

```text
Namespace   Set     Index Name        Bin Name          Type
teranode    utxos   pruner_dah_index  deleteAtHeight    NUMERIC
```

## Chunk Processing Settings

These settings control parallel processing during UTXO pruning operations.

### pruner_utxoChunkSize

**Type**: Integer

**Default**: `1000`

**Description**: Number of records to process in each parallel chunk during pruning

Controls the granularity of parallel processing. Records are accumulated into chunks of this size, then processed in parallel.

**Example:**

```conf
pruner_utxoChunkSize = 1000
```

**Tuning:**

- **Higher values**: Fewer chunks, less parallelism overhead, more memory per chunk
- **Lower values**: More chunks, better parallelism, more overhead

**Recommendation**: Default value works well for most deployments.

### pruner_utxoChunkGroupLimit

**Type**: Integer

**Default**: `10`

**Description**: Maximum number of chunks to process in parallel

Limits concurrent chunk processing to prevent overwhelming Aerospike or exhausting system resources.

**Example:**

```conf
pruner_utxoChunkGroupLimit = 10
```

**Tuning:**

- **Higher values**: More parallelism, faster pruning, higher resource usage
- **Lower values**: Less parallelism, slower pruning, lower resource usage

**Constraint**: Limited by Aerospike connection pool size. Setting too high causes connection contention.

### pruner_utxoProgressLogInterval

**Type**: Duration

**Default**: `30s`

**Description**: Interval for logging progress during long-running pruning operations

When set, the pruner logs periodic progress updates showing records pruned and elapsed time.

**Example:**

```conf
pruner_utxoProgressLogInterval = 30s
```

**Values:**

- `0`: Disable progress logging
- `30s`: Log every 30 seconds (default)
- `1m`: Log every minute

**Use Case**: Monitor progress during large pruning operations or troubleshoot slow pruning.

### pruner_utxoPartitionQueries

**Type**: Integer

**Default**: `0` (auto-detect based on CPU cores)

**Environment Variable**: `pruner_utxoPartitionQueries`

**Description**: Number of parallel Aerospike partition queries for scanning prunable records

Aerospike's keyspace is divided into 4096 partitions. This setting controls how many workers scan partitions in parallel. Each worker processes a range of partitions independently, achieving up to 100x performance improvement over sequential queries.

**Values:**

- `0` (default): Auto-detect based on CPU cores and Aerospike query-threads-limit
- `N > 0`: Fixed number of partition workers (capped at 4096)

**Tuning:**

- **Higher values**: Faster scanning, more Aerospike load and connections
- **Lower values**: Reduced cluster pressure, slower pruning

**Recommendation**: Use default (`0`) for automatic scaling. Set explicitly to match your Aerospike cluster's capacity.

## Defensive Mode Settings

These settings control the defensive pruning mode, which adds safety checks before deleting parent transactions.

### pruner_utxoDefensiveEnabled

**Type**: Boolean

**Default**: `false`

**Description**: Enable defensive child verification before parent deletion

When enabled, the pruner verifies that ALL spending children of a parent transaction are mined and stable (for at least `blockHeightRetention` blocks) before deleting the parent. This prevents orphaning children during chain reorganizations.

**Example:**

```conf
pruner_utxoDefensiveEnabled = true
```

**Impact:**

- `true`: Safer pruning with child verification, slightly slower due to batch verification
- `false`: Faster pruning, relies on retention period alone for safety

**When to Enable:**

- Production environments with high transaction resubmission rates
- Environments experiencing frequent chain reorganizations
- When data integrity is critical

**Trade-off**: Enabling adds Aerospike batch read operations to verify children, which increases pruning time but provides stronger safety guarantees.

### pruner_utxoDefensiveBatchReadSize

**Type**: Integer

**Default**: `10000`

**Description**: Batch size for defensive child verification queries

Controls how many child transactions are verified in a single Aerospike BatchGet call when defensive mode is enabled.

**Example:**

```conf
pruner_utxoDefensiveBatchReadSize = 10000
```

**Tuning:**

- **Higher values**: Fewer Aerospike round trips, more memory per batch
- **Lower values**: More round trips, less memory per batch

**Applies To**: Only used when `pruner_utxoDefensiveEnabled = true`

## Context-Specific Configuration

### Development Context

```conf
[dev]
startPruner = true
pruner_grpcAddress = localhost:8096
pruner_grpcListenAddress = localhost:8096
pruner_jobTimeout = 10m
```

### Docker Context (Single Machine, Multi-Node)

```conf
[docker.m]
startPruner = true
pruner_grpcAddress = pruner:8096
pruner_grpcListenAddress = :8096
pruner_jobTimeout = 10m
```

### Docker Context (Per-Service Containers)

```conf
[docker]
startPruner = true
pruner_grpcAddress = ${clientName}:8096
pruner_grpcListenAddress = :8096
pruner_jobTimeout = 10m
```

### Docker Host Context (Access from Host)

```conf
[docker.host]
startPruner = true
pruner_grpcAddress = localhost:${PORT_PREFIX}8096
pruner_grpcListenAddress = localhost:${PORT_PREFIX}8096
pruner_jobTimeout = 10m

# Disable pruner for specific nodes
startPruner.teranode1.coinbase = false
startPruner.teranode2.coinbase = false
```

### Kubernetes/Operator Context

```conf
[operator]
startPruner = true
pruner_grpcAddress = k8s:///pruner.${clientName}.svc.cluster.local:8096
pruner_grpcListenAddress = :8096
pruner_jobTimeout = 15m  # Increase for distributed environments
```

## Configuration Examples

### Minimal Configuration (Defaults)

```conf
# settings.conf
startPruner = true
```

All other settings use defaults.

### High-Performance Configuration

```conf
# settings.conf
startPruner = true
pruner_jobTimeout = 5m  # Shorter timeout
pruner_utxoChunkSize = 2000  # Larger chunks
pruner_utxoChunkGroupLimit = 20  # More parallel chunks
utxostore_unminedTxRetention = 5000  # Prune sooner
utxostore_parentPreservationBlocks = 10000  # Shorter preservation
```

**Use Case**: High-throughput nodes with fast storage

### Conservative Configuration

```conf
# settings.conf
startPruner = true
pruner_jobTimeout = 20m  # Longer timeout
pruner_utxoChunkGroupLimit = 5  # Fewer parallel chunks
pruner_utxoDefensiveEnabled = true  # Enable safety checks
utxostore_unminedTxRetention = 10000  # Retain longer
utxostore_parentPreservationBlocks = 20000  # Longer preservation
```

**Use Case**: Production nodes prioritizing data safety over performance

### Testing Configuration

```conf
# settings_local.conf
startPruner = false  # Disable pruning for testing
```

**Use Case**: Local testing requiring full UTXO history

### Multi-Node Setup (Some Nodes Pruning)

```conf
# settings.conf
startPruner = true

# Disable for coinbase nodes (they don't need pruning)
startPruner.docker.host.teranode1.coinbase = false
startPruner.docker.host.teranode2.coinbase = false

# Enable for main processing node
startPruner.docker.host.teranode3 = true
```

**Use Case**: Multi-node setup where only certain nodes perform pruning

## Environment Variable Overrides

Settings can be overridden via environment variables using uppercase names with underscores:

```bash
# Override pruner enable/disable
export STARTPRUNER=false

# Override gRPC port
export PRUNER_GRPCPORT=8097

# Override job timeout
export PRUNER_JOBTIMEOUT=15m

# Override UTXO settings
export UTXOSTORE_UNMINEDTXRETENTION=10000
export UTXOSTORE_PARENTPRESERVATIONBLOCKS=20000
```

## Monitoring Settings

While not configuration settings, these Prometheus metrics should be monitored:

- `pruner_duration_seconds`: Adjust `jobTimeout` if consistently near timeout
- `pruner_skipped_total{reason="not_running"}`: Indicates Block Assembly issues
- `pruner_errors_total`: Indicates database or connectivity issues
- `utxo_cleanup_batch_duration_seconds`: Indicates Aerospike performance

## Troubleshooting Configuration Issues

### Pruner Not Starting

**Check:**

1. Verify `startPruner = true`
2. Check port 8096 availability: `lsof -i :8096`
3. Review logs: `grep "\[Pruner\]" teranode.log`

### Port Conflicts

**Solution:**

```conf
PRUNER_GRPC_PORT = 8097
pruner_grpcAddress = localhost:8097
pruner_grpcListenAddress = :8097
```

### Frequent Timeouts

**Symptoms**: `WARN [Pruner] Job timeout reached`

**Solution**: Increase `pruner_jobTimeout`:

```conf
pruner_jobTimeout = 20m  # or higher
```

### Slow Pruning

**Symptoms**: High `pruner_duration_seconds` values

**Solutions:**

1. Increase parallel chunk processing:

    ```conf
    pruner_utxoChunkGroupLimit = 20  # Process more chunks in parallel
    pruner_utxoChunkSize = 2000  # Larger chunks
    ```

2. Verify Aerospike index exists:

    ```bash
    asadm -e "show indexes" | grep pruner_dah_index
    ```

3. Check database performance

4. If defensive mode is enabled and slowing pruning:

    ```conf
    pruner_utxoDefensiveEnabled = false  # Disable if safety checks not needed
    ```

### Database Growth

**Symptoms**: UTXO database growing despite pruning enabled

**Check:**

1. Verify pruner running:

    ```bash
    curl http://localhost:8096/metrics | grep pruner_processed_total
    ```

2. Check `pruner_skipped_total` reasons:

    ```bash
    curl http://localhost:8096/metrics | grep pruner_skipped_total
    ```

3. Verify Block Assembly in RUNNING state

## Related Documentation

- [Pruner Service Topic Documentation](../../../topics/services/pruner.md)
- [Pruner API Reference](../../services/pruner_reference.md)
- [UTXO Store Settings](../stores/utxo_settings.md)
- [Global Settings Reference](../global_settings.md)

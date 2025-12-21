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

## Job Management Settings

### pruner_jobTimeout

**Type**: Duration

**Default**: `10m` (10 minutes)

**Description**: Timeout for waiting for pruning job completion before coordinator moves on

**Format**: Duration string (e.g., `5m`, `30s`, `1h`)

**Example:**

```conf
pruner_jobTimeout = 10m
```

**Behavior:**

- Coordinator waits up to `jobTimeout` for job completion
- On timeout:

    - Coordinator moves on (non-error)
    - Job continues running in background
    - Will be re-queued immediately if needed

**Tuning Guidelines:**

- **Small databases**: `5m` may be sufficient
- **Large databases**: Increase to `15m` or `20m`
- **Slow storage**: Increase timeout accordingly
- **High latency networks**: Add buffer for network delays

**Impact:**

- Too short: Frequent timeouts, coordinator moves on repeatedly
- Too long: Coordinator waits unnecessarily for long-running jobs

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
blocksInADayOnAverage = 1440  # Typical Bitcoin block time
utxostore_parentPreservationBlocks = 14400  # 10 days
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
utxostore_prunerMaxConcurrentOperations = 16  # More workers
utxostore_unminedTxRetention = 5000  # Prune sooner
utxostore_parentPreservationBlocks = 10000  # Shorter preservation
```

**Use Case**: High-throughput nodes with fast storage

### Conservative Configuration

```conf
# settings.conf
startPruner = true
pruner_jobTimeout = 20m  # Longer timeout
utxostore_prunerMaxConcurrentOperations = 4  # Fewer workers
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

1. Increase worker count:

    ```conf
    utxostore_prunerMaxConcurrentOperations = 16
    ```

2. Verify Aerospike index exists:

    ```bash
    asadm -e "show indexes" | grep pruner_dah_index
    ```

3. Check database performance

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

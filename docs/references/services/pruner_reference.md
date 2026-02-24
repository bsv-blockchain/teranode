# Pruner Service API Reference

## Overview

The Pruner service is a standalone microservice responsible for UTXO data pruning operations. This document provides technical API reference for the Pruner service.

**Service Information:**

- **gRPC Port**: 8096 (default)
- **Protocol**: gRPC with Protocol Buffers
- **API Version**: v1
- **Proto File**: `/services/pruner/pruner_api/pruner_api.proto`

## gRPC API

### Health Check API

#### HealthGRPC

Checks the health status of the Pruner service.

**Request:**

```protobuf
message EmptyMessage {}
```

**Response:**

```protobuf
message HealthResponse {
  bool ok = 1;           // true if service is healthy
  string details = 2;    // Additional health information
}
```

**Example:**

```bash
grpcurl -plaintext localhost:8096 pruner.PrunerAPI/HealthGRPC
```

**Response:**

```json
{
  "ok": true,
  "details": ""
}
```

**Health Checks Performed:**

- gRPC server listening
- Block Assembly client health
- Blockchain client health + FSM state
- UTXO store health

**Status Codes:**

- `ok: true` - Service is healthy and ready
- `ok: false` - Service has issues (see `details` field)

## Prometheus Metrics

### Service-Level Metrics

Located in `/services/pruner/metrics.go`:

#### pruner_duration_seconds

**Type**: Histogram

**Description**: Time taken to complete pruning operations

**Labels:**

- `operation`: Operation type
    - `preserve_parents` - Phase 1: Parent preservation
    - `dah_pruner` - Phase 2: DAH pruning

**Example:**

```prometheus
pruner_duration_seconds{operation="preserve_parents"} 1.234
pruner_duration_seconds{operation="dah_pruner"} 5.678
```

#### pruner_skipped_total

**Type**: Counter

**Description**: Count of pruning operations skipped

**Labels:**

- `reason`: Reason for skipping
    - `not_running` - Block Assembly not in RUNNING state
    - `no_new_height` - No new block height to process
    - `already_in_progress` - Pruning already running

**Example:**

```prometheus
pruner_skipped_total{reason="not_running"} 42
pruner_skipped_total{reason="already_in_progress"} 10
pruner_skipped_total{reason="preserve_failed"} 0
```

**Note**: When defensive mode is enabled, skipped records are logged but not tracked as a separate metric label. Monitor logs for "Defensive skip" messages.

#### pruner_updating_parents_total

**Type**: Counter

**Description**: Total number of parent-updating operations completed during pruning.

**Example:**

```prometheus
pruner_updating_parents_total 1000
```

#### pruner_deleting_children_total

**Type**: Counter

**Description**: Total number of child-deletion operations completed during pruning.

**Example:**

```prometheus
pruner_deleting_children_total 500
```

#### pruner_current_height

**Type**: Gauge

**Description**: The current block height being pruned.

**Example:**

```prometheus
pruner_current_height 850000
```

#### pruner_active

**Type**: Gauge

**Description**: Whether the pruner is currently active (1) or idle (0).

**Example:**

```prometheus
pruner_active 1
```

#### pruner_errors_total

**Type**: Counter

**Description**: Total number of pruning errors encountered

**Labels:**

- `operation`: Operation where error occurred
    - `preserve_parents` - Error during parent preservation
    - `dah_pruner` - Error during DAH pruning
    - `state_check` - Error checking Block Assembly state

**Example:**

```prometheus
pruner_errors_total{operation="preserve_parents"} 0
pruner_errors_total{operation="dah_pruner"} 2
pruner_errors_total{operation="state_check"} 0
```

### Store-Level Metrics

Located in `/stores/utxo/aerospike/pruner/`:

#### utxo_cleanup_batch_duration_seconds

**Type**: Histogram

**Description**: Time taken to process pruning batches in Aerospike

**Note**: Aerospike-specific metric

**Example:**

```prometheus
utxo_cleanup_batch_duration_seconds 0.123
```

## Internal Interfaces

### Service Interface

Located in `/stores/utxo/pruner/interfaces.go`:

```go
type Service interface {
    // Start starts the pruner service.
    // This should not block.
    // The service should stop when the context is cancelled.
    Start(ctx context.Context)

    // Prune removes transactions marked for deletion at or before the specified height.
    // Returns the number of records processed and any error encountered.
    // This method is synchronous and blocks until pruning completes or context is cancelled.
    Prune(ctx context.Context, height uint32) (recordsProcessed int64, err error)

    // SetPersistedHeightGetter sets the function used to get block persister progress.
    // This allows pruner to coordinate with block persister to avoid premature deletion.
    SetPersistedHeightGetter(getter func() uint32)
}
```

### Provider Interface

```go
type PrunerServiceProvider interface {
    // GetPrunerService returns a pruner service for the store.
    // Returns nil if the store doesn't support pruner functionality.
    GetPrunerService() (Service, error)
}
```

## Event Subscriptions

The Pruner service subscribes to the following blockchain events:

### BlockPersisted Notification

**Source**: Block Persister service via Blockchain service

**Trigger**: After Block Persister completes persisting a block

**Payload**:

```go
type BlockNotification struct {
    BlockHash   *chainhash.Hash
    BlockHeight uint32
    // ... other fields
}
```

**Handler**: Updates `lastPersistedHeight` and triggers pruning

### Block Notification (Fallback)

**Source**: Blockchain service

**Trigger**: When block is set to mined

**Condition**: Only processed if `lastPersistedHeight == 0` (Block Persister not running)

**Payload**:

```go
type BlockNotification struct {
    BlockHash   *chainhash.Hash
    Block       *wire.Block
    MinedSet    bool  // Must be true
    // ... other fields
}
```

**Handler**: Triggers pruning if `mined_set == true`

## Error Codes

### Phase 1 Errors (Critical)

| Error | Description | Action |
|-------|-------------|--------|
| `PreserveParentsFailed` | Failed to update parent PreserveUntil | Abort entire pruning |
| `QueryUnminedFailed` | Failed to query old unmined transactions | Abort entire pruning |

### Phase 2 Errors (Non-Critical)

| Error | Description | Action |
|-------|-------------|--------|
| `DAHQueryFailed` | Failed to query records for deletion | Retry job |
| `DeleteRecordFailed` | Failed to delete UTXO record | Log error, continue |
| `ExternalStorageDeleteFailed` | Failed to delete .tx file | Log error, continue |

### State Check Errors

| Error | Description | Action |
|-------|-------------|--------|
| `BlockAssemblyNotRunning` | Block Assembly in non-RUNNING state | Skip pruning, retry later |
| `ClientHealthCheckFailed` | Service client unhealthy | Skip pruning, retry later |

## Configuration via Environment

While settings are primarily in `settings.conf`, environment variables can override:

```bash
# Override gRPC port
export PRUNER_GRPCPORT=8097

# Disable pruner
export STARTPRUNER=false

# Set job timeout
export PRUNER_JOBTIMEOUT=15m
```

## Logging

### Log Prefixes

- `[Pruner]` - Service-level logs
- `[PreserveParents]` - Phase 1 logs
- `[AerospikeCleanupService]` - Aerospike store logs
- `[SQLCleanupService]` - SQL store logs

### Log Levels

- **DEBUG**: Queue operations, deduplication, job lifecycle
- **INFO**: Phase start/completion, height updates, successful operations
- **WARN**: Timeouts (non-errors), skipped operations
- **ERROR**: Critical failures, Phase 1 errors (abort pruning)

### Example Logs

```text
INFO  [Pruner] Service initialized successfully
INFO  [Pruner] Subscribed to BlockPersisted notifications
INFO  [Pruner] Subscribed to Block notifications (fallback)
DEBUG [Pruner] Received BlockPersisted notification for height 12345
INFO  [PreserveParents] Starting parent preservation for height 12345
INFO  [PreserveParents] Preserved 42 parent transactions
INFO  [AerospikeCleanupService] Starting DAH pruning for height 12345
INFO  [AerospikeCleanupService] Deleted 1000 UTXO records
INFO  [Pruner] Pruning completed successfully for height 12345
WARN  [Pruner] Pruning skipped: Block Assembly not running
ERROR [PreserveParents] Failed to preserve parent transaction: CRITICAL - aborting pruning
```

## Performance Considerations

### Chunk Processing Configuration

**Chunk Size (`pruner_utxoChunkSize`):**

- Default: 1000 records per chunk
- Increase for faster pruning with more memory usage
- Decrease for lower memory footprint

**Parallel Chunks (`pruner_utxoChunkGroupLimit`):**

- Default: 10 parallel chunks
- Increase for high-throughput nodes with fast storage
- Limited by Aerospike connection pool size

### Operation Timeout

- Default: 10 minutes (`pruner_jobTimeout`)
- On timeout: Operation continues in background
- Adjust based on:

    - Database size
    - Network latency
    - Chunk configuration

### Channel Buffering

- Buffer size: 1 (non-blocking)
- Prevents queue buildup during catchup
- Deduplication ensures only latest height processed

### Defensive Mode

- Default: Disabled (`pruner_utxoDefensiveEnabled = false`)
- When enabled: Adds batch verification of spending children
- Performance impact: Additional Aerospike BatchGet operations
- Trade-off: Safer pruning vs. slower performance

### Secondary Index (Aerospike)

- **Required**: Index on `DeleteAtHeight` field
- **Index Name**: `pruner_dah_index` (default)
- **Impact**: Query performance critical for DAH pruning
- **Creation**: Automatic on service start via index waiter

## Troubleshooting

### Service Not Starting

1. Check port availability:

    ```bash
    lsof -i :8096
    ```

2. Verify UTXO store connection:

    ```bash
    # Check UTXO store health
    grpcurl -plaintext localhost:8090 asset.AssetAPI/HealthGRPC
    ```

3. Check logs for initialization errors:

    ```bash
    grep "\[Pruner\].*error" teranode.log
    ```

### No Pruning Activity

1. Verify service enabled:

    ```conf
    startPruner = true
    ```

2. Check Block Assembly state:

    ```bash
    # Should be in RUNNING state
    curl http://localhost:8087/api/blockchain/status
    ```

3. Verify event subscriptions:

    ```bash
    grep "Subscribed to.*notifications" teranode.log
    ```

4. Check metrics:

    ```bash
    curl http://localhost:8096/metrics | grep pruner_skipped_total
    ```

### High Error Rate

1. Check `pruner_errors_total` by operation:

    ```prometheus
    pruner_errors_total{operation="preserve_parents"}
    pruner_errors_total{operation="dah_pruner"}
    ```

2. Review error logs:

    ```bash
    grep "\[Pruner\].*ERROR" teranode.log
    ```

3. Verify database connectivity:

    - Aerospike: Check cluster health
    - SQL: Check connection pool

### Slow Pruning

1. Check `pruner_duration_seconds` histogram:

    ```bash
    curl http://localhost:8096/metrics | grep pruner_duration_seconds
    ```

2. Increase parallel chunk processing:

    ```conf
    pruner_utxoChunkGroupLimit = 20  # More parallel chunks
    pruner_utxoChunkSize = 2000  # Larger chunks
    ```

3. If defensive mode is enabled, consider disabling:

    ```conf
    pruner_utxoDefensiveEnabled = false
    ```

4. Verify secondary index exists (Aerospike):

    ```bash
    asadm -e "show indexes"
    # Should show: pruner_dah_index
    ```

## Related Documentation

- [Pruner Service Topic Documentation](../../topics/services/pruner.md)
- [Pruner Settings Reference](../settings/services/pruner_settings.md)
- [UTXO Store Reference](../stores/utxo_reference.md)
- [Prometheus Metrics Reference](../prometheusMetrics.md)

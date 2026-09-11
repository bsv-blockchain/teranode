# Pruner Service Settings

**Related Topic**: [Pruner Service](../../../topics/services/pruner.md)

## Configuration Settings

Settings are organized under the `Pruner` struct in `settings.Settings`.

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| GRPCListenAddress | string | ":8096" | pruner_grpcListenAddress | **CRITICAL** - gRPC server bind address |
| GRPCAddress | string | "localhost:8096" | pruner_grpcAddress | gRPC client address other services dial |
| SkipDuringCatchup | bool | false | pruner_skipDuringCatchup | Skip all pruning while the FSM is catching up (see "Settings not wired to configuration" below) |
| BlockAssemblyWaitTimeout | time.Duration | 10m | pruner_blockAssemblyWaitTimeout | Maximum wait for Block Assembly to be ready before pruning |
| ConnectionPoolWarningThreshold | float64 | 0.7 | pruner_connectionPoolWarningThreshold | Aerospike connection pool utilization (0.0-1.0) above which the chunk group limit is auto-reduced |
| BlockTrigger | string | "OnBlockPersisted" | pruner_block_trigger | What triggers a pruning cycle ("OnBlockPersisted" or "OnBlockMined") |
| ForceIgnoreBlockPersisterHeight | bool | false | pruner_force_ignore_block_persister_height | Use Block notifications with mined_set=true instead of the Block Persister height (see "Settings not wired to configuration" below) |
| UTXODefensiveEnabled | bool | false | pruner_utxoDefensiveEnabled | Verify every spending child is mined and stable before deleting a parent |
| UTXODefensiveBatchReadSize | int | 10000 (Go default; settings.conf ships 1024) | pruner_utxoDefensiveBatchReadSize | Children verified per Aerospike BatchGet in defensive mode |
| UTXOChunkSize | int | 1000 (Go default; settings.conf ships 1024) | pruner_utxoChunkSize | Records accumulated per parallel chunk |
| UTXOChunkGroupLimit | int | 10 (Go default; settings.conf ships 1) | pruner_utxoChunkGroupLimit | Maximum chunks processed in parallel per worker |
| UTXOProgressLogInterval | time.Duration | 30s | pruner_utxoProgressLogInterval | Progress-log interval during long pruning runs (0 disables) |
| UTXOPartitionQueries | int | 0 (auto-detect from CPU cores) | pruner_utxoPartitionQueries | Parallel Aerospike partition-scan workers, capped at 4096 |
| UTXOSetTTL | bool | false | pruner_utxoSetTTL | Set a 1-second record TTL instead of hard deleting |
| RelaxRemovalCommitLevel | bool | true | pruner_relaxRemovalCommitLevel | COMMIT_MASTER for the pruner's own removals and TTL touches |
| SkipBlobDeletion | bool | false | pruner_skipBlobDeletion | Skip scheduled blob-store deletions |
| BlobDeletionSafetyWindow | uint32 | 10 | pruner_blobDeletionSafetyWindow | Blocks behind the triggering height before a blob may be deleted |
| BlobDeletionBatchSize | int | 1000 | pruner_blobDeletionBatchSize | Maximum blob deletions per pruning trigger |
| BlobDeletionMaxRetries | int | 3 | pruner_blobDeletionMaxRetries | Retry attempts for a failed blob deletion |
| SkipPreserveParents | bool | false | pruner_skipPreserveParents | Skip Phase 1 - parent preservation for unmined transactions |
| SkipProcessExpiredPreservations | bool | false | pruner_skipProcessExpiredPreservations | Skip Phase 1b - expiry of old parent preservations (see "Settings not wired to configuration" below) |
| MinBlockHeight | uint32 | 0 | pruner_min_block_height | Skip all pruning until block height exceeds this value |
| SkipDeletions | bool | false | pruner_skipDeletions | Skip deletion operations during pruning |
| UTXOPrunedSetMaxEntries | int | 10000000 | pruner_utxoPrunedSetMaxEntries | Soft cap on the in-memory pruned-TX set (0 = built-in 2B default, not unlimited) |

### Service Control

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| startPruner | bool | false (no code default - `gocore.Config().GetBool` with no fallback; settings.conf ships `true`, and `false` for the `docker.m` and `operator` contexts) | startPruner | **CRITICAL** - Enable or disable the Pruner service |

`startPruner` is read directly by `daemon.shouldStart()`, one per service, and is not part of the
settings package - it has no struct tag and does not appear in `ExportMetadata()`.

### Aerospike Index

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| IndexName | string | "pruner_dah_index" | pruner_IndexName | Aerospike secondary index on `DeleteAtHeight` used by Phase 2 |

`IndexName` is a package-level `var` in `stores/utxo/aerospike/pruner/pruner_service.go` read via
`gocore.Config().Get`, not a settings-package key.

### Related Settings (from UTXOStore struct)

These control pruning behaviour at the UTXO store level. Full reference:
[UTXO Store Settings](../stores/utxo_settings.md).

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| UnminedTxRetention | uint32 | 144 (globalBlockHeightRetention/2, with the default global retention of 288) | utxostore_unminedTxRetention | Blocks an unmined transaction is retained before its parents are considered for preservation |
| ParentPreservationBlocks | uint32 | 1440 (blocksInADayOnAverage*10) | utxostore_parentPreservationBlocks | Blocks a preserved parent of an old unmined transaction is kept |
| DisableDAHCleaner | bool | false | utxostore_disableDAHCleaner | **CRITICAL** - Disable Delete-At-Height pruning (Phase 2) |

## Settings not wired to configuration

`SkipDuringCatchup`, `ForceIgnoreBlockPersisterHeight` and `SkipProcessExpiredPreservations` carry
`key` struct tags and are read by `services/pruner`, but `settings.NewSettings()` never populates
them - they always hold the Go zero value `false`. Setting `pruner_skipDuringCatchup`,
`pruner_force_ignore_block_persister_height` or `pruner_skipProcessExpiredPreservations` in
settings.conf or the environment therefore has no effect today. The documented defaults above match
the effective behaviour; the keys are documented because the fields exist and are consumed.

## Configuration Dependencies

### Network Addressing

`settings.conf` builds every pruner address from the `${PRUNER_GRPC_PORT}` template variable, so a
port change only needs to happen in one place:

```conf
PRUNER_GRPC_PORT = 8096

pruner_grpcAddress              = localhost:${PRUNER_GRPC_PORT}
pruner_grpcAddress.docker.m     = pruner:${PRUNER_GRPC_PORT}
pruner_grpcAddress.docker       = ${clientName}:${PRUNER_GRPC_PORT}
pruner_grpcAddress.operator     = k8s:///pruner.${clientName}.svc.cluster.local:${PRUNER_GRPC_PORT}

pruner_grpcListenAddress        = :${PRUNER_GRPC_PORT}
pruner_grpcListenAddress.dev    = localhost:${PRUNER_GRPC_PORT}
```

That is the complete set of pruner address overrides in `settings.conf`; every other context falls
back to the base values.

`PRUNER_GRPC_PORT` is not a settings-package key - it is an operator-defined gocore config value
resolved by generic `${VAR}` substitution, so it can be named anything as long as both address
settings reference the same variable.

Bind address forms for `pruner_grpcListenAddress`:

- `:8096` - all interfaces
- `localhost:8096` - localhost only (more secure)
- `0.0.0.0:8096` - explicitly all IPv4 interfaces

### Service Control

- `true`: Pruner service starts and performs UTXO pruning
- `false`: Pruner service disabled, UTXO database will grow unbounded

Disable only temporarily - for testing scenarios requiring full UTXO history, debugging transaction
issues, or as a workaround for pruning errors.

**Warning**: Disabling pruning will cause the UTXO database to grow continuously.

### Pruning Triggers

- `OnBlockPersisted` (default): triggers on BlockPersisted notifications, coordinated with the Block
  Persister
- `OnBlockMined`: triggers on Block notifications with mined_set=true

`ForceIgnoreBlockPersisterHeight` selects the same Block-notification source for the safe prune
height instead of the Block Persister's height tracking - useful when the Block Persister is not
deployed or its height tracking is unreliable.

`MinBlockHeight` gates everything: while the chain height is at or below it, all operations (parent
preservation, DAH deletion, blob deletion) are skipped. Useful when bootstrapping a fresh
environment where the initial blocks must stay available for cross-node validation.

### Catchup Safety

When `SkipDuringCatchup` is enabled the pruner checks FSM state and skips all deletion operations
during catchup. That prevents the race where block validation marks transactions as mined faster
than the pruner can preserve their parents. Leaving it off is only safe with a retention of at
least 288 blocks.

### Block Assembly Coordination

`BlockAssemblyWaitTimeout` caps how long the pruner waits for Block Assembly to reach the running
state before proceeding. If Block Assembly is temporarily reorging or resetting, the pruner retries
the state check until the timeout, which prevents pruning data that could still be needed for block
construction.

### Removal Commit Level

`RelaxRemovalCommitLevel` controls the Aerospike commit level for the pruner's record removals -
hard deletes, and the 1-second TTL touches used when `UTXOSetTTL` is enabled.

| Value | Behaviour |
|-------|-----------|
| `true` (default) | `COMMIT_MASTER` - the call returns once the master replica has the removal; replicas catch up asynchronously. |
| `false` | `COMMIT_ALL` - wait for every replica, the same as every other Teranode write. |

Relaxing is safe here because pruning is idempotent and self-healing: the pruner only removes
records that are already provably safe to drop, and a replica that misses a removal is re-found by
the `delete_at_height` partition scan on the next session and re-pruned. Waiting for a
full-replication ACK buys no correctness, only latency in the per-block removal burst.

**Scope** - record removal only:

- Pruner **parent updates** (the `deletedChildren` map writes and the `addDeletedChildren` UDF)
  mutate records that survive the prune and always use `COMMIT_ALL`.
- Every write **outside** the pruner always uses `COMMIT_ALL`. There is no cluster-wide commit-level
  setting: UTXO record creation, the transaction-creation lock, the conflict WAL, setMined, unspend
  and the preserve / delete-at-height writes are none of them self-healing, so relaxing them would
  trade a resync for throughput.
- Only has an effect on namespaces with `replication-factor` > 1. On a single-copy namespace both
  values behave identically.

**When to set `false`**: only to rule the relaxation out while diagnosing missing-record or
replication issues.

### Blob Deletion

`BlobDeletionSafetyWindow` provides a safety margin by only deleting blobs whose delete-at-height is
at least that many blocks behind the triggering block height (mined or persisted, depending on
`BlockTrigger`). While the triggering height has not yet exceeded the safety window, all blob
deletions are skipped. This prevents deletion of data that might be needed during a reorg.

`BlobDeletionBatchSize` limits deletions per cycle so the blob store is not overwhelmed; the
remainder is processed on subsequent triggers.

### Parent Preservation

When Phase 1 preservation runs, parent transactions get their `PreserveUntil` flag set to:

```go
PreserveUntil = currentHeight + parentPreservationBlocks
```

This prevents parent UTXOs from being deleted for that many blocks, so resubmitted transactions can
still validate. `blocksInADayOnAverage` (144, ~1 day at a 10-minute block target) is a fixed
constant in settings.go, not a configurable key - the `ParentPreservationBlocks` default is derived
from it, not read from settings.conf. Likewise `UnminedTxRetention` defaults to half of
`global_blockHeightRetention`, so raising the global retention raises it too:

```conf
global_blockHeightRetention = 14400
utxostore_unminedTxRetention = 7200
```

Tuning both: increasing retains transactions and parents longer, which is safer for resubmissions
but prunes more slowly; decreasing prunes sooner at higher risk. Setting `UnminedTxRetention` too
low may cause valid resubmitted transactions to fail because their parent UTXOs were already pruned.

When `SkipProcessExpiredPreservations` is off (the default), each pruner cycle clears expired
`PreserveUntil` markers and re-stamps `DeleteAtHeight` on preserved parents that have become safe to
prune (mined, on the longest chain, and fully spent, or conflicting). When on, preserved parents
keep `PreserveUntil` set and `DeleteAtHeight` cleared indefinitely and are never pruned - an
emergency kill-switch only.

### DAH Cleaner (Phase 2)

- `DisableDAHCleaner` false: normal operation, Phase 2 pruning runs
- `DisableDAHCleaner` true: Phase 2 disabled, only Phase 1 (parent preservation) runs

**Warning**: enabling it prevents UTXO record deletion and causes database growth. Testing and
debugging only.

Phase 2 queries Aerospike through the `IndexName` secondary index for efficient record filtering:

```sql
SELECT * FROM utxos WHERE deleteAtHeight <= safeHeight
```

The index is created automatically on service start via the index waiter, and the service waits for
it to be ready before pruning. Manual creation, if needed:

```bash
asadm -e "asinfo -v 'sindex-create:ns=teranode;set=utxos;indexname=pruner_dah_index;indextype=NUMERIC;binname=deleteAtHeight'"
```

Verification:

```bash
asadm -e "show indexes"
```

Expected output:

```text
Namespace   Set     Index Name        Bin Name          Type
teranode    utxos   pruner_dah_index  deleteAtHeight    NUMERIC
```

### Chunk Processing

Aerospike's keyspace is divided into 4096 partitions. `UTXOPartitionQueries` controls how many
workers scan partitions in parallel; each processes a range independently, achieving up to 100x
improvement over sequential queries. `0` auto-detects from CPU cores and Aerospike's
query-threads-limit; a value above 0 fixes the worker count (capped at 4096).

Records scanned for deletion are accumulated into chunks of `UTXOChunkSize` and processed in
parallel, at most `UTXOChunkGroupLimit` chunks at a time. Higher chunk sizes mean fewer chunks, less
parallelism overhead and more memory per chunk; higher group limits mean more parallelism, faster
pruning and higher resource usage.

`UTXOChunkGroupLimit` is bounded by the Aerospike connection pool. On startup the pruner validates
`(workers × chunkGroupLimit) + workers` against `ConnectionQueueSize × ConnectionPoolWarningThreshold`
and reduces the group limit if it would exceed the threshold. `settings.conf` deliberately ships a
group limit of 1: at scale, parallel chunk processing can starve transaction throughput.

### Pruned-TX Set

`UTXOPrunedSetMaxEntries` caps the in-memory `PrunedTxSet`, which tracks TXIDs pruned across
sessions so wasteful Aerospike parent updates can be skipped pre-flight. It is held on the service
struct and reused across prune sessions within a process, never persisted across restarts. The set
is a sharded two-generation cuckoo filter: when the current generation fills it rotates into the
previous slot and a fresh one is allocated, so `Add` keeps succeeding and old entries age out. A
high `utxo_pruner_pruned_set_rotations` rate is the signal that the cap is too small.

The cap is the total entry budget across both generations of all shards, so memory is roughly one
byte per entry: 10M ≈ 10 MiB, 100M ≈ 100 MiB, and `0` selects the built-in 2B default ≈ 2 GiB. The
optimisation is disabled automatically when `UTXODefensiveEnabled` is true.

### Defensive Mode

When `UTXODefensiveEnabled` is true the pruner verifies that ALL spending children of a parent are
mined and stable (for at least `blockHeightRetention` blocks) before deleting the parent, which
prevents orphaning children during chain reorganizations. It adds Aerospike batch reads, so pruning
takes longer in exchange for a stronger safety guarantee. `UTXODefensiveBatchReadSize` controls how
many children are verified per BatchGet, and only applies while defensive mode is on.

Enable it for production environments with high transaction resubmission rates, environments with
frequent reorgs, or wherever data integrity outweighs pruning speed.

## Context-Specific Configuration

The values below are what `settings.conf` ships. Note that `startPruner` is off in `docker.m` and
`operator`.

### Development Context

```conf
[dev]
startPruner = true
pruner_grpcAddress = localhost:8096
pruner_grpcListenAddress = localhost:8096
```

### Docker Context (Single Machine, Multi-Node)

```conf
[docker.m]
startPruner = false
pruner_grpcAddress = pruner:8096
pruner_grpcListenAddress = :8096
pruner_utxoPartitionQueries = 8
```

### Docker Context (Per-Service Containers)

```conf
[docker]
startPruner = true
pruner_grpcAddress = ${clientName}:8096
pruner_grpcListenAddress = :8096
```

### Docker Host Context (Access from Host)

`settings.conf` ships no pruner overrides for `docker.host`, so the base values apply. To reach a
port-prefixed container from the host, and to keep the pruner off specific nodes:

```conf
[docker.host]
pruner_grpcAddress = localhost:${PORT_PREFIX}8096

# Disable pruner for specific nodes
startPruner.teranode1.coinbase = false
startPruner.teranode2.coinbase = false
```

### Kubernetes/Operator Context

```conf
[operator]
startPruner = false
pruner_grpcAddress = k8s:///pruner.${clientName}.svc.cluster.local:8096
pruner_grpcListenAddress = :8096
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
pruner_utxoChunkSize = 2000
pruner_utxoChunkGroupLimit = 20
utxostore_unminedTxRetention = 5000
utxostore_parentPreservationBlocks = 10000
```

**Use Case**: High-throughput nodes with fast storage. Raising the chunk group limit trades
transaction throughput for pruning speed - watch tx ingest when you do.

### Conservative Configuration

```conf
# settings.conf
startPruner = true
pruner_utxoChunkGroupLimit = 5
pruner_utxoDefensiveEnabled = true
utxostore_unminedTxRetention = 10000
utxostore_parentPreservationBlocks = 20000
```

**Use Case**: Production nodes prioritizing data safety over performance

### Testing Configuration

```conf
# settings_local.conf
startPruner = false
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

### Aerospike Index Name Override

```conf
pruner_IndexName = pruner_dah_index
```

## Environment Variable Overrides

Settings can be overridden via environment variables using the exact key name (case-sensitive, no
automatic uppercasing):

```bash
# Override pruner enable/disable
export startPruner=false

# Override gRPC port (via the settings.conf ${PRUNER_GRPC_PORT} template variable used above)
export PRUNER_GRPC_PORT=8097

# Override UTXO settings
export utxostore_unminedTxRetention=10000
export utxostore_parentPreservationBlocks=20000
```

## Monitoring

While not configuration settings, these Prometheus metrics should be monitored:

- `pruner_duration_seconds`: watch for a rising trend, which may indicate the chunk/concurrency
  settings need tuning
- `pruner_skipped_total{reason="not_running"}`: indicates Block Assembly issues
- `pruner_errors_total`: indicates database or connectivity issues
- `utxo_cleanup_batch_duration_seconds`: indicates Aerospike performance
- `utxo_pruner_pruned_set_rotations`: high rate means `pruner_utxoPrunedSetMaxEntries` is too small

## Troubleshooting Configuration Issues

### Pruner Not Starting

**Check:**

1. Verify `startPruner = true` for the active settings context
2. Check port 8096 availability: `lsof -i :8096`
3. Review logs: `grep "\[Pruner\]" teranode.log`

### Port Conflicts

**Solution:**

```conf
PRUNER_GRPC_PORT = 8097
pruner_grpcAddress = localhost:8097
pruner_grpcListenAddress = :8097
```

### Slow Pruning

**Symptoms**: High `pruner_duration_seconds` values

**Solutions:**

1. Increase parallel chunk processing:

    ```conf
    pruner_utxoChunkGroupLimit = 20
    pruner_utxoChunkSize = 2000
    ```

2. Verify Aerospike index exists:

    ```bash
    asadm -e "show indexes" | grep pruner_dah_index
    ```

3. Check database performance

4. If defensive mode is enabled and slowing pruning:

    ```conf
    pruner_utxoDefensiveEnabled = false
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

4. Verify `pruner_min_block_height` is not still gating the chain height

## Related Documentation

- [Pruner Service Topic Documentation](../../../topics/services/pruner.md)
- [Pruner API Reference](../../services/pruner_reference.md)
- [UTXO Store Settings](../stores/utxo_settings.md)
- [Global Settings Reference](../global_settings.md)

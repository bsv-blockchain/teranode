# UTXO Store Settings

**Related Topic**: [UTXO Store](../../../topics/stores/utxo.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| UtxoStore | *url.URL | "" | utxostore | **CRITICAL** - UTXO store backend URL |
| BlockHeightRetention | uint32 | globalBlockHeightRetention | utxostore_blockHeightRetention | Block height retention period |
| UnminedTxRetention | uint32 | globalBlockHeightRetention/2 | utxostore_unminedTxRetention | Unmined transaction retention |
| ParentPreservationBlocks | uint32 | blocksInADayOnAverage*10 | utxostore_parentPreservationBlocks | Parent preservation period |
| OutpointBatcherSize | int | 100 | utxostore_outpointBatcherSize | Outpoint operation batch size |
| OutpointBatcherDurationMillis | int | 10 | utxostore_outpointBatcherDurationMillis | Outpoint batch duration |
| SpendBatcherDurationMillis | int | 100 | utxostore_spendBatcherDurationMillis | Spend batch duration |
| SpendBatcherSize | int | 100 | utxostore_spendBatcherSize | Spend operation batch size |
| SpendBatcherConcurrency | int | 32 | utxostore_spendBatcherConcurrency | Spend batch concurrency |
| SpendWaitTimeout | time.Duration | 30s | utxostore_spendWaitTimeout | Spend operation wait timeout |
| SpendCircuitBreakerFailureCount | int | 10 | utxostore_spendCircuitBreakerFailureCount | Circuit breaker failure threshold |
| SpendCircuitBreakerCooldown | time.Duration | 30s | utxostore_spendCircuitBreakerCooldown | Circuit breaker cooldown period |
| SpendCircuitBreakerHalfOpenMax | int | 4 | utxostore_spendCircuitBreakerHalfOpenMax | Circuit breaker half-open max |
| StoreBatcherDurationMillis | int | 100 | utxostore_storeBatcherDurationMillis | Store batch duration |
| StoreBatcherSize | int | 100 | utxostore_storeBatcherSize | Store operation batch size |
| UtxoBatchSize | int | 128 | utxostore_utxoBatchSize | UTXO operation batch size |
| IncrementBatcherSize | int | 256 | utxostore_incrementBatcherSize | Increment operation batch size |
| IncrementBatcherDurationMillis | int | 10 | utxostore_incrementBatcherDurationMillis | Increment batch duration |
| SetDAHBatcherSize | int | 256 | utxostore_setDAHBatcherSize | DAH operation batch size |
| SetDAHBatcherDurationMillis | int | 10 | utxostore_setDAHBatcherDurationMillis | DAH batch duration |
| LockedBatcherSize | int | 1024 | utxostore_lockedBatcherSize | Locked operation batch size |
| LockedBatcherDurationMillis | int | 5 | utxostore_lockedBatcherDurationMillis | Locked batch duration |
| LongestChainBatcherSize | int | 1024 | utxostore_longestChainBatcherSize | Longest chain batch size |
| LongestChainBatcherDurationMillis | int | 5 | utxostore_longestChainBatcherDurationMillis | Longest chain batch duration |
| GetBatcherSize | int | 1 | utxostore_getBatcherSize | Get operation batch size |
| GetBatcherDurationMillis | int | 10 | utxostore_getBatcherDurationMillis | Get batch duration |
| DBTimeout | time.Duration | 5s | utxostore_dbTimeoutDuration | **CRITICAL** - Database operation timeout |
| UseExternalTxCache | bool | true | utxostore_useExternalTxCache | External transaction cache usage |
| ExternalizeAllTransactions | bool | false | utxostore_externalizeAllTransactions | Transaction externalization control |
| ExternalStoreConcurrency | int | 16 | utxostore_externalStoreConcurrency | Maximum concurrent external storage operations |
| VerboseDebug | bool | false | utxostore_verbose_debug | Verbose debug logging |
| UpdateTxMinedStatus | bool | true | utxostore_updateTxMinedStatus | Transaction mined status updates |
| MaxMinedRoutines | int | 128 | utxostore_maxMinedRoutines | Max mined transaction routines |
| MaxMinedBatchSize | int | 1024 | utxostore_maxMinedBatchSize | Max mined transaction batch size |
| BlockHeightRetentionAdjustment | int32 | 0 | utxostore_blockHeightRetentionAdjustment | **CRITICAL** - Retention adjustment |
| DisableDAHCleaner | bool | false | utxostore_disableDAHCleaner | **CRITICAL** - DAH cleaner process control |
| PrunerParentUpdateBatcherSize | int | 2000 | utxostore_prunerParentUpdateBatcherSize | Pruner parent update batch size |
| PrunerParentUpdateBatcherDurationMillis | int | 100 | utxostore_prunerParentUpdateBatcherDurationMillis | Pruner parent update batch duration |
| PrunerDeleteBatcherSize | int | 5000 | utxostore_prunerDeleteBatcherSize | Pruner delete batch size |
**Note**: PostgreSQL connection pool settings (MaxOpenConns, MaxIdleConns, ConnMaxLifetime, ConnMaxIdleTime) are now configured globally via `PostgresSettings`. See [Global Settings](../global_settings.md) for details.
| PrunerDeleteBatcherDurationMillis | int | 100 | utxostore_prunerDeleteBatcherDurationMillis | Pruner delete batch duration |
| PrunerMaxConcurrentOperations | int | 0 | utxostore_prunerMaxConcurrentOperations | Pruner max concurrent operations (0=use queue size) |

## URL Query Parameters

| Parameter | Type | Default | Usage | Impact |
|-----------|------|---------|-------|--------|
| logging | bool | false | `storeURL.Query().Get("logging") == "true"` | **CRITICAL** - Enables operation logging wrapper |

## Configuration Dependencies

### Block Height Retention

- Effective retention = `GlobalBlockHeightRetention + BlockHeightRetentionAdjustment`
- Used in `sql/sql.go` for DAH calculations and cleanup operations
- Bounds checking prevents negative results

### Database Operations

- `DBTimeout` controls all SQL operation timeouts in `sql/sql.go`
- PostgreSQL connection settings control connection pooling
- Used across Create, Get, Spend, Delete, and batch operations

### Batch Processing

- Size and duration settings work together for different operation types
- Controls memory usage and performance for bulk operations
- Separate batchers for outpoint, spend, store, increment, DAH, and locked operations

### DAH Functionality

- When `DisableDAHCleaner = false`, uses retention settings for cleanup
- DAH calculations use block height retention values
- Pruning operations respect retention adjustment settings

### Debug Logging

- URL `logging` parameter enables operation wrapper in `factory/utxo.go`
- `VerboseDebug` controls detailed logging output
- Logs all store operations with parameters and duration

## Backend Support

| Backend | Scheme | Parameters Supported |
|---------|--------|---------------------|
| aerospike | aerospike:// | All settings, logging parameter |
| postgres | postgres:// | All settings, logging parameter |
| sqlite | sqlite:// | All settings, logging parameter |
| sqlitememory | sqlitememory:// | All settings, logging parameter |
| memory | memory:// | All settings, logging parameter |
| null | null:// | All settings, logging parameter |

## Validation Rules

| Setting | Validation | Impact | When Checked |
|---------|------------|--------|-------------|
| UtxoStore | Must be valid URL | Store initialization | During store factory initialization |
| DBTimeout | Used for context timeout | Operation reliability | During SQL operations |
| BlockHeightRetentionAdjustment | Bounds checking applied | Retention calculation | During DAH calculations |
| logging | Boolean string check | Logging wrapper creation | During store initialization |

## Configuration Examples

### PostgreSQL Store

```text
utxostore = "postgres://user:pass@host:5432/db?logging=true"
utxostore_dbTimeout = "60s"
```

### Aerospike Store

```text
utxostore = "aerospike://host:3000/namespace"
utxostore_spendBatcherSize = 200
```

### SQLite Store

```text
utxostore = "sqlite:///data/utxo.db?logging=true"
utxostore_blockHeightRetentionAdjustment = 100
```

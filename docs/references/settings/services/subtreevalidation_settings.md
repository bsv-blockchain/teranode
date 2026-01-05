# Subtree Validation Service Settings

**Related Topic**: [Subtree Validation Service](../../../topics/services/subtreeValidation.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| QuorumPath | string | "" | subtree_quorum_path | Quorum coordination directory |
| QuorumAbsoluteTimeout | time.Duration | 30s | subtree_quorum_absolute_timeout | Quorum operation timeout |
| SubtreeStore | *url.URL | "" | subtreestore | **CRITICAL** - Subtree data storage |
| GetMissingTransactions | int | max(4, CPU/2) | subtreevalidation_getMissingTransactions | **CRITICAL** - Missing transaction retrieval concurrency |
| GRPCAddress | string | "localhost:8089" | subtreevalidation_grpcAddress | gRPC client connections |
| GRPCListenAddress | string | ":8089" | subtreevalidation_grpcListenAddress | **CRITICAL** - gRPC server binding, health checks only run if not empty |
| ProcessTxMetaUsingCacheBatchSize | int | 1024 | subtreevalidation_processTxMetaUsingCache_BatchSize | **CRITICAL** - Cache processing batch size |
| ProcessTxMetaUsingCacheConcurrency | int | 32 | subtreevalidation_processTxMetaUsingCache_Concurrency | **CRITICAL** - Cache processing concurrency |
| ProcessTxMetaUsingCacheMissingTxThreshold | int | 1 | subtreevalidation_processTxMetaUsingCache_MissingTxThreshold | Cache miss threshold |
| SubtreeBlockHeightRetention | uint32 | globalBlockHeightRetention | subtreevalidation_subtreeBlockHeightRetention | Block height retention |
| SubtreeDAHConcurrency | int | 8 | subtreevalidation_subtreeDAHConcurrency | DAH processing concurrency |
| TxMetaCacheEnabled | bool | true | subtreevalidation_txMetaCacheEnabled | **CRITICAL** - Transaction metadata cache |
| TxMetaCacheMaxMB | int | 256 | txMetaCacheMaxMB | Cache memory limit |
| TxChanBufferSize | int | 0 | subtreevalidation_txChanBufferSize | Transaction channel buffer size |
| BatchMissingTransactions | bool | true | subtreevalidation_batch_missing_transactions | **CRITICAL** - Missing transaction batching |
| SpendBatcherSize | int | 1024 | subtreevalidation_spendBatcherSize | **CRITICAL** - Spend operation batch size and concurrency control |
| MissingTransactionsBatchSize | int | 16384 | subtreevalidation_missingTransactionsBatchSize | **CRITICAL** - Missing transaction batch size |
| PercentageMissingGetFullData | float64 | 20 | subtreevalidation_percentageMissingGetFullData | **CRITICAL** - Full subtree vs individual transaction threshold |
| BlacklistedBaseURLs | map[string]struct{} | {} | subtreevalidation_blacklisted_baseurls | URL blacklisting |
| BlockHeightRetentionAdjustment | int32 | 0 | subtreevalidation_blockHeightRetentionAdjustment | Block height retention adjustment |
| OrphanageTimeout | time.Duration | 15m | subtreevalidation_orphanageTimeout | Orphaned transaction timeout |
| OrphanageMaxSize | int | 100000 | subtreevalidation_orphanageMaxSize | **CRITICAL** - Maximum orphanage transactions |
| CheckBlockSubtreesConcurrency | int | 32 | subtreevalidation_check_block_subtrees_concurrency | **CRITICAL** - Block subtree checking concurrency |
| PauseTimeout | time.Duration | 5m | subtreevalidation_pauseTimeout | **CRITICAL** - Maximum pause duration |
| TxBatchSize | int | 1048576 | subtreevalidation_check_block_subtrees_tx_batch_size | Transaction batch size for CheckBlockSubtrees (0 = no batching) |
| UseOrderedLevelAlgorithm | bool | true | subtreevalidation_useOrderedLevelAlgorithm | **CRITICAL** - Optimized O(V*I) algorithm for ordered transactions |

## Configuration Dependencies

### Cache Processing
- `ProcessTxMetaUsingCacheBatchSize`, `ProcessTxMetaUsingCacheConcurrency`, and `ProcessTxMetaUsingCacheMissingTxThreshold` work together
- Controls cache-based transaction metadata processing performance

### Missing Transaction Handling
- When `BatchMissingTransactions = true`, uses `MissingTransactionsBatchSize` and `GetMissingTransactions`
- `PercentageMissingGetFullData` determines full subtree vs individual transaction fetching

### Concurrency Control
- `CheckBlockSubtreesConcurrency` controls block subtree checking operations
- `SpendBatcherSize` controls spend operation batch processing and concurrency limits
- `GetMissingTransactions` controls missing transaction retrieval concurrency
- `TxBatchSize` controls transaction batching for CheckBlockSubtrees (0 disables batching)

### Algorithm Optimization
- `UseOrderedLevelAlgorithm` enables optimized O(V*I) algorithm assuming transactions are ordered
- When true, improves performance for ordered transaction processing

### gRPC Server Management
- When `GRPCListenAddress` is not empty, gRPC server starts and health checks are enabled

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| SubtreeStore | blob.Store | **CRITICAL** - Subtree data storage and retrieval |
| TxStore | blob.Store | **CRITICAL** - Transaction data access |
| UTXOStore | utxo.Store | **CRITICAL** - UTXO operations and validation |
| ValidatorClient | validator.Interface | **CRITICAL** - Transaction validation operations |

## Validation Rules

| Setting | Validation | Impact | When Checked |
|---------|------------|--------|-------------|
| GRPCListenAddress | Health checks only if not empty | Service monitoring | During service initialization |
| SubtreeStore | Must be valid URL format | Storage access | During service initialization |
| TxMetaCacheEnabled | Controls cache usage | Performance | During transaction processing |
| PauseTimeout | Controls maximum pause duration | Processing control | During block validation |
| UseOrderedLevelAlgorithm | Controls algorithm selection | Performance optimization | During subtree validation |

## Configuration Examples

### Basic Configuration

```bash
subtreevalidation_grpcListenAddress=:8089
subtreestore=memory:///
```

### Performance Tuning

```bash
subtreevalidation_check_block_subtrees_concurrency=64
subtreevalidation_getMissingTransactions=16
subtreevalidation_spendBatcherSize=2048
subtreevalidation_orphanageMaxSize=200000
```

### Cache Configuration

```bash
subtreevalidation_txMetaCacheEnabled=true
txMetaCacheMaxMB=512
subtreevalidation_processTxMetaUsingCache_BatchSize=2048
```

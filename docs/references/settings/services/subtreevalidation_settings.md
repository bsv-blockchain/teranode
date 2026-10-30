# Subtree Validation Service Settings

**Related Topic**: [Subtree Validation Service](../../../topics/services/subtreeValidation.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| QuorumPath | string | "" | subtree_quorum_path | Quorum coordination directory |
| QuorumAbsoluteTimeout | time.Duration | 30s | subtree_quorum_absolute_timeout | Quorum operation timeout |
| SubtreeStore | *url.URL | "" | subtreestore | **CRITICAL** - Subtree data storage |
| GetMissingTransactions | int | max(4, CPU/2) | subtreevalidation_getMissingTransactions | **CRITICAL** - Missing transaction retrieval concurrency |
| GRPCAddress | string | "localhost:8089" (Go default; overridden to `localhost:8086` by `settings.conf` via `SUBTREE_VALIDATION_GRPC_PORT`) | subtreevalidation_grpcAddress | gRPC client connections |
| GRPCListenAddress | string | ":8089" (Go default; overridden to `:8086` by `settings.conf` via `SUBTREE_VALIDATION_GRPC_PORT`) | subtreevalidation_grpcListenAddress | **CRITICAL** - gRPC server binding, health checks only run if not empty |
| ProcessTxMetaUsingCacheConcurrency | int | 32 | subtreevalidation_processTxMetaUsingCache_Concurrency | **CRITICAL** - Cache-probe workers shared by all subtrees being validated (server-wide, not per subtree; peer subtrees and block validation each get a pool of this size) |
| PeerSubtreeConcurrency | int | 32 | subtreevalidation_peerSubtreeConcurrency | Peer-announced subtrees validated at once (each holds ~1 GiB per 1M txs); taken before the quorum lock, so block validation never waits behind the queue. Block subtrees do not use it. 0 or less disables the limit |
| TxMetaTombstoneWindow | time.Duration | 30m | subtreevalidation_txMetaTombstoneWindow | How long a txmeta DELETE (a tx demoted to conflicting) keeps that tx out of the txmeta cache, against late ADDs and read-through writes; entries live one to two windows, about 60 MB per million demoted txs. Must exceed the worst txmeta consumer lag; must be positive |
| SkippedSubtreeBackfillMax | int | 4096 | subtreevalidation_skippedSubtreeBackfillMax | Peer subtrees announced while catching up that are kept and validated once the node is RUNNING again (deduplicated by hash, oldest dropped when full, replayed with at most half the peer-subtree slots). 0 or less turns the backfill off |
| ProcessTxMetaUsingCacheMissingTxThreshold | int | 1 | subtreevalidation_processTxMetaUsingCache_MissingTxThreshold | Cache miss threshold |
| BlockHeightRetentionAdjustment | int32 | 0 | subtreevalidation_blockHeightRetentionAdjustment | Adjustment applied to `GlobalBlockHeightRetention` (see `GetSubtreeValidationBlockHeightRetention`) |
| SubtreeDAHConcurrency | int | 8 | subtreevalidation_subtreeDAHConcurrency | DAH processing concurrency |
| TxMetaCacheEnabled | bool | true | subtreevalidation_txMetaCacheEnabled | **CRITICAL** - Transaction metadata cache |
| TxMetaCacheMaxMB | int | 256 | txMetaCacheMaxMB | Cache memory limit |
| TxChanBufferSize | int | 0 | subtreevalidation_txChanBufferSize | Transaction channel buffer size |
| BatchMissingTransactions | bool | true | subtreevalidation_batch_missing_transactions | **CRITICAL** - Missing transaction batching |
| SpendBatcherSize | int | 1024 | subtreevalidation_spendBatcherSize | **CRITICAL** - Spend operation batch size and concurrency control |
| MissingTransactionsBatchSize | int | 16384 | subtreevalidation_missingTransactionsBatchSize | **CRITICAL** - Missing transaction batch size |
| PercentageMissingGetFullData | float64 | 20 | subtreevalidation_percentageMissingGetFullData | **CRITICAL** - Full subtree vs individual transaction threshold |
| BlacklistedBaseURLs | map[string]struct{} | {} | subtreevalidation_blacklisted_baseurls | URL blacklisting |
| CheckBlockSubtreesConcurrency | int | 32 | subtreevalidation_check_block_subtrees_concurrency | **CRITICAL** - Block subtree checking concurrency |
| PauseTimeout | time.Duration | 5m | subtreevalidation_pauseTimeout | **CRITICAL** - Maximum pause duration |
| TxBatchSize | int | 1048576 | subtreevalidation_check_block_subtrees_tx_batch_size | Transaction batch size for CheckBlockSubtrees (0 = no batching) |
| UseOrderedLevelAlgorithm | bool | true | subtreevalidation_useOrderedLevelAlgorithm | **CRITICAL** - Optimized O(V*I) algorithm for ordered transactions |
| BlocksOnly | bool | false | subtreevalidation_blocks_only | Only process subtrees from blocks, skip peer-announced subtrees |

## Configuration Dependencies

### Cache Processing

- `ProcessTxMetaUsingCacheConcurrency` (server-wide probe workers, one pool for peer subtrees and one for block validation) and `ProcessTxMetaUsingCacheMissingTxThreshold` work together
- A subtree's cache lookups are grouped by cache bucket range; each range is one batched read on one of the shared workers

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
subtreevalidation_grpcListenAddress=:8086
subtreestore=memory:///
```

### Performance Tuning

```bash
subtreevalidation_check_block_subtrees_concurrency=64
subtreevalidation_getMissingTransactions=16
subtreevalidation_spendBatcherSize=2048
```

### Cache Configuration

```bash
subtreevalidation_txMetaCacheEnabled=true
txMetaCacheMaxMB=512
```

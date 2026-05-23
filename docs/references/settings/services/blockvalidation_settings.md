# Block Validation Service Settings

**Related Topic**: [Block Validation Service](../../../topics/services/blockValidation.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| MaxRetries | int | 3 | blockValidationMaxRetries | General retry behavior |
| RetrySleep | time.Duration | 1s | blockValidationRetrySleep | Retry delay timing |
| GRPCAddress | string | "localhost:8088" | blockvalidation_grpcAddress | Client connection address |
| GRPCListenAddress | string | ":8088" | blockvalidation_grpcListenAddress | **CRITICAL** - gRPC server binding (service skipped if empty) |
| KafkaWorkers | int | 0 | blockvalidation_kafkaWorkers | Kafka consumer parallelism |
| LocalSetTxMinedConcurrency | int | 8 | blockvalidation_localSetTxMinedConcurrency | Transaction mining concurrency |
| MaxPreviousBlockHeadersToCheck | uint64 | 100 | blockvalidation_maxPreviousBlockHeadersToCheck | Block header validation depth |
| MissingTransactionsBatchSize | int | 5000 | blockvalidation_missingTransactionsBatchSize | Missing transaction batch size |
| ProcessTxMetaUsingCacheBatchSize | int | 1024 | blockvalidation_processTxMetaUsingCache_BatchSize | Cache processing batch size |
| ProcessTxMetaUsingCacheConcurrency | int | 32 | blockvalidation_processTxMetaUsingCache_Concurrency | Cache processing concurrency |
| ProcessTxMetaUsingCacheMissingTxThreshold | int | 1 | blockvalidation_processTxMetaUsingCache_MissingTxThreshold | Cache miss threshold |
| ProcessTxMetaUsingStoreBatchSize | int | max(4, CPU/2) | blockvalidation_processTxMetaUsingStore_BatchSize | Store processing batch size |
| ProcessTxMetaUsingStoreConcurrency | int | 32 | blockvalidation_processTxMetaUsingStore_Concurrency | Store processing concurrency |
| ProcessTxMetaUsingStoreMissingTxThreshold | int | 1 | blockvalidation_processTxMetaUsingStore_MissingTxThreshold | Store miss threshold |
| SkipCheckParentMined | bool | false | blockvalidation_skipCheckParentMined | Parent block mining validation |
| SubtreeFoundChConcurrency | int | 1 | blockvalidation_subtreeFoundChConcurrency | Subtree processing concurrency |
| SubtreeValidationAbandonThreshold | int | 1 | blockvalidation_subtree_validation_abandon_threshold | Subtree validation abandonment |
| ValidateBlockSubtreesConcurrency | int | max(4, CPU/2) | blockvalidation_validateBlockSubtreesConcurrency | Block subtree validation concurrency |
| ValidationMaxRetries | int | 3 | blockvalidation_validation_max_retries | Validation retry attempts |
| ValidationRetrySleep | time.Duration | 5s | blockvalidation_validation_retry_sleep | Validation retry delay |
| OptimisticMining | bool | true | blockvalidation_optimistic_mining | Optimistic mining behavior |
| IsParentMinedRetryMaxRetry | int | 45 | blockvalidation_isParentMined_retry_max_retry | Parent mining check retries |
| IsParentMinedRetryBackoffMultiplier | int | 4 | blockvalidation_isParentMined_retry_backoff_multiplier | Parent mining retry backoff multiplier |
| IsParentMinedRetryBackoffDuration | time.Duration | 20ms | blockvalidation_isParentMined_retry_backoff_duration | Parent mining retry backoff base duration |
| SubtreeGroupConcurrency | int | 1 | blockvalidation_subtreeGroupConcurrency | Subtree group processing concurrency |
| BlockFoundChBufferSize | int | 1000 | blockvalidation_blockFoundCh_buffer_size | Block discovery pipeline buffer |
| CatchupChBufferSize | int | 100 | blockvalidation_catchupCh_buffer_size | Catchup processing pipeline buffer |
| UseCatchupWhenBehind | bool | false | blockvalidation_useCatchupWhenBehind | **CRITICAL** - Catchup mode enablement |
| CatchupConcurrency | int | max(4, CPU/2) | blockvalidation_catchupConcurrency | Catchup processing concurrency |
| ValidationWarmupCount | int | 128 | blockvalidation_validation_warmup_count | Validation warmup behavior |
| BatchMissingTransactions | bool | false | blockvalidation_batch_missing_transactions | Missing transaction batching |
| CheckSubtreeFromBlockTimeout | time.Duration | 5m | blockvalidation_check_subtree_from_block_timeout | Subtree validation timeout |
| CheckSubtreeFromBlockRetries | int | 5 | blockvalidation_check_subtree_from_block_retries | Subtree validation retries |
| CheckSubtreeFromBlockRetryBackoffDuration | time.Duration | 30s | blockvalidation_check_subtree_from_block_retry_backoff_duration | Subtree retry backoff |
| SecretMiningThreshold | uint32 | 99 | blockvalidation_secret_mining_threshold | **CRITICAL** - Secret mining detection |
| PreviousBlockHeaderCount | uint64 | 100 | blockvalidation_previous_block_header_count | **CRITICAL** - Header chain cache size |
| CatchupMaxRetries | int | 3 | blockvalidation_catchup_max_retries | Catchup operation retries |
| CatchupIterationTimeout | int | 30 | blockvalidation_catchup_iteration_timeout | **CRITICAL** - Catchup iteration timeout |
| CatchupOperationTimeout | int | 300 | blockvalidation_catchup_operation_timeout | **CRITICAL** - Catchup operation timeout |
| CatchupMaxAccumulatedHeaders | int | 100000 | blockvalidation_max_accumulated_headers | **CRITICAL** - Memory protection during catchup |
| CircuitBreakerFailureThreshold | int | 5 | blockvalidation_circuit_breaker_failure_threshold | Circuit breaker failure detection |
| CircuitBreakerSuccessThreshold | int | 2 | blockvalidation_circuit_breaker_success_threshold | Circuit breaker recovery |
| CircuitBreakerTimeoutSeconds | int | 30 | blockvalidation_circuit_breaker_timeout_seconds | Circuit breaker timeout |
| MaxBlocksBehindBlockAssembly | int | 20 | blockvalidation_maxBlocksBehindBlockAssembly | **CRITICAL** - Max blocks behind block assembly |
| PeriodicProcessingInterval | time.Duration | 1m | blockvalidation_periodic_processing_interval | Periodic processing interval |
| RecentBlockIDsLimit | uint64 | 50000 | blockvalidation_recentBlockIDsLimit | **CRITICAL** - Fast-path double-spend checking window |
| MaxParallelForks | int | 4 | blockvalidation_max_parallel_forks | Maximum parallel fork processing |
| MaxTrackedForks | int | 1000 | blockvalidation_max_tracked_forks | Maximum total forks tracked |
| NearForkThreshold | int | 0 | blockvalidation_near_fork_threshold | Near fork detection (0=coinbase maturity/2) |
| FetchLargeBatchSize | int | 100 | blockvalidation_fetch_large_batch_size | Block fetch batch size |
| FetchNumWorkers | int | 1 | blockvalidation_fetch_num_workers | Block fetch worker goroutines |
| FetchBufferSize | int | 50 | blockvalidation_fetch_buffer_size | Block fetch channel buffer |
| SubtreeFetchConcurrency | int | 8 | blockvalidation_subtree_fetch_concurrency | Concurrent subtree fetches per block |
| SubtreeBatchSize | int | 16 | blockvalidation_subtree_batch_size | Subtrees processed per batch in quick validation |
| SubtreeBatchPrefetchDepth | int | 2 | blockvalidation_subtree_batch_prefetch_depth | Batches to prefetch ahead in pipeline (0=sequential) |
| GetBlockTransactionsConcurrency | int | 64 | blockvalidation_get_block_transactions_concurrency | Block transaction fetch concurrency |
| ExtendTransactionTimeout | time.Duration | 120s | blockvalidation_extend_transaction_timeout | Timeout for extending transaction inputs |
| SubtreeBatchWriteConcurrency | int | 64 | blockvalidation_subtree_batch_write_concurrency | Concurrent subtree file writes per batch |
| CatchupMinThroughputKBps | int | 100 | blockvalidation_catchup_min_throughput_kbps | Minimum throughput (KB/s) before switching peers |
| CatchupParallelFetchEnabled | bool | true | blockvalidation_catchup_parallel_fetch_enabled | Enable parallel fetching from multiple peers |
| CatchupParallelFetchWorkers | int | 3 | blockvalidation_catchup_parallel_fetch_workers | Number of parallel fetch workers |

## Configuration Dependencies

### Service Startup

- Service skipped (not added to ServiceManager) if `GRPCListenAddress` is empty
- Kafka consumer created for block processing

### Catchup Mode

- When `UseCatchupWhenBehind = true`, all catchup settings control behavior
- `CatchupMaxAccumulatedHeaders` prevents memory exhaustion
- Timeout settings control iteration and operation limits
- Catchup automatically engages when node falls behind

### Optimistic Mining

- `OptimisticMining = true`: Enables background validation for performance
- Block validation proceeds while subtree validation runs in background
- Can be overridden per-validation via DisableOptimisticMining option
- Disabled during catchup mode for better performance

### Quick Validation Pipeline

For checkpoint-verified blocks, a fan-in pipeline overlaps I/O with processing:

- `SubtreeBatchPrefetchDepth` controls how many batches to prefetch ahead (default: 2)
- Setting to 0 disables the pipeline and uses sequential processing
- Three pipeline stages:
  1. **Reader**: Prefetches subtree batches from disk
  2. **Extender**: Extends transactions with UTXO data (sequential for map dependency)
  3. **Processor**: Creates/spends UTXOs and writes files in parallel per batch

### Transaction Metadata Processing
- Cache and store processing work together with threshold-based fallback
- Batch sizes and concurrency settings control performance

### Secret Mining Detection
- `SecretMiningThreshold` uses `PreviousBlockHeaderCount` for analysis
- Detection triggers when block difference exceeds threshold

### Two-Phase Double-Spend Detection
- `RecentBlockIDsLimit` controls the size of the fast-path in-memory block ID window
- Transactions mined in blocks within this window are detected immediately (fast path)
- Transactions mined in older blocks trigger a blockchain service query (slow path)
- Larger values use more memory but reduce slow-path queries
- Default of 50,000 covers approximately 347 days of blocks at 10-minute intervals

### Channel Buffer Management
- `BlockFoundChBufferSize` and `CatchupChBufferSize` must accommodate processing loads

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| UTXOStore | utxo.Store | **CRITICAL** - UTXO operations and validation |
| TxStore | blob.Store | **CRITICAL** - Transaction data access |
| SubtreeStore | blob.Store | **CRITICAL** - Subtree validation |
| BlockchainClient | blockchain.ClientI | **CRITICAL** - Blockchain operations and header retrieval |
| SubtreeValidationClient | subtreevalidation.ClientI | **CRITICAL** - Subtree validation operations |
| ValidatorClient | validator.ClientI | **CRITICAL** - Block validation operations |

## Validation Rules

| Setting | Validation | Impact | When Checked |
|---------|------------|--------|-------------|
| GRPCListenAddress | Must not be empty | Service skipped if empty | During daemon startup |
| UseCatchupWhenBehind | Controls catchup mode activation | Chain synchronization |
| CatchupMaxAccumulatedHeaders | Limits memory usage | Memory protection |
| SecretMiningThreshold | Enables attack detection | Security |

## Configuration Examples

### Basic Configuration

```bash
blockvalidation_grpcListenAddress=:8088
blockvalidation_useCatchupWhenBehind=false
```

### High Performance Configuration

```bash
blockvalidation_validateBlockSubtreesConcurrency=16
blockvalidation_processTxMetaUsingStoreBatchSize=2048
blockvalidation_catchupConcurrency=8
blockvalidation_fetch_num_workers=32
blockvalidation_subtree_batch_size=32
```

### Catchup Mode Configuration

```bash
blockvalidation_useCatchupWhenBehind=true
blockvalidation_catchup_max_retries=5
blockvalidation_catchup_iteration_timeout=60
blockvalidation_max_accumulated_headers=50000
```

### Pipeline Processing Configuration

```bash
# Enable pipeline with larger prefetch depth for high-latency storage
blockvalidation_subtree_batch_prefetch_depth=4
blockvalidation_subtree_batch_size=32

# Disable pipeline (sequential processing)
blockvalidation_subtree_batch_prefetch_depth=0
```

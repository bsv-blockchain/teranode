# Block Validation Service Settings

**Related Topic**: [Block Validation Service](../../../topics/services/blockValidation.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| MaxRetries | int | 3 | blockValidationMaxRetries | General retry behavior |
| RetrySleep | time.Duration | 1s | blockValidationRetrySleep | Retry delay timing |
| GRPCAddress | string | "localhost:8088" | blockvalidation_grpcAddress | Client connection address |
| GRPCListenAddress | string | ":8088" | blockvalidation_grpcListenAddress | **CRITICAL** - gRPC server binding, health checks only run if not empty |
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
| IsParentMinedRetryMaxRetry | int | 20 | blockvalidation_isParentMined_retry_max_retry | Parent mining check retries |
| IsParentMinedRetryBackoffMultiplier | int | 30 | blockvalidation_isParentMined_retry_backoff_multiplier | Parent mining retry backoff |
| SubtreeGroupConcurrency | int | 1 | blockvalidation_subtreeGroupConcurrency | Subtree group processing concurrency |
| BlockFoundChBufferSize | int | 1000 | blockvalidation_blockFoundCh_buffer_size | Block discovery pipeline buffer |
| CatchupChBufferSize | int | 10 | blockvalidation_catchupCh_buffer_size | Catchup processing pipeline buffer |
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

## Configuration Dependencies

### Catchup Mode
- When `UseCatchupWhenBehind = true`, all catchup settings control behavior
- `CatchupMaxAccumulatedHeaders` prevents memory exhaustion
- Timeout settings control iteration and operation limits

### Transaction Metadata Processing
- Cache and store processing work together with threshold-based fallback
- Batch sizes and concurrency settings control performance

### Secret Mining Detection
- `SecretMiningThreshold` uses `PreviousBlockHeaderCount` for analysis
- Detection triggers when block difference exceeds threshold

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

| Setting | Validation | Impact |
|---------|------------|--------|
| GRPCListenAddress | Health checks only if not empty | Service monitoring |
| UseCatchupWhenBehind | Controls catchup mode activation | Chain synchronization |
| CatchupMaxAccumulatedHeaders | Limits memory usage | Memory protection |
| SecretMiningThreshold | Enables attack detection | Security |

## Configuration Examples

### Basic Configuration

```text
blockvalidation_grpcListenAddress = ":8088"
blockvalidation_useCatchupWhenBehind = false
```

### High Performance Configuration

```text
blockvalidation_validateBlockSubtreesConcurrency = 16
blockvalidation_processTxMetaUsingStoreBatchSize = 2048
blockvalidation_catchupConcurrency = 8
```

### Catchup Mode Configuration

```text
blockvalidation_useCatchupWhenBehind = true
blockvalidation_catchup_max_retries = 5
blockvalidation_catchup_iteration_timeout = 60
blockvalidation_max_accumulated_headers = 50000
```

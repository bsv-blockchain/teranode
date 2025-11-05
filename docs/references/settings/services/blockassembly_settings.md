# Block Assembly Service Settings

**Related Topic**: [Block Assembly Service](../../../topics/services/blockAssembly.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| Disabled | bool | false | blockassembly_disabled | **CRITICAL** - Disables all block assembly operations |
| GRPCAddress | string | "localhost:8085" | blockassembly_grpcAddress | Client connection address |
| GRPCListenAddress | string | ":8085" | blockassembly_grpcListenAddress | **CRITICAL** - gRPC server binding |
| GRPCMaxRetries | int | 3 | blockassembly_grpcMaxRetries | gRPC client retry attempts |
| GRPCRetryBackoff | time.Duration | 2s | blockassembly_grpcRetryBackoff | Retry delay timing |
| LocalDAHCache | string | "" | blockassembly_localDAHCache | Configuration placeholder |
| MaxBlockReorgCatchup | int | 100 | blockassembly_maxBlockReorgCatchup | Reorganization processing limit |
| MaxBlockReorgRollback | int | 100 | blockassembly_maxBlockReorgRollback | Reorganization rollback limit |
| MoveBackBlockConcurrency | int | 375 | blockassembly_moveBackBlockConcurrency | Block rollback parallelism |
| ProcessRemainderTxHashesConcurrency | int | 375 | blockassembly_processRemainderTxHashesConcurrency | Transaction hash processing parallelism |
| SendBatchSize | int | 100 | blockassembly_sendBatchSize | Batch operation size |
| SendBatchTimeout | int | 2 | blockassembly_sendBatchTimeout | Batch operation timeout |
| SubtreeProcessorBatcherSize | int | 1000 | blockassembly_subtreeProcessorBatcherSize | Subtree processing batch size |
| SubtreeProcessorConcurrentReads | int | 375 | blockassembly_subtreeProcessorConcurrentReads | **CRITICAL** - Subtree read parallelism |
| NewSubtreeChanBuffer | int | 1000 | blockassembly_newSubtreeChanBuffer | **CRITICAL** - New subtree channel buffer |
| SubtreeRetryChanBuffer | int | 1000 | blockassembly_subtreeRetryChanBuffer | **CRITICAL** - Retry channel buffer |
| SubmitMiningSolutionWaitForResponse | bool | true | blockassembly_SubmitMiningSolution_waitForResponse | **CRITICAL** - Synchronous mining solution processing |
| InitialMerkleItemsPerSubtree | int | 1048576 | initial_merkle_items_per_subtree | Initial subtree size |
| MinimumMerkleItemsPerSubtree | int | 1024 | minimum_merkle_items_per_subtree | Minimum subtree size |
| MaximumMerkleItemsPerSubtree | int | 1048576 | maximum_merkle_items_per_subtree | Maximum subtree size |
| DoubleSpendWindow | time.Duration | Calculated | N/A | Double-spend detection window |
| MaxGetReorgHashes | int | 10000 | blockassembly_maxGetReorgHashes | **CRITICAL** - Reorganization hash limit |
| MinerWalletPrivateKeys | []string | [] | miner_wallet_private_keys | Mining wallet keys |
| DifficultyCache | bool | true | blockassembly_difficultyCache | Difficulty calculation caching |
| UseDynamicSubtreeSize | bool | false | blockassembly_useDynamicSubtreeSize | Dynamic subtree sizing |
| MiningCandidateCacheTimeout | time.Duration | 5s | blockassembly_miningCandidateCacheTimeout | **CRITICAL** - Mining candidate cache validity |
| BlockchainSubscriptionTimeout | time.Duration | 5m | blockassembly_blockchainSubscriptionTimeout | Blockchain subscription timeout |

## Configuration Dependencies

### Service Disable
- When `Disabled = true`, all block assembly operations return early
- All other settings become irrelevant when service is disabled

### Channel Buffer Management
- `NewSubtreeChanBuffer` and `SubtreeRetryChanBuffer` must accommodate concurrent processing loads
- Buffer sizes affect pipeline performance and memory usage

### Mining Solution Processing
- `SubmitMiningSolutionWaitForResponse` controls synchronous vs asynchronous processing
- Affects `MiningCandidateCacheTimeout` behavior and response handling

### Reorganization Handling
- `MaxGetReorgHashes` prevents excessive memory usage during large reorganizations
- Works with `MaxBlockReorgCatchup`, `MaxBlockReorgRollback`, `MoveBackBlockConcurrency`

### Dynamic Subtree Sizing
- When `UseDynamicSubtreeSize = true`, uses `InitialMerkleItemsPerSubtree`, `MinimumMerkleItemsPerSubtree`, `MaximumMerkleItemsPerSubtree`

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| TxStore | blob.Store | Transaction data access |
| UTXOStore | utxostore.Store | **CRITICAL** - UTXO operations and validation |
| SubtreeStore | blob.Store | **CRITICAL** - Subtree storage and retrieval |
| BlockchainClient | blockchain.ClientI | **CRITICAL** - Blockchain operations, block submission |

## Validation Rules

| Setting | Validation | Impact |
|---------|------------|--------|
| GRPCListenAddress | Health checks only run if not empty | Service monitoring |
| MaxGetReorgHashes | Limits reorganization processing | Memory protection |
| Channel Buffers | Must accommodate processing loads | Pipeline performance |

## Configuration Examples

### Basic Configuration

```text
blockassembly_grpcListenAddress = ":8085"
blockassembly_disabled = false
```

### Performance Tuning

```text
blockassembly_subtreeProcessorConcurrentReads = 500
blockassembly_newSubtreeChanBuffer = 2000
blockassembly_subtreeRetryChanBuffer = 2000
```

### Mining Configuration

```text
blockassembly_SubmitMiningSolution_waitForResponse = true
blockassembly_miningCandidateCacheTimeout = 10s
miner_wallet_private_keys = "key1|key2"
```

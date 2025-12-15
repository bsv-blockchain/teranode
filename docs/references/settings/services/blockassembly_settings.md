# Block Assembly Service Settings

**Related Topic**: [Block Assembly Service](../../../topics/services/blockAssembly.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| Disabled | bool | false | blockassembly_disabled | Service-level kill switch, all operations return early |
| GRPCAddress | string | "localhost:8085" | blockassembly_grpcAddress | Client connection address |
| GRPCListenAddress | string | ":8085" | blockassembly_grpcListenAddress | **CRITICAL** - gRPC server binding (service won't start if empty) |
| GRPCMaxRetries | int | 3 | blockassembly_grpcMaxRetries | gRPC client retry attempts |
| GRPCRetryBackoff | time.Duration | 2s | blockassembly_grpcRetryBackoff | Retry delay timing |
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
| DoubleSpendWindow | time.Duration | BlockTime * 6 | N/A | Double-spend detection window (calculated) |
| MaxGetReorgHashes | int | 10000 | blockassembly_maxGetReorgHashes | **CRITICAL** - Reorganization hash limit |
| MinerWalletPrivateKeys | []string | [] | miner_wallet_private_keys | Mining wallet keys |
| DifficultyCache | bool | true | blockassembly_difficultyCache | Difficulty calculation caching |
| UseDynamicSubtreeSize | bool | false | blockassembly_useDynamicSubtreeSize | Dynamic subtree sizing |
| MiningCandidateCacheTimeout | time.Duration | 5s | blockassembly_miningCandidateCacheTimeout | Mining candidate cache validity (same height) |
| MiningCandidateSmartCacheMaxAge | time.Duration | 10s | blockassembly_miningCandidateSmartCacheMaxAge | Stale cache max age for high-load scenarios |
| BlockchainSubscriptionTimeout | time.Duration | 5m | blockassembly_blockchainSubscriptionTimeout | Blockchain subscription timeout |

## Hardcoded Settings (Not Configurable)

| Setting | Value | Usage | Code Reference |
|---------|-------|-------|----------------|
| GetMiningCandidateResponseTimeout | Not configurable | Mining candidate generation timeout | BlockAssembler.go:980 |
| GetMiningCandidateSendTimeout | Not configurable | Mining candidate send timeout | BlockAssembler.go:1049 |
| ParentValidationBatchSize | Not configurable | Parent chain validation batch size | BlockAssembler.go:1609 |
| OnRestartRemoveInvalidParentChainTxs | Not configurable | Filter invalid txs on restart | BlockAssembler.go:1695 |
| jobTTL | 10 minutes | Mining job cache TTL | Server.go:57 |

## Configuration Dependencies

### Service Startup

- Service skipped if `GRPCListenAddress` is empty
- Channel buffers allocated during Init() based on configured sizes

### Service Disable

- When `Disabled = true`, all block assembly operations return early
- All other settings become irrelevant when service is disabled

### Channel Buffer Management
- `NewSubtreeChanBuffer` and `SubtreeRetryChanBuffer` must accommodate concurrent processing loads
- Buffer sizes affect pipeline performance and memory usage

### Mining Candidate Caching

- `MiningCandidateCacheTimeout`: Cache valid for same height within timeout
- `MiningCandidateSmartCacheMaxAge`: Fallback for stale cache during high load
- Cache invalidated on significant transaction/subtree changes

### Mining Solution Processing

- `SubmitMiningSolutionWaitForResponse` controls synchronous vs asynchronous processing
- Affects response handling and mining candidate cache behavior

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

| Setting | Validation | Impact | When Checked |
|---------|------------|--------|-------------|
| GRPCListenAddress | Must not be empty | Service won't start if empty | During daemon startup |
| MaxGetReorgHashes | Limits reorganization processing | Memory protection during reorgs | During reorg processing |
| Channel Buffers | Must accommodate processing loads | Pipeline performance and backpressure | During Init() |

## Configuration Examples

### Basic Configuration

```text
blockassembly_grpcListenAddress = ":8085"
blockassembly_disabled = false
```

### Performance Tuning

```bash
blockassembly_subtreeProcessorConcurrentReads=500
blockassembly_newSubtreeChanBuffer=2000
blockassembly_subtreeRetryChanBuffer=2000
```

### Mining Configuration

```bash
blockassembly_SubmitMiningSolution_waitForResponse=true
blockassembly_miningCandidateCacheTimeout=10s
blockassembly_miningCandidateSmartCacheMaxAge=15s
miner_wallet_private_keys=key1|key2
```

# Block Assembly Service Settings

**Related Topic**: [Block Assembly Service](../../../topics/services/blockAssembly.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| Disabled | bool | false | blockassembly_disabled | Service-level kill switch, all operations return early |
| GRPCAddress | string | "localhost:8085" | blockassembly_grpcAddress | Client connection address |
| GRPCListenAddress | string | ":8085" | blockassembly_grpcListenAddress | **CRITICAL** - gRPC server binding (service skipped if empty) |
| GRPCMaxRetries | int | 3 | blockassembly_grpcMaxRetries | gRPC client retry attempts |
| GRPCRetryBackoff | time.Duration | 2s | blockassembly_grpcRetryBackoff | Retry delay timing |
| LocalDAHCache | string | "" | blockassembly_localDAHCache | **UNUSED** - Reserved for future DAH caching |
| MaxBlockReorgCatchup | int | 100 | blockassembly_maxBlockReorgCatchup | Map capacity for current chain tracking |
| MaxBlockReorgRollback | int | 100 | blockassembly_maxBlockReorgRollback | **UNUSED** - Defined but not referenced in code |
| MoveBackBlockConcurrency | int | 375 | blockassembly_moveBackBlockConcurrency | Concurrency limit for reorg processing (SubtreeProcessor) |
| ProcessRemainderTxHashesConcurrency | int | 375 | blockassembly_processRemainderTxHashesConcurrency | Concurrency limit for remainder tx hash processing |
| SendBatchSize | int | 100 | blockassembly_sendBatchSize | Client batch size for sending transactions |
| SendBatchTimeout | int | 2 | blockassembly_sendBatchTimeout | Client batch timeout in milliseconds |
| SubtreeProcessorBatcherSize | int | 1000 | blockassembly_subtreeProcessorBatcherSize | Subtree processing batch size |
| SubtreeProcessorConcurrentReads | int | 375 | blockassembly_subtreeProcessorConcurrentReads | **CRITICAL** - Subtree read parallelism |
| NewSubtreeChanBuffer | int | 1000 | blockassembly_newSubtreeChanBuffer | **CRITICAL** - New subtree channel buffer |
| SubtreeRetryChanBuffer | int | 1000 | blockassembly_subtreeRetryChanBuffer | **CRITICAL** - Retry channel buffer |
| SubmitMiningSolutionWaitForResponse | bool | true | blockassembly_SubmitMiningSolution_waitForResponse | **CRITICAL** - Sync (true) vs async (false) mining solution processing |
| InitialMerkleItemsPerSubtree | int | 1048576 | initial_merkle_items_per_subtree | Initial subtree size |
| MinimumMerkleItemsPerSubtree | int | 1024 | minimum_merkle_items_per_subtree | Minimum subtree size |
| MaximumMerkleItemsPerSubtree | int | 1048576 | maximum_merkle_items_per_subtree | Maximum subtree size |
| DoubleSpendWindow | time.Duration | BlockTime * 6 | N/A | Double-spend detection window (calculated) |
| MaxGetReorgHashes | int | 10000 | blockassembly_maxGetReorgHashes | **CRITICAL** - Reorganization hash limit |
| MinerWalletPrivateKeys | []string | [] | miner_wallet_private_keys | Mining wallet keys |
| DifficultyCache | bool | true | blockassembly_difficultyCache | Enables difficulty calculation caching (Blockchain service) |
| UseDynamicSubtreeSize | bool | false | blockassembly_useDynamicSubtreeSize | Dynamic subtree sizing |
| MiningCandidateCacheTimeout | time.Duration | 5s | blockassembly_miningCandidateCacheTimeout | Mining candidate cache validity (same height) |
| MiningCandidateSmartCacheMaxAge | time.Duration | 10s | blockassembly_miningCandidateSmartCacheMaxAge | Stale cache max age for high-load scenarios |
| BlockchainSubscriptionTimeout | time.Duration | 5m | blockassembly_blockchainSubscriptionTimeout | Blockchain event subscription timeout |
| OnRestartValidateParentChain | bool | true | blockassembly_onRestartValidateParentChain | Enables parent chain validation on restart |
| ParentValidationBatchSize | int | 1000 | blockassembly_parentValidationBatchSize | Parent validation batch size |
| OnRestartRemoveInvalidParentChainTxs | bool | false | blockassembly_onRestartRemoveInvalidParentChainTxs | Filters transactions with invalid parent chains |
| GetMiningCandidateSendTimeout | time.Duration | 1s | blockassembly_getMiningCandidate_send_timeout | Timeout sending request on internal channel |
| GetMiningCandidateResponseTimeout | time.Duration | 10s | blockassembly_getMiningCandidate_response_timeout | Timeout waiting for mining candidate response |
| SubtreeAnnouncementInterval | time.Duration | 10s | blockassembly_subtreeAnnouncementInterval | Subtree announcement frequency |

## Hardcoded Settings (Not Configurable)

| Setting | Value | Usage |
|---------|-------|-------|
| jobTTL | 10 minutes | Mining job cache TTL |

## Configuration Dependencies

### Service Startup

- Service skipped (not added to ServiceManager) if `GRPCListenAddress` is empty
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

- `SubmitMiningSolutionWaitForResponse = true`: gRPC call blocks until submission completes
- `SubmitMiningSolutionWaitForResponse = false`: Returns immediately for async processing
- Significantly affects mining pool integration behavior

### Reorganization Handling
- `MaxGetReorgHashes` prevents excessive memory usage during large reorganizations
- Works with `MaxBlockReorgCatchup`, `MaxBlockReorgRollback`, `MoveBackBlockConcurrency`

### Dynamic Subtree Sizing

- When `UseDynamicSubtreeSize = true`, subtree size adjusts based on transaction volume
- Uses `InitialMerkleItemsPerSubtree` as starting size
- Adjusts within `MinimumMerkleItemsPerSubtree` and `MaximumMerkleItemsPerSubtree` bounds

### Parent Chain Validation

- `OnRestartValidateParentChain = true`: Validates transaction parent chains after service restart
- `ParentValidationBatchSize`: Controls batch processing size (default: 1000)
- `OnRestartRemoveInvalidParentChainTxs = true`: Filters out transactions with invalid parent chains
- `OnRestartRemoveInvalidParentChainTxs = false` (default): Keeps transactions despite invalid parents

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
| GRPCListenAddress | Must not be empty | Service skipped if empty | During daemon startup |
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

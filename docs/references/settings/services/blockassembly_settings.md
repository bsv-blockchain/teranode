# Block Assembly Service Settings

**Related Topic**: [Block Assembly Service](../../../topics/services/blockAssembly.md)

## Configuration Settings

| Setting                              | Type          | Default          | Environment Variable                               | Usage                                                                                |
|--------------------------------------|---------------|------------------|----------------------------------------------------|--------------------------------------------------------------------------------------|
| Disabled                             | bool          | false            | blockassembly_disabled                             | Service-level kill switch, all operations return early                               |
| GenerateTipWaitTimeout               | time.Duration | 90s              | blockassembly_generateTipWaitTimeout               | Bounds the generate readiness wait; effective bound is min(this, caller deadline)     |
| GRPCAddress                          | string        | "localhost:8085" | blockassembly_grpcAddress                          | Client connection address                                                            |
| GRPCListenAddress                    | string        | ":8085"          | blockassembly_grpcListenAddress                    | **CRITICAL** - gRPC server binding (service skipped if empty)                        |
| GRPCMaxRetries                       | int           | 3                | blockassembly_grpcMaxRetries                       | gRPC client retry attempts                                                           |
| GRPCRetryBackoff                     | time.Duration | 2s               | blockassembly_grpcRetryBackoff                     | Retry delay timing                                                                   |
| LivenessStallTimeout                 | time.Duration | 0s               | blockassembly_livenessStallTimeout                 | Liveness reports 503 if the main select stops being serviced for this long; 0 = off  |
| LocalDAHCache                        | string        | ""               | blockassembly_localDAHCache                        | **UNUSED** - Reserved for future DAH caching                                         |
| MaxBlockReorgCatchup                 | int           | 100              | blockassembly_maxBlockReorgCatchup                 | Map capacity for current chain tracking                                              |
| MaxBlockReorgRollback                | int           | 100              | blockassembly_maxBlockReorgRollback                | **UNUSED** - Defined but not referenced in code                                      |
| MoveBackBlockConcurrency             | int           | 375              | blockassembly_moveBackBlockConcurrency             | Concurrency limit for reorg processing (SubtreeProcessor)                            |
| ProcessRemainderTxHashesConcurrency  | int           | 375              | blockassembly_processRemainderTxHashesConcurrency  | Concurrency limit for remainder tx hash processing                                   |
| SendBatchSize                        | int           | 100              | blockassembly_sendBatchSize                        | Client batch size for sending transactions                                           |
| SendBatchTimeout                     | int           | 2                | blockassembly_sendBatchTimeout                     | Client batch timeout in seconds                                                      |
| SubtreeProcessorBatcherSize          | int           | 1000             | blockassembly_subtreeProcessorBatcherSize          | Subtree processing batch size                                                        |
| SubtreeProcessorConcurrentReads      | int           | 375              | blockassembly_subtreeProcessorConcurrentReads      | **CRITICAL** - Subtree read parallelism                                              |
| NewSubtreeChanBuffer                 | int           | 1000             | blockassembly_newSubtreeChanBuffer                 | **CRITICAL** - New subtree channel buffer                                            |
| SubtreeRetryChanBuffer               | int           | 1000             | blockassembly_subtreeRetryChanBuffer               | **CRITICAL** - Retry channel buffer                                                  |
| SubmitMiningSolutionWaitForResponse  | bool          | true             | blockassembly_SubmitMiningSolution_waitForResponse | **CRITICAL** - Sync (true) vs async (false) mining solution processing               |
| InitialMerkleItemsPerSubtree         | int           | 1048576          | initial_merkle_items_per_subtree                   | Initial subtree size                                                                 |
| MinimumMerkleItemsPerSubtree         | int           | 1024             | minimum_merkle_items_per_subtree                   | Minimum subtree size                                                                 |
| MaximumMerkleItemsPerSubtree         | int           | 1048576          | maximum_merkle_items_per_subtree                   | Maximum subtree size                                                                 |
| DoubleSpendWindow                    | time.Duration | BlockTime * 6    | N/A                                                | Double-spend detection window (calculated)                                           |
| MaxGetReorgHashes                    | int           | 10000            | blockassembly_maxGetReorgHashes                    | **CRITICAL** - Reorganization hash limit                                             |
| MinerWalletPrivateKeys               | []string      | []               | miner_wallet_private_keys                          | Mining wallet keys                                                                   |
| DifficultyCache                      | bool          | true             | blockassembly_difficultyCache                      | Enables difficulty calculation caching (Blockchain service)                          |
| UseDynamicSubtreeSize                | bool          | false            | blockassembly_useDynamicSubtreeSize                | Dynamic subtree sizing                                                               |
| BlockchainSubscriptionTimeout        | time.Duration | 5m               | blockassembly_blockchainSubscriptionTimeout        | Blockchain event subscription timeout                                                |
| OnRestartValidateParentChain         | bool          | true             | blockassembly_onRestartValidateParentChain         | Enables parent chain validation on restart                                           |
| ParentValidationBatchSize            | int           | 1000             | blockassembly_parentValidationBatchSize            | Parent validation batch size                                                         |
| OnRestartRemoveInvalidParentChainTxs | bool          | true             | blockassembly_onRestartRemoveInvalidParentChainTxs | Filters transactions with invalid parent chains                                      |
| SubtreeStorageWorkers                | int           | 4                | blockassembly_subtreeStorageWorkers                | Workers for subtree storage operations                                               |
| SubtreeAnnouncementInterval          | time.Duration | 10s              | blockassembly_subtreeAnnouncementInterval          | Subtree announcement frequency                                                       |
| UseColumnarBatch                     | bool          | false            | blockassembly_useColumnarBatch                     | Use columnar batch format for data layout                                            |
| UnminedTxDiskSortPath                | string        | ""               | blockassembly_unminedTxDiskSortPath                | Path for unmined transaction disk sorting                                            |
| UnminedTxDiskSortEnabled             | bool          | false            | blockassembly_unminedTxDiskSortEnabled             | Enable disk-based sorting for large mempools                                         |
| UnminedLoadingBatchSize              | int           | 10485760         | blockassembly_unminedLoadingBatchSize              | Batch size for loading unmined transactions                                          |
| ParallelSetIfNotExistsThreshold      | int           | 10000            | blockassembly_parallelSetIfNotExistsThreshold      | Threshold for parallelizing conditional writes                                       |
| StoreTxInpointsForSubtreeMeta        | bool          | true             | blockassembly_storeTxInpointsForSubtreeMeta        | Store transaction input points in subtree metadata (required for checkblocktemplate) |
| IdleSleepDuration                    | time.Duration | 10ms             | blockassembly_idle_sleep_duration                  | Sleep duration when subtree processor queue is empty                                 |
| CoinbaseRecoveryMaxGapBlocks         | int           | 200              | blockassembly_coinbaseRecoveryMaxGapBlocks         | Startup scan window below the tip, and the largest coinbase gap auto-repair will fix |
| CoinbaseRecoveryConsecutiveGood      | int           | 6                | blockassembly_coinbaseRecoveryConsecutiveGood      | Consecutive present coinbases proving the gap floor during walk-back                 |
| CoinbaseRecoveryMaxAttempts          | int           | 3                | blockassembly_coinbaseRecoveryMaxAttempts          | Automatic coinbase-recovery attempts before raising an operator alert                |

## Coinbase Divergence Recovery

Block assembly keeps two pieces of state that have to agree: the chain pointer (the
header it believes it is on) and the coinbase UTXOs it has created. Nothing makes
those two writes atomic, so an unclean shutdown — a crash part-way through the
fast-forward create loop, for instance — can leave the pointer ahead of the coinbase
set. The damage is not visible immediately: it shows up a hundred blocks later, when
a coinbase-maturity spend looks for a parent coinbase that was never written, cannot
find it, and wedges the tip permanently.

To catch that, block assembly runs a check once during startup, after the subtree
processor is running but before it starts consuming block notifications. It walks
down from the persisted tip looking for canonical coinbases missing from the UTXO
store. Each one it finds is repaired in place: it scopes the contiguous gap beneath
that height and re-creates just those coinbases — never the transactions in those
blocks, so the cost is proportional to the number of affected blocks rather than to
chain size. The scan then carries on below the repair, because a repair only proves
a few present coinbases underneath itself and a second hole further down would
otherwise go unnoticed until the next restart.

Three settings bound that work:

- `blockassembly_coinbaseRecoveryMaxGapBlocks` does double duty. It is how far below
  the tip the startup scan looks, and it is the largest gap recovery will repair on
  its own. The two are deliberately the same number: there is no value in detecting a
  divergence deeper than the node is willing to fix automatically. A clean boot costs
  one blockchain-client lookup and one UTXO store read per height in the window, issued
  one after another — and the first of those is a call to the blockchain service, not a
  local read, so the startup cost tracks the latency to that service rather than local
  disk. The scan ends early only when a divergence cannot be repaired, since a refusal
  is structural and would repeat identically at every lower height. Tuning this down to
  limit repair scope also shrinks detection coverage by the same amount.
  A second, non-configurable bound applies underneath it — see the safety floor below —
  so with the default settings the effective reach is the coinbase-maturity window.
- `blockassembly_coinbaseRecoveryConsecutiveGood` guards against under-repair. The
  fast-forward create loop runs one goroutine per block, so a crash can leave a
  present coinbase sitting above still-missing ones. Stopping the walk-back at the
  first present coinbase would miss those, so it keeps going until it has seen this
  many present coinbases in a row.
- `blockassembly_coinbaseRecoveryMaxAttempts` caps automatic retries. Retries exist
  for transient failures (a store or blockchain-client blip), with a short pause
  between them. A gap larger than the cap is structural and escalates straight away
  without spending retries on it.

With the shipped defaults the gap cap never fires. The walk-back stops at the
maturity safety floor described below, so it can never collect more than
`CoinbaseMaturity` (100) misses, while `coinbaseRecoveryMaxGapBlocks` defaults to
200. The cap only becomes a live control when it is set *below* the maturity —
otherwise every escalation you see came from the safety floor, not from the cap.
Its other role, bounding the startup scan window, stays live at any setting.

### The safety floor

Recovery never looks further back than the coinbase-maturity window, no matter how
the settings above are tuned. This is a correctness limit rather than a performance
one, and it is worth understanding because it is what keeps the repair safe.

"No coinbase in the UTXO store" is an ambiguous observation. It is true of a coinbase
that was never created — the divergence being repaired — but it is equally true of one
that was created, matured, fully spent, and then deleted by the pruner. Both look
identical to a lookup. Re-creating the second kind would put outputs that were already
legitimately spent back into the UTXO set as unspent, which is a far worse problem
than the wedge this feature exists to fix.

Coinbase maturity separates the two cases cleanly. A coinbase cannot be spent until
maturity blocks have been built on top of it, and an unspent output is never marked
for deletion, so it is never pruned. Above `tip - CoinbaseMaturity`, therefore, absence
can only mean "never created". Below it, the answer is unknowable from a lookup alone,
so the walk stops rather than guessing. If a genuine divergence extends past that line,
recovery escalates to an operator instead of repairing it.

The `tip` in that subtraction is the chain tip as the UTXO store sees it, not block
assembly's own position. The pruner marks outputs for deletion against the store's
height, and block assembly's pointer can sit hundreds of blocks behind it during a
normal catch-up. Measuring the floor from the lagging pointer would put the bottom of
the window inside already-prunable territory, which is the exact mistake the floor
exists to prevent, so the higher of the two heights is always used. The visible effect
is that a block assembler well behind the chain scans a narrower window, and one behind
by more than the maturity window scans nothing and logs that it skipped the check —
correct, because at that distance nothing it could look at is provably unpruned.

Detection coverage is consequently a bounded window, not the whole chain. A hole
further below the tip than the smaller of `coinbaseRecoveryMaxGapBlocks` and the
maturity window is not found at startup; detecting that cheaply during normal
operation is separate, future work.

Because the maturity window is also the ceiling on how far the walk-back can reach,
a network configured with a small coinbase maturity gets little or nothing out of
this feature. Below `coinbaseRecoveryConsecutiveGood` there are not enough probeable
heights left to prove a floor, so every miss is refused rather than repaired, and at
a maturity of zero — where a coinbase is spendable immediately and so nothing is ever
provably unspent — the feature is a deliberate no-op. Every shipped network uses 100,
which leaves ample room; only custom chain parameters can land in that territory.

When recovery cannot fix a divergence it logs a single `MANUAL INTERVENTION REQUIRED`
line naming `resetblockassembly`, and increments the `escalated` outcome on the
`teranode_blockassembly_coinbase_divergence_total` metric. Startup does **not** fail
in that case — the node boots and keeps building on the diverged tip. That is
deliberate: crash-looping the process helps nobody, and a running node an operator can
inspect is strictly more useful. The trade-off is that the escalation log is a call to
action, not a description of a node that has stopped itself.

Every detection records exactly one follow-up outcome on that metric, so
`detected == repaired + no_gap + escalated + aborted`. A non-zero `no_gap` means the
divergence had already closed by the time recovery scoped it. `aborted` means the node
was shut down while a repair was still retrying — nothing is known to be wrong in that
case, and it deliberately raises no alarm, so a node stopped mid-boot does not tell its
operator that the UTXO state needs manual intervention.

## Hardcoded Settings (Not Configurable)

| Setting | Value | Usage |
|---------|-------|-------|
| jobTTL | 10 minutes | Mining job cache TTL |
| coinbaseRecoveryRetryBackoff | 500 milliseconds | Pause between automatic coinbase-recovery attempts |
| coinbaseRecoveryProbeRetries | 2 | Probe retries the whole startup divergence scan may spend, shared across every height it visits |

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
- `OnRestartRemoveInvalidParentChainTxs = true` (default): Filters out transactions with invalid parent chains
- `OnRestartRemoveInvalidParentChainTxs = false`: Keeps transactions despite invalid parents

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
miner_wallet_private_keys=key1|key2
```

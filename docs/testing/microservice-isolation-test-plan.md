# Microservice Isolation Testing Plan

## Overview

This document defines the complete testing strategy for each Teranode microservice tested **in isolation** — meaning: real business logic, in-process fake infrastructure (in-memory Kafka, SQLite stores, memory blob stores), and no Docker or external services required.

### Current State

| Layer | Status |
|---|---|
| Unit tests (mocks, no Kafka flow) | Exists — most services |
| E2E tests (full Docker stack) | Exists — `test/e2e/`, `test/sequentialtest/` |
| Chaos tests (Toxiproxy) | Exists — `test/chaos/` (8 scenarios) |
| **Service isolation tests (this plan)** | **Missing** |
| Benchmarks (per-service, repeatable) | Partial — validator, subtreevalidation, blockassembly |

### Test Infrastructure Available

| Tool | Location | Purpose |
|---|---|---|
| In-memory Kafka | `util/kafka/in_memory_kafka/` | Kafka produce/consume without broker |
| In-memory blob store | `stores/blob/memory/` | S3/filesystem replacement |
| In-memory blockchain store | `stores/blockchain/sql/` + SQLite | PostgreSQL replacement |
| In-memory UTXO store | `stores/utxo/sql/` + SQLite | Aerospike replacement |
| TestContainers (Aerospike) | `test/utils/aerospike/` | Real Aerospike when needed |
| TestContainers (PostgreSQL) | `test/utils/postgres/` | Real PostgreSQL when needed |
| TestLogger | `ulogger.TestLogger{}` | Structured test logging |
| Base settings | `test.CreateBaseTestSettings(t)` | Base config for tests |

---

## Test Taxonomy

### Level 1: Isolation Unit Tests

A single function or struct, all dependencies mocked. These already exist and should be maintained.

### Level 2: Service Integration Tests (this plan)

A single service started end-to-end with in-process fakes. No mocks of the service under test. Validates the full message flow through a single service boundary:

```
in-memory Kafka topic  →  [ Service Under Test ]  →  in-memory Kafka topic
                                    ↕
                       SQLite / memory stores (in-process)
```

### Level 3: Benchmark Tests

Measure throughput (tx/s, blocks/s), latency (p50, p99), and allocations per operation. Repeatable via `go test -bench`.

### Level 4: Chaos Tests

Inject faults (latency, packet drop, connection reset, resource exhaustion) into a running service and verify graceful degradation. Extends the existing `test/chaos/` framework.

---

## File Structure

```
services/
├── validator/
│   ├── isolation_test.go          # Level 2: service isolation
│   ├── benchmark_test.go          # Level 3: benchmarks (exists, extend)
│   └── chaos_test.go              # Level 4: chaos scenarios

test/
├── isolation/                     # Shared isolation test helpers
│   ├── harness.go                 # Common service harness setup
│   ├── kafka_helper.go            # Subscribe/publish helpers
│   └── store_helper.go            # Store initialization helpers
├── chaos/                         # Existing — extend with service-specific cases
│   ├── scenario_01_...            # Existing Toxiproxy scenarios
│   └── scenario_09_validator_...  # New per-service chaos scenarios
└── benchmarks/                    # Aggregate benchmark runner
    └── run_benchmarks.sh
```

---

## Isolation Test Pattern

All service isolation tests follow this structure:

```go
//go:build integration

package validator_test

import (
    "context"
    "testing"
    "time"

    "github.com/stretchr/testify/require"
    inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
    blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
    "github.com/bsv-blockchain/teranode/test"
    ulogger "github.com/bsv-blockchain/teranode/util/logger"
)

func TestValidatorIsolation_ValidTx(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    // 1. Build in-memory infrastructure
    broker := inmemorykafka.NewInMemoryBroker()
    utxoStore := testutil.NewSQLiteMemoryUTXOStore(t)
    blobStore := blob_memory.NewMemoryStore()
    blockchainClient := testutil.NewMemorySQLiteBlockchainClient(t)

    // 2. Pre-populate state
    require.NoError(t, utxoStore.SetUTXO(ctx, testUTXO))

    // 3. Subscribe to outputs BEFORE starting service
    outputCh := subscribeKafkaTopic(t, broker, "txmeta")

    // 4. Start the service
    settings := test.CreateBaseTestSettings(t)
    svc := validator.NewServer(
        ulogger.TestLogger{},
        settings,
        utxoStore,
        blockchainClient,
        inmemorykafka.NewInMemoryAsyncProducer(broker, 100),  // txmeta
        inmemorykafka.NewInMemoryAsyncProducer(broker, 100),  // rejectedtx
        nil,
    )
    require.NoError(t, svc.Start(ctx))
    defer svc.Stop(ctx)

    // 5. Exercise the service
    resp, err := svc.ValidateTransaction(ctx, &validator_api.ValidateTransactionRequest{
        Transaction: testTx.Bytes(),
    })

    // 6. Assert on direct return + Kafka output
    require.NoError(t, err)
    require.True(t, resp.Valid)

    select {
    case msg := <-outputCh:
        var meta txmeta.TxMeta
        require.NoError(t, proto.Unmarshal(msg.Value, &meta))
        require.Equal(t, testTx.TxID(), meta.TxID)
    case <-ctx.Done():
        t.Fatal("timeout: no txmeta Kafka message received")
    }
}
```

---

## Service 1: Validator

**File:** `services/validator/isolation_test.go`

**Infrastructure needed:**
- `utxo.Store` → `stores/utxo/sql/` with SQLite in-memory
- `blockassembly.ClientI` → mock (verify `Store` called)
- `blockchain.ClientI` → mock (return fixed height/median time)
- Kafka: produces `txmeta`, `rejectedtx` → `InMemoryBroker`

**Kafka topics to wire:**
- Out: `txmeta` (validated tx metadata)
- Out: `rejectedtx` (rejected transaction IDs)

### Happy Path Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestValidator_ValidP2PKHTx` | Valid P2PKH tx, matching UTXO in store | `txmeta` published; `BlockAssembly.Store` called; no `rejectedtx` |
| `TestValidator_ValidBatch` | 50 valid txs | 50 `txmeta` messages; 0 `rejectedtx` |
| `TestValidator_CoinbaseTxInGenesis` | Coinbase tx at height 0 | Accepted (genesis special case) |
| `TestValidator_SkipPolicyChecks` | Consolidation tx with dust outputs | Accepted with `skipPolicyChecks=true` |
| `TestValidator_SkipUtxoCreation` | Valid tx | UTXO not written; `txmeta` still published |
| `TestValidator_LargeTx` | Tx > 1MB | Routed to HTTP endpoint instead of Kafka |
| `TestValidator_BatchFlushOnTimeout` | 3 txs; wait for timeout | Partial batch flushed after `TxMetaKafkaBatchTimeoutMs` |
| `TestValidator_TriggerBatcher` | 3 txs; call `TriggerBatcher` | Batch flushed immediately |

### Rejection Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestValidator_DoubleSpend` | Tx spending already-spent UTXO | `rejectedtx` published; error returned |
| `TestValidator_InvalidSignature` | Tx with bad ECDSA sig | `rejectedtx` published; error returned |
| `TestValidator_MissingUTXO` | Tx spending nonexistent UTXO | `rejectedtx` published; error returned |
| `TestValidator_BelowMinFee` | Tx with insufficient fee | `rejectedtx` published; error returned |
| `TestValidator_CoinbaseInMempool` | Coinbase tx submitted | Rejected; no UTXO created |
| `TestValidator_EmptyTx` | Zero-byte body | Error returned before any processing |
| `TestValidator_OutputsExceedInputs` | Outputs > inputs | Rejected; `rejectedtx` published |
| `TestValidator_DustOutput` | Output below dust threshold | Rejected (policy); `rejectedtx` published |

### State Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestValidator_RejectedTxNotPublishedDuringCatchup` | Reject during CATCHUP FSM state | No `rejectedtx` published (by design) |
| `TestValidator_UTXOStoreDown` | Store returns error | Error returned; no crash; no Kafka produced |
| `TestValidator_BlockAssemblyDown` | BlockAssembly gRPC unavailable | Validation succeeds; `Store` error logged |

---

## Service 2: Propagation

**File:** `services/propagation/isolation_test.go`

**Infrastructure needed:**
- `validator.Interface` → in-process real validator OR mock for forwarding tests
- `blockchain.ClientI` → mock
- `blob.Store` → memory store

**Kafka topics to wire:** None (propagation routes via gRPC to validator)

### Happy Path Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestPropagation_SingleTxGRPC` | Valid tx via gRPC `ProcessTransaction` | Forwarded to validator; success returned |
| `TestPropagation_SingleTxHTTP` | Valid tx via `POST /tx` | Forwarded to validator; 200 response |
| `TestPropagation_BatchGRPC` | 50 txs via `ProcessTransactionBatch` | All forwarded to validator |
| `TestPropagation_BatchHTTP` | 50 txs via `POST /txs` | All forwarded; 200 response |
| `TestPropagation_MaxBatch` | 1024 txs exactly | Accepted and forwarded |
| `TestPropagation_LargeTxFallback` | Single tx > 1MB | Forwarded via HTTP fallback endpoint |
| `TestPropagation_UDP6Listener` | Tx datagram on UDP6 port | Forwarded to validator |
| `TestPropagation_RateLimiting` | Requests above rate limit | Excess requests throttled/rejected |

### Rejection Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestPropagation_EmptyTx` | Zero-byte body | 400 error; validator not called |
| `TestPropagation_OversizeBatch` | 1025 txs | Rejected; 400 error |
| `TestPropagation_BatchExceedsMaxBytes` | Batch > 32MB total | Rejected before forwarding |
| `TestPropagation_ValidatorDown` | Validator gRPC unavailable | Error returned to caller; no crash |

### State Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestPropagation_HealthEndpoint` | `GET /health` | 200; validator connectivity reported |
| `TestPropagation_GracefulStop` | Stop during active request | In-flight request completes; new requests rejected |

---

## Service 3: SubtreeValidation

**File:** `services/subtreevalidation/isolation_test.go`

**Infrastructure needed:**
- `utxo.Store` → SQLite in-memory (for cache miss fallback)
- `blob.Store` (subtreeStore, txStore) → memory store
- `blockchain.ClientI` → mock
- `validator.Interface` → real (or mock for unit-style)
- `p2p.ClientI` → mock (verify `ReportValidSubtree`/`ReportInvalidSubtree` called)
- Kafka: consumes `subtrees`, `txmeta`; produces `invalid-subtrees` → `InMemoryBroker`

**Kafka topics to wire:**
- In: `subtrees` (new subtree announcements from P2P)
- In: `txmeta` (tx metadata for cache)
- Out: `invalid-subtrees` (failed subtrees)

### Happy Path Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestSubtreeValidation_ValidSubtree` | Publish valid subtree hash to `subtrees` Kafka | No `invalid-subtrees`; subtree stored in blob store; `ReportValidSubtree` called |
| `TestSubtreeValidation_TxMetaCachePopulated` | Publish `txmeta` Kafka messages | Cache populated; subsequent subtree validation uses cache hits |
| `TestSubtreeValidation_CacheMissUTXOFallback` | Subtree with tx not in cache | Falls through to `utxo.Store`; succeeds |
| `TestSubtreeValidation_CheckBlockSubtrees` | Call `CheckBlockSubtrees` gRPC | All subtrees validated; responses correct |
| `TestSubtreeValidation_ParallelSubtrees` | 10 concurrent subtrees | All processed without deadlock or duplicate processing |

### Rejection Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestSubtreeValidation_InvalidTxInSubtree` | Subtree with invalid tx | `invalid-subtrees` Kafka message published |
| `TestSubtreeValidation_InvalidSubtreeDeduplication` | Same bad subtree published twice within 1 min | Only 1 `invalid-subtrees` message |
| `TestSubtreeValidation_SubtreeNotFound` | Hash with no corresponding data | `invalid-subtrees` published |

### Orphanage Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestSubtreeValidation_OrphanSubtree` | Subtree whose parent tx not yet validated | Queued in orphanage |
| `TestSubtreeValidation_OrphanResolved` | Parent tx validated → orphan processed | Orphan subtree now validated |
| `TestSubtreeValidation_OrphanageMaxSize` | Orphanage at capacity | Oldest orphan evicted; new orphan rejected gracefully |
| `TestSubtreeValidation_OrphanageNoDuplicate` | Same orphan submitted twice | Second submission ignored |

### Concurrency Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestSubtreeValidation_TryLockPreventsDoubleProcess` | Same subtree hash submitted concurrently | Only one goroutine processes; other waits/returns |
| `TestSubtreeValidation_HighThroughput` | 1000 txmeta messages then 100 subtrees | All processed; no goroutine leaks |

---

## Service 4: Blockchain

**File:** `services/blockchain/isolation_test.go`

**Infrastructure needed:**
- `stores/blockchain` → SQLite in-memory (`sqlitememory` driver)
- `blob.Store` → memory store
- Kafka: produces `blocks-final` → `InMemoryBroker`

**Kafka topics to wire:**
- Out: `blocks-final` (finalized block headers for legacy peers)

### Happy Path Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestBlockchain_AddGenesisBlock` | `AddBlock` with genesis block | Height = 1; `blocks-final` Kafka published; `NotificationType_Block` notification sent |
| `TestBlockchain_ChainGrowth` | Add 10 sequential blocks | `GetBestBlockHeader` returns height 10; `GetBlockByHeight` returns each correctly |
| `TestBlockchain_GetBlockLocator` | Chain of 50 blocks | Locator hashes cover exponentially spaced heights |
| `TestBlockchain_SubscribeAndNotify` | `Subscribe`; then `AddBlock` | Subscriber receives `NotificationType_Block` |
| `TestBlockchain_MultipleSubscribers` | 5 subscribers; `AddBlock` | All 5 receive notification |
| `TestBlockchain_BlobDeletionRoundTrip` | Schedule → acquire → complete | Batch lifecycle completed; scheduling cleared |
| `TestBlockchain_FSMTransitions` | IDLE → RUN via `SendFSMEvent` | `GetFSMCurrentState` returns RUNNING |
| `TestBlockchain_SetAndGetState` | `SetState("key", "value")` | `GetState("key")` returns `"value"` |
| `TestBlockchain_SetBlockMinedSet` | `SetBlockMinedSet(hash)` | `GetBlocksMinedNotSet` no longer includes hash |

### Orphan and Fork Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestBlockchain_OrphanBlock` | `AddBlock` with unknown parent | Block stored; chain tip unchanged |
| `TestBlockchain_InvalidateBlock` | `InvalidateBlock(hash)` | Block and descendants marked invalid |
| `TestBlockchain_RevalidateBlock` | `RevalidateBlock` on previously invalid block | Block marked valid |
| `TestBlockchain_ChainReorg` | Competing chain longer than current | `GetBestBlockHeader` returns new chain tip |
| `TestBlockchain_GetChainTips` | Fork at height 5 | `GetChainTips` returns 2 tips |

### Notification Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestBlockchain_SlowSubscriberNoDeadlock` | Subscriber with full channel | Notification dropped; no deadlock; new notifications still delivered |
| `TestBlockchain_SubscriberCleanupOnCancel` | Cancel subscriber context | Subscriber removed; memory released |

---

## Service 5: BlockValidation

**File:** `services/blockvalidation/isolation_test.go`

**Infrastructure needed:**
- `blockchain.ClientI` → SQLite in-memory via local client
- `subtreevalidation.Interface` → mock (return success/failure)
- `blockassembly.ClientI` → mock
- `validator.Interface` → mock
- `blob.Store` (subtreeStore, txStore) → memory store
- `utxo.Store` → SQLite in-memory
- `p2p.ClientI` → mock (verify `RecordCatchupFailure`, `IsPeerMalicious` etc.)
- Kafka: consumes `blocks`; produces `invalid-blocks` → `InMemoryBroker`

**Kafka topics to wire:**
- In: `blocks` (block announcements from P2P)
- Out: `invalid-blocks` (invalid block notifications)

### Happy Path Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestBlockValidation_ValidBlock` | Publish valid block hash to `blocks` Kafka | `blockchain.AddBlock` called; no `invalid-blocks` |
| `TestBlockValidation_BlockFoundGRPC` | Call `BlockFound` with valid hash | Block validated and stored |
| `TestBlockValidation_ProcessBlock` | Call `ProcessBlock` directly | Returns success; blockchain updated |
| `TestBlockValidation_ValidateOnly` | Call `ValidateBlock` | Returns success; no state change in blockchain |
| `TestBlockValidation_PriorityQueue` | Main chain block + fork block | Main chain block processed first |

### Deduplication Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestBlockValidation_DuplicateBlockIgnored` | Same block hash via Kafka twice | Processed only once; second call returns immediately |
| `TestBlockValidation_DeDuplicatorTTLExpiry` | Same hash after TTL expires | Block re-processed after TTL |

### Rejection Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestBlockValidation_InvalidPoW` | Block with invalid proof-of-work | `invalid-blocks` published; `blockchain.InvalidateBlock` called |
| `TestBlockValidation_SubtreeValidationFails` | Subtree returns error | `invalid-blocks` published |
| `TestBlockValidation_PeerBannedOnInvalidBlock` | Invalid block from peer X | Peer X ban initiated via P2P client |

### ForkManager Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestBlockValidation_ForkManagerAtomicCounters` | Concurrent block processing | Counters consistent after all goroutines finish |
| `TestBlockValidation_ForkManagerTwoForks` | Fork A and Fork B processed concurrently | Both processed correctly; winner selected by chain work |

### Catchup Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestBlockValidation_CatchupPeerTimeout` | Peer fails to serve block | `RecordCatchupFailure` called; next peer tried |
| `TestBlockValidation_CatchupMaliciousPeer` | Peer serves invalid block during catchup | `RecordCatchupMalicious` called; peer banned |
| `TestBlockValidation_CatchupAllPeersExhausted` | All peers fail | Error returned; `GetCatchupStatus` returns true |

---

## Service 6: BlockAssembly

**File:** `services/blockassembly/isolation_test.go`

**Infrastructure needed:**
- `utxo.Store` → SQLite in-memory
- `blob.Store` (subtreeStore) → memory store
- `blockchain.ClientI` → local SQLite client (for notification subscription)
- `blockassembly.Store` → SQL in-memory
- Notification bus: real blockchain server's notification system

**Kafka topics to wire:** None (uses blockchain notification subscription, not Kafka)

### Happy Path Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestBlockAssembly_StoreAndGetTx` | `Store(hash, fee, size, inpoints)` | Returns true; tx appears in `GetTransactionHashes` |
| `TestBlockAssembly_StoreDuplicate` | Same hash stored twice | Second `Store` returns false |
| `TestBlockAssembly_GetMiningCandidateEmptyMempool` | `GetMiningCandidate` with 0 txs | Valid empty block template returned |
| `TestBlockAssembly_GetMiningCandidateWithTxs` | 100 txs in mempool | Template includes all txs; merkle root correct |
| `TestBlockAssembly_GetCurrentDifficulty` | Call `GetCurrentDifficulty` | Returns current nBits from blockchain |
| `TestBlockAssembly_SubmitMiningSolutionValid` | Valid `SubmitMiningSolution` | Block sent to blockchain; state reset for next block |
| `TestBlockAssembly_GetBlockAssemblyState` | Query state | Returns current state string |

### State Machine Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestBlockAssembly_ResetState` | `ResetBlockAssembly` | State transitions; pending txs cleared |
| `TestBlockAssembly_ReorgHandling` | `NotificationType_Block` with reorg flag | State transitions to StateReorging; conflicted txs cleared |
| `TestBlockAssembly_BlockPersistedHeightTracking` | `NotificationType_BlockPersisted` at height N | `lastPersistedHeight` updated to N |
| `TestBlockAssembly_CleanupSafetyWithPersister` | Cleanup at height < lastPersistedHeight | Txs NOT deleted (safety check) |

### Rejection Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestBlockAssembly_SubmitWrongPreviousHash` | Solution with mismatched prev hash | Error returned; state unchanged |
| `TestBlockAssembly_RemoveTx` | `RemoveTx(hash)` after `Store` | Tx no longer in `GetTransactionHashes` |

---

## Service 7: BlockPersister

**File:** `services/blockpersister/isolation_test.go`

**Infrastructure needed:**
- `blockchain.ClientI` → local SQLite client (notifications + `GetBlocksNotPersisted`)
- `blob.Store` (blockStore) → memory store
- `blob.Store` (subtreeStore) → memory store
- `utxo.Store` → SQLite in-memory

**Kafka topics to wire:** None (uses blockchain notifications)

### Happy Path Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestBlockPersister_PersistNewBlock` | Block in blockchain, not yet persisted | Block bytes in blobstore; `SetBlockPersistedAt` called; `NotificationType_BlockPersisted` sent |
| `TestBlockPersister_PersistIdempotent` | Block already in blobstore | No error; `SetBlockPersistedAt` still called; notification sent |
| `TestBlockPersister_NothingToPersist` | `GetBlocksNotPersisted` returns empty | Service sleeps; no crash |
| `TestBlockPersister_StartupCoordination` | Start service | `BlockPersisterHeight` state key published in blockchain before processing begins |
| `TestBlockPersister_MultipleBlocks` | 5 unpersisted blocks | All persisted in order; notifications sent for each |

### Error Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestBlockPersister_BlockchainClientDown` | `GetBlocksNotPersisted` errors | Error logged; service retries on next tick |
| `TestBlockPersister_BlobWriteFails` | Memory blob store returns error | Error logged; block remains unpersisted; retried next cycle |
| `TestBlockPersister_SetPersistedAtFails` | `SetBlockPersistedAt` errors after successful blob write | Blob idempotently re-written next cycle; eventually marked persisted |
| `TestBlockPersister_NotificationFails` | `SendNotification` errors | Warning logged; process continues |

---

## Service 8: Pruner

**File:** `services/pruner/isolation_test.go`

**Infrastructure needed:**
- `utxo.Store` → SQLite in-memory (for `PruneParents`)
- `blockchain.ClientI` → local SQLite client (notifications + blob deletion APIs)
- `blockassembly.ClientI` → mock (return StateRunning or not)
- `blob.Store` (multiple) → memory stores

**Kafka topics to wire:** None (uses blockchain notifications)

### Happy Path Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestPruner_TriggeredByBlockPersisted` | Send `NotificationType_BlockPersisted` | Pruner goroutine receives signal; `PruneParents` called |
| `TestPruner_SkipsWhenAssemblyNotReady` | BlockAssembly returns StateResetting | Pruning skipped; no `PruneParents` called |
| `TestPruner_PruneParentsExecution` | UTXO DAH records at height <= N | Records deleted after trigger |
| `TestPruner_BlobDeletionRoundTrip` | Schedule blobs; trigger pruner | Blobs deleted; batch completed with all IDs in `completedIDs` |
| `TestPruner_BlobDeletionPartialFailure` | Some blob deletes fail | `failedIDs` passed to `CompleteBlobDeletionBatch`; retry counter incremented |
| `TestPruner_BlobDeletionMaxRetries` | Same blob fails N times | Blob removal abandoned after max retries |
| `TestPruner_SignalBuffering` | 10 rapid block notifications | Only 1 prune cycle triggered (channel capacity 1) |

### Trigger Mode Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestPruner_OnBlockMinedTrigger` | Setting `pruner_block_trigger=OnBlockMined` | Triggered by `NotificationType_Block` instead |
| `TestPruner_NoDeadlock` | Rapid notifications + slow prune | Channel never blocks producer; notifications consumed eventually |

---

## Service 9: UTXOPersister

**File:** `services/utxopersister/isolation_test.go`

**Infrastructure needed:**
- `blockchain.ClientI` → local SQLite client (notifications)
- `stores/blockchain` store → SQLite in-memory (direct access)
- `blob.Store` (blockStore) → memory store

**Kafka topics to wire:** None (uses blockchain notifications)

### Happy Path Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestUTXOPersister_SkipsUnconfirmedBlock` | Block at height 50 (< 100 confirmations) | No UTXO set file written |
| `TestUTXOPersister_ExportsAt100Confirmations` | Block at height 100 | UTXO set file for height 1 written to blob store |
| `TestUTXOPersister_IdempotentExport` | Height 100 processed twice | Second run: file exists; no error; no duplicate write |
| `TestUTXOPersister_UTXOReconciliation` | Block adds 5 UTXOs, deletes 2 | Output file contains net 3 UTXOs correctly |
| `TestUTXOPersister_HeadersAndFootersValid` | Export UTXO set | `Headers` parseable; `Footers` count matches UTXO count |
| `TestUTXOPersister_LargeUTXOSet` | 100K UTXOs | File written without OOM; footer count = 100K |

### Error Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestUTXOPersister_BlockchainStoreDown` | Store returns error | Processing stalls; no crash |
| `TestUTXOPersister_BlobWriteFails` | Memory blob returns error | Error logged; retried on next tick |

---

## Service 10: P2P

**File:** `services/p2p/isolation_test.go`

**Infrastructure needed:**
- `blockchain.ClientI` → local SQLite client
- `blockassembly.ClientI` → mock
- `PeerRegistry` → real (in-memory)
- `BanManager` → real (in-memory)
- Kafka: consumes `rejectedtx`, `invalid-blocks`, `invalid-subtrees`; produces `blocks`, `subtrees` → `InMemoryBroker`

**Kafka topics to wire:**
- In: `rejectedtx`, `invalid-blocks`, `invalid-subtrees`
- Out: `blocks`, `subtrees`

### Peer Management Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestP2P_BanPeerAndCheck` | `BanPeer(addr)` then `IsBanned(addr)` | Returns true |
| `TestP2P_UnbanPeer` | `BanPeer` then `UnbanPeer` | `IsBanned` returns false |
| `TestP2P_ListBanned` | Ban 5 peers | `ListBanned` returns 5 entries |
| `TestP2P_BanScoreAccumulation` | `AddBanScore` 10 times for peer | Peer eventually auto-banned when threshold reached |
| `TestP2P_BanScoreDecay` | Add score; wait decay period | Score decreases; peer unbanned |
| `TestP2P_GetPeersForCatchup` | 5 peers with varying reputation | Returns peers sorted best-first |

### Kafka Flow Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestP2P_InvalidBlockBansPeer` | Publish to `invalid-blocks` with peerID X | Peer X banned |
| `TestP2P_InvalidSubtreeDecrementReputation` | Publish to `invalid-subtrees` | Peer reputation decremented |
| `TestP2P_RejectedTxFiltersPropagation` | Publish to `rejectedtx` | TxID added to propagation filter |
| `TestP2P_BlockReceivedPublishesToKafka` | libp2p block message received | `blocks` Kafka topic message published |
| `TestP2P_SubtreeReceivedPublishesToKafka` | libp2p subtree message received | `subtrees` Kafka topic message published |

### WebSocket Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestP2P_WebSocketBroadcastToSlowClient` | 1 fast + 1 slow client | Fast client gets all; slow client times out without deadlock |
| `TestP2P_WebSocketClientDisconnect` | Client disconnects mid-broadcast | No panic; remaining clients unaffected |

### SyncCoordinator Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestP2P_SyncCoordinatorPeerFailover` | Primary peer fails | Coordinator switches to next peer |
| `TestP2P_SyncCoordinatorAllPeersFail` | All peers fail | Coordinator returns error |
| `TestP2P_CatchupMetrics` | Record attempt + success + failure | Metrics stored and retrievable |

---

## Service 11: Asset

**File:** `services/asset/isolation_test.go`

**Infrastructure needed:**
- `utxo.Store` → SQLite in-memory
- `blob.Store` (txStore, subtreeStore, blockPersisterStore) → memory stores
- `blockchain.ClientI` → local SQLite client
- `blockvalidation.Interface` → mock
- `p2p.ClientI` → mock

**Kafka topics to wire:** None

### Happy Path Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestAsset_HealthLiveness` | `GET /health/liveness` | 200 always |
| `TestAsset_HealthReadiness` | All deps healthy | 200 |
| `TestAsset_HealthReadinessUTXOStoreDown` | UTXO store errors | 503 |
| `TestAsset_FSMWaitBlocksUntilNonIdle` | FSM in IDLE state | HTTP server starts; Centrifuge delayed |
| `TestAsset_HTTPServerStopsOnContextCancel` | Cancel context | HTTP server stops cleanly |
| `TestAsset_CentrifugeDisabledByConfig` | No `centrifugeAddr` configured | No Centrifuge instance created |
| `TestAsset_InitMissingHTTPAddress` | Empty `asset_httpListenAddress` | `ConfigurationError` returned |

---

## Service 12: RPC

**File:** `services/rpc/isolation_test.go`

**Infrastructure needed:**
- `blockchain.ClientI` → local SQLite client
- `blockassembly.ClientI` → mock
- `validator.Interface` → mock
- `p2p.ClientI` → mock
- `propagation.ClientInterface` → mock
- `blockvalidation.Interface` → mock
- `utxo.Store` → SQLite in-memory
- `blob.Store` (txStore) → memory store

**Kafka topics to wire:** None

### Auth Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestRPC_ValidCredentials` | Correct user:password | 200 |
| `TestRPC_InvalidCredentials` | Wrong password | 401 |
| `TestRPC_NoCredentials` | No auth header | 401 |
| `TestRPC_ConnectionLimit` | Connections > limit | Excess connections rejected |

### Method Tests

| Test | Method | Expected |
|---|---|---|
| `TestRPC_GetBlockCount` | `getblockcount` | Returns current chain height |
| `TestRPC_GetBlockHash` | `getblockhash` at height 1 | Returns hash |
| `TestRPC_GetBlock` | `getblock` with valid hash | Returns block JSON |
| `TestRPC_GetBlockUnknownHash` | `getblock` with unknown hash | Returns error |
| `TestRPC_GetBestBlockHash` | `getbestblockhash` | Returns current tip hash |
| `TestRPC_SendRawTransactionValid` | `sendrawtransaction` | Forwarded to propagation; txid returned |
| `TestRPC_SendRawTransactionInvalidHex` | `sendrawtransaction` with bad hex | Decode error; propagation not called |
| `TestRPC_GetMiningCandidate` | `getminingcandidate` | Returns template from BlockAssembly |
| `TestRPC_SubmitMiningSolution` | `submitminingsolution` | Forwarded to BlockAssembly |
| `TestRPC_GetPeerInfo` | `getpeerinfo` | Calls P2P `GetPeers` |
| `TestRPC_SetBan` | `setban` | Calls P2P `BanPeer` |
| `TestRPC_ListBanned` | `listbanned` | Calls P2P `ListBanned` |
| `TestRPC_GenerateOnMainnet` | `generate` with mainnet setting | Error returned |
| `TestRPC_GetChainTips` | `getchaintips` | Returns fork info |

---

## Service 13: Alert

**File:** `services/alert/isolation_test.go`

**Infrastructure needed:**
- `utxo.Store` → SQLite in-memory (for UTXO blacklisting)
- `blockchain.ClientI` → mock (for `InvalidateBlock`)
- `p2p.ClientI` → mock (for `BanPeer`)
- `legacy/peer.ClientI` → mock (for legacy peer banning)
- Alert SQLite store → in-memory

**Kafka topics to wire:** None

### Happy Path Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestAlert_ValidSignatureAccepted` | Alert signed with known genesis key | Processed and relayed |
| `TestAlert_InvalidSignatureRejected` | Alert with bad signature | Rejected; no action taken |
| `TestAlert_UnknownKeyRejected` | Alert with unknown key | Rejected |
| `TestAlert_ConsensusBlacklist` | Blacklist alert with UTXO references | UTXOs locked in store |
| `TestAlert_PeerBanAlert` | Ban peer alert | `BanPeer` called on both P2P and legacy clients |
| `TestAlert_BlockInvalidationAlert` | Block invalidation alert | `InvalidateBlock` called on blockchain client |
| `TestAlert_DuplicateAlertIdempotent` | Same alert processed twice | Applied only once |

### Error Tests

| Test | Scenario | Expected |
|---|---|---|
| `TestAlert_UTXOStoreDown` | Store errors on blacklist | Alert processed partially; error logged |
| `TestAlert_P2PClientDown` | P2P unavailable on ban | Error logged; alert still stored |

---

## Service 14: Legacy

**File:** `services/legacy/isolation_test.go`

**Infrastructure needed:**
- `blockchain.ClientI` → local SQLite client
- `validator.Interface` → mock
- `blockassembly.ClientI` → mock
- `subtreevalidation.Interface` → mock
- `blob.Store` (subtreeStore, tempStore) → memory stores
- `utxo.Store` → SQLite in-memory
- Kafka: consumes `blocks-final`, `legacy-inv`; produces `legacy-inv` → `InMemoryBroker`

**Kafka topics to wire:**
- In: `blocks-final` (finalized blocks to relay to legacy peers)
- In/Out: `legacy-inv` (inventory messages bidirectional)

### Happy Path Tests

| Test | Input | Expected Output |
|---|---|---|
| `TestLegacy_BlocksFinalRelayed` | Publish to `blocks-final` Kafka | Connected legacy peers receive `headers` message |
| `TestLegacy_LegacyInvForwarded` | Publish to `legacy-inv` Kafka | Legacy peers receive `inv` message |
| `TestLegacy_MalformedBlocksFinalIgnored` | Corrupt `blocks-final` message | Error logged; processing continues |
| `TestLegacy_NoPeersConnected` | Kafka message with 0 peers | Message consumed; no crash |
| `TestLegacy_MultistreamSupport` | Multiple concurrent streams from 1 peer | All streams processed independently |

---

## Benchmark Tests

### Goals

Benchmarks must be:
- **Repeatable**: same input, same environment → same result within 5%
- **Comparable**: tracked against `main` branch via CI (existing framework in `docs/benchmark-framework.md`)
- **Dimensional**: measure throughput, latency, and allocations
- **Isolated**: no network I/O; all in-process

### Benchmark Runner

```bash
# Run all benchmarks with profiling
./test/benchmarks/run_benchmarks.sh

# Run specific service
go test -bench=. -benchmem -benchtime=30s -count=3 ./services/validator/...

# With CPU profile
go test -bench=BenchmarkValidator_Throughput -cpuprofile=cpu.prof ./services/validator/
go tool pprof -http=:8080 cpu.prof

# With memory profile
go test -bench=. -memprofile=mem.prof ./services/validator/
go tool pprof -http=:8081 mem.prof

# With execution trace (goroutine/scheduler analysis)
go test -bench=. -trace=trace.out ./services/validator/
go tool trace trace.out
```

### Script: `test/benchmarks/run_benchmarks.sh`

```bash
#!/usr/bin/env bash
# Run all service benchmarks and write results to benchmark-results/
set -e

OUTDIR="benchmark-results/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUTDIR"

SERVICES=(
    "services/validator"
    "services/propagation"
    "services/subtreevalidation"
    "services/blockchain"
    "services/blockassembly"
    "services/blockvalidation"
    "services/pruner"
    "services/blockpersister"
)

for svc in "${SERVICES[@]}"; do
    name=$(basename "$svc")
    echo "=== Benchmarking $name ==="
    go test \
        -bench=. \
        -benchmem \
        -benchtime=10s \
        -count=3 \
        -run=^$ \
        "./$svc/..." \
        | tee "$OUTDIR/$name.txt"
done

echo "Results written to $OUTDIR"
```

### Per-Service Benchmarks

#### Validator

```go
// Existing — extend
func BenchmarkValidator_SingleTxThroughput(b *testing.B)    // tx/s; single goroutine
func BenchmarkValidator_BatchThroughput(b *testing.B)       // tx/s; batch of 100
func BenchmarkValidator_ConcurrentThroughput(b *testing.B)  // tx/s; 8 goroutines
func BenchmarkValidator_ScriptVerification(b *testing.B)    // script execution only
func BenchmarkValidator_UTXOLookup(b *testing.B)            // UTXO store hit latency
func BenchmarkValidator_KafkaProduce(b *testing.B)          // in-memory Kafka throughput
```

**Metrics to report:**
- `b.ReportMetric(float64(txCount)/elapsed.Seconds(), "tx/s")`
- `b.ReportAllocs()` — allocations per tx
- Latency histogram: p50, p99 via custom histogram in sub-benchmarks

#### Propagation

```go
func BenchmarkPropagation_GRPCIngestRate(b *testing.B)         // gRPC tx/s
func BenchmarkPropagation_HTTPIngestRate(b *testing.B)         // HTTP tx/s
func BenchmarkPropagation_BatchGRPCThroughput(b *testing.B)    // batch tx/s
func BenchmarkPropagation_UDP6ThroughputSmallTx(b *testing.B)  // UDP6 ingest (< 512 bytes)
```

#### SubtreeValidation

```go
// Existing — extend
func BenchmarkSubtreeValidation_SmallBlock(b *testing.B)        // 100 tx subtrees
func BenchmarkSubtreeValidation_MediumBlock(b *testing.B)       // 1K tx subtrees
func BenchmarkSubtreeValidation_LargeBlock(b *testing.B)        // 10K tx subtrees
func BenchmarkSubtreeValidation_CacheHitRate(b *testing.B)      // all-cache vs all-miss
func BenchmarkSubtreeValidation_ParallelProcessing(b *testing.B) // N subtrees concurrently
```

#### Blockchain

```go
func BenchmarkBlockchain_AddBlock(b *testing.B)               // AddBlock latency
func BenchmarkBlockchain_GetBestBlockHeader(b *testing.B)     // Read latency
func BenchmarkBlockchain_Notify100Subscribers(b *testing.B)   // Notification fanout
func BenchmarkBlockchain_GetBlockLocator50Blocks(b *testing.B) // Locator computation
```

#### BlockAssembly

```go
// Existing — extend
func BenchmarkBlockAssembly_StoreTransactions(b *testing.B)    // tx/s into mempool
func BenchmarkBlockAssembly_GetMiningCandidate(b *testing.B)   // template build latency
func BenchmarkBlockAssembly_SubtreeProcessor(b *testing.B)     // subtree build throughput
func BenchmarkBlockAssembly_LoadUnmined(b *testing.B)          // startup scan speed
```

#### BlockValidation

```go
func BenchmarkBlockValidation_ValidBlockThroughput(b *testing.B)  // blocks/s (small blocks)
func BenchmarkBlockValidation_LargeBlock1MTx(b *testing.B)        // 1M tx block validation time
func BenchmarkBlockValidation_ForkManagerContention(b *testing.B) // concurrent fork processing
```

#### BlockPersister

```go
func BenchmarkBlockPersister_PersistSmallBlock(b *testing.B)    // 100 tx block persist latency
func BenchmarkBlockPersister_PersistLargeBlock(b *testing.B)    // 1M tx block persist time
func BenchmarkBlockPersister_ThroughputBlocks(b *testing.B)     // blocks/s persist rate
```

#### Pruner

```go
func BenchmarkPruner_PruneParents(b *testing.B)                 // UTXO DAH prune speed
func BenchmarkPruner_BlobDeletionBatch(b *testing.B)            // blob batch acquire+complete
```

### Standard Benchmark Dimensions

Every benchmark must report:

| Metric | How |
|---|---|
| Throughput | `b.ReportMetric(ops/elapsed, "ops/s")` |
| Allocations | `-benchmem` + `b.ReportAllocs()` |
| Bytes/op | Via `-benchmem` |
| CPU time | Default from `go test -bench` |

Latency benchmarks additionally report:

```go
// p50/p99 via buckets
var latencies []time.Duration
for i := 0; i < b.N; i++ {
    start := time.Now()
    // ... operation
    latencies = append(latencies, time.Since(start))
}
sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
b.ReportMetric(float64(latencies[len(latencies)/2].Microseconds()), "p50_us")
b.ReportMetric(float64(latencies[len(latencies)*99/100].Microseconds()), "p99_us")
```

---

## Profiling

### CPU Profiling Workflow

```bash
# 1. Generate profile
go test -bench=BenchmarkValidator_SingleTxThroughput \
    -cpuprofile=cpu.prof \
    -benchtime=30s \
    ./services/validator/

# 2. Identify top functions
go tool pprof -top cpu.prof

# 3. Interactive browser UI
go tool pprof -http=:8080 cpu.prof
# → flame graph at /ui/flamegraph
# → top callees at /ui/top
```

### Memory Profiling Workflow

```bash
# 1. Generate profile
go test -bench=BenchmarkValidator_BatchThroughput \
    -memprofile=mem.prof \
    -benchtime=30s \
    ./services/validator/

# 2. Heap allocations
go tool pprof -http=:8081 mem.prof
# → look for unexpected heap allocations in hot paths
```

### Goroutine Leak Detection

Add to every `isolation_test.go` file:

```go
import "go.uber.org/goleak"

func TestMain(m *testing.M) {
    goleak.VerifyTestMain(m,
        // ignore known background goroutines if needed
        goleak.IgnoreTopFunction("..."),
    )
}
```

### Continuous Profiling in CI

The existing benchmark CI framework (`docs/benchmark-framework.md`) already:
- Runs benchmarks on PRs against `main`
- Posts comparison reports as PR comments
- Fails PRs with > 5% regression

Extend it to include all new service benchmarks by adding benchmark files following the existing naming convention (`Benchmark*`).

---

## Chaos Tests

Chaos tests verify **graceful degradation** — the service continues operating correctly (or fails cleanly) when infrastructure misbehaves.

### Existing Chaos Framework

Location: `test/chaos/`

Uses [Toxiproxy](https://github.com/Shopify/toxiproxy) for network fault injection. 8 scenarios exist. Extend with service-specific scenarios.

### New Chaos Scenarios Per Service

#### Validator Chaos

```
test/chaos/scenario_09_validator_utxo_latency_test.go
```

| Scenario | Fault | Expected Behavior |
|---|---|---|
| `TestChaos_ValidatorUTXOHighLatency` | 500ms added to UTXO store calls | Validation succeeds but slow; no timeout panic |
| `TestChaos_ValidatorUTXOIntermittentFailure` | 20% packet drop to UTXO store | Retried txs eventually succeed; rejected txs not falsely accepted |
| `TestChaos_ValidatorKafkaProducerDown` | Kill `txmeta` Kafka producer | Validation still returns success; `txmeta` buffered or dropped without crash |
| `TestChaos_ValidatorBlockAssemblyDown` | Kill BlockAssembly gRPC server | Validation returns success; `Store` error logged; no crash |

#### Propagation Chaos

```
test/chaos/scenario_10_propagation_validator_fault_test.go
```

| Scenario | Fault | Expected Behavior |
|---|---|---|
| `TestChaos_PropagationValidatorHighLatency` | 1s added to validator calls | Client gets slow response; no goroutine leak |
| `TestChaos_PropagationValidatorDisconnect` | TCP reset on validator connection | Error returned to client; reconnect on next request |
| `TestChaos_PropagationValidatorBandwidthLimit` | 10KB/s to validator | Large txs timeout gracefully |

#### SubtreeValidation Chaos

```
test/chaos/scenario_11_subtreevalidation_peer_fault_test.go
```

| Scenario | Fault | Expected Behavior |
|---|---|---|
| `TestChaos_SubtreeValidationPeerHTTPLatency` | 2s latency on peer asset server URL | Fetch times out; `invalid-subtrees` published |
| `TestChaos_SubtreeValidationPeerHTTPDown` | Peer asset server returns 503 | Subtree marked invalid; `invalid-subtrees` published |
| `TestChaos_SubtreeValidationKafkaConsumerLag` | Slow consumer (paused for 5s) | After resume, all buffered messages processed |

#### Blockchain Chaos

```
test/chaos/scenario_12_blockchain_db_fault_test.go
```

| Scenario | Fault | Expected Behavior |
|---|---|---|
| `TestChaos_BlockchainDBHighLatency` | 200ms DB latency | `AddBlock` slow; subscribers still notified eventually |
| `TestChaos_BlockchainDBConnectionReset` | DB connection reset mid-transaction | Block not partially committed; retry succeeds |
| `TestChaos_BlockchainNotificationChannelFull` | Slow subscriber | No deadlock; notifications dropped for slow subscriber only |

#### BlockValidation Chaos

```
test/chaos/scenario_13_blockvalidation_peer_fault_test.go
```

| Scenario | Fault | Expected Behavior |
|---|---|---|
| `TestChaos_BlockValidationPeerFetchLatency` | High latency on peer block fetch | Fetch times out; `invalid-blocks` published; peer penalized |
| `TestChaos_BlockValidationAllPeersDown` | All peer connections severed | Catchup stalls gracefully; no crash |
| `TestChaos_BlockValidationForkDuringChaos` | Network partition + fork | ForkManager processes both chains when partition heals |

#### P2P Chaos

```
test/chaos/scenario_14_p2p_peer_churn_test.go
```

| Scenario | Fault | Expected Behavior |
|---|---|---|
| `TestChaos_P2PPeerChurn` | 50% of peers disconnect/reconnect every second | P2P continues routing; no panic; peer registry eventually consistent |
| `TestChaos_P2PBanStormScenario` | 100 invalid blocks from different peers | Ban manager processes all; no ban list corruption |
| `TestChaos_P2PKafkaProducerHighLatency` | 500ms on `blocks` topic | Block announcements delayed; no goroutine leak |

#### Pruner Chaos

```
test/chaos/scenario_15_pruner_utxo_fault_test.go
```

| Scenario | Fault | Expected Behavior |
|---|---|---|
| `TestChaos_PrunerUTXOStoreSlow` | 1s UTXO prune latency | Pruner completes eventually; next cycle not blocked |
| `TestChaos_PrunerBlobDeleteTimeout` | Blob store times out on delete | Failed IDs reported; retry counter incremented; no crash |
| `TestChaos_PrunerBlockAssemblyFlapping` | BlockAssembly alternates ready/not-ready | Pruner only executes when ready; no partial prune |

### Chaos Test Infrastructure

#### Fault Injection Patterns

```go
// Pattern 1: Toxiproxy for network faults (external deps)
proxy := toxiproxy.NewProxy("aerospike", "localhost:3000", "aerospike:3000")
proxy.AddToxic("latency", "latency", "downstream", 1.0, toxiproxy.Attributes{
    "latency": 500,
    "jitter":  50,
})
defer proxy.Delete()

// Pattern 2: Wrapper store with injected faults (in-process)
type FaultyUTXOStore struct {
    utxo.Store
    failRate float64
    latency  time.Duration
}
func (f *FaultyUTXOStore) GetUTXO(ctx context.Context, key []byte) (*utxo.UTXO, error) {
    if rand.Float64() < f.failRate {
        return nil, errors.New("injected fault")
    }
    time.Sleep(f.latency)
    return f.Store.GetUTXO(ctx, key)
}

// Pattern 3: Kafka consumer pause (simulate lag)
consumerGroup.PauseAll()
time.Sleep(5 * time.Second)
consumerGroup.ResumeAll()

// Pattern 4: Context cancellation (simulate timeout)
ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
defer cancel()
```

#### Chaos Test Assertions

Each chaos test must assert:
1. **No panic** — service does not crash
2. **No goroutine leak** — `goleak.VerifyNone(t)` after cleanup
3. **No data corruption** — state is consistent after fault heals
4. **Correct error signaling** — errors returned to callers, not silently swallowed
5. **Recovery** — service returns to normal operation after fault removed

---

## Prioritized Implementation Order

| Priority | Service | Reasoning |
|---|---|---|
| 1 | **Validator** | Highest tx throughput, most complex validation rules, already has benchmark stubs |
| 2 | **Propagation** | Entry point for all transactions; tests the full ingest path |
| 3 | **SubtreeValidation** | Complex Kafka flow + orphanage; partial test coverage exists |
| 4 | **Blockchain** | State management; SQLite in-memory client exists; other services depend on it |
| 5 | **BlockValidation** | Complex fork/catchup logic; most test files already exist; needs integration harness |
| 6 | **BlockAssembly** | Mining critical path; reorg tests exist; needs Kafka-free isolation harness |
| 7 | **Pruner** | Data integrity; minimal tests currently |
| 8 | **BlockPersister** | Minimal tests; critical for persistence correctness |
| 9 | **P2P** | Peer management; good unit test coverage; isolation harness needs libp2p abstraction |
| 10 | **RPC** | External interface; good unit coverage; isolation adds auth + routing tests |
| 11 | **Asset** | HTTP layer; straightforward |
| 12 | **UTXOPersister** | Data export; low risk; good data structure tests exist |
| 13 | **Legacy** | Complex wire protocol; lower priority than core validation path |
| 14 | **Alert** | Infrequent; low throughput; basic isolation tests sufficient |

---

## Running the Tests

```bash
# Run all service isolation tests (no external deps required)
go test -tags integration -v ./services/... -run TestIsolation

# Run isolation tests for a single service
go test -tags integration -v -run TestIsolation ./services/validator/...

# Run all benchmarks
go test -bench=. -benchmem -benchtime=10s -count=3 -run=^$ ./services/...

# Run chaos tests (requires Toxiproxy)
go test -tags chaos -v ./test/chaos/...

# Full isolation + benchmark run
./test/benchmarks/run_benchmarks.sh

# Check for goroutine leaks
go test -tags integration -v ./services/validator/... -race
```

---

## Acceptance Criteria

### Isolation Tests
- [ ] Each service has `isolation_test.go` with happy path + rejection + state machine tests
- [ ] All tests run with `go test -tags integration` — no Docker, no external services
- [ ] `goleak.VerifyTestMain` in each package's `TestMain`
- [ ] Tests complete in < 30 seconds per service

### Benchmarks
- [ ] Each service has at least throughput + latency + allocation benchmarks
- [ ] Benchmarks reproducible: 3 runs within 5% of each other
- [ ] Integrated into CI via existing benchmark framework
- [ ] CPU and memory profiles generated in CI artifacts

### Chaos Tests
- [ ] Each service has at least 3 chaos scenarios
- [ ] Each chaos test asserts: no panic, no goroutine leak, no data corruption, recovery
- [ ] Chaos tests tagged `chaos` and run separately from normal CI
- [ ] Toxiproxy container started/stopped within each test

### Coverage Targets

| Service | Current Coverage | Target (Isolation) |
|---|---|---|
| Validator | ~60% | 80% |
| Propagation | ~50% | 75% |
| SubtreeValidation | ~55% | 80% |
| Blockchain | ~45% | 75% |
| BlockValidation | ~65% | 80% |
| BlockAssembly | ~55% | 75% |
| BlockPersister | ~20% | 65% |
| Pruner | ~15% | 65% |
| UTXOPersister | ~25% | 65% |
| P2P | ~60% | 75% |
| RPC | ~40% | 70% |
| Asset | ~20% | 65% |
| Legacy | ~30% | 60% |
| Alert | ~25% | 60% |

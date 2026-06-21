# TeraSlab UTXO Store

This package implements the `utxo.Store` interface using TeraSlab as the storage backend, communicating with a TeraSlab server via the Go client library at `github.com/icellan/teraslab/client/go`.

## Architecture

```
Teranode services
       |
  utxo.Store interface
       |
  stores/utxo/teraslab/    <-- this package
       |
  teraslab Go client       (github.com/icellan/teraslab/client/go)
       |
  TeraSlab wire protocol   (binary TCP, pipelined)
       |
  TeraSlab server          (Rust, direct NVMe I/O)
```

This package is a thin translation layer. It converts between Teranode types (`bt.Tx`, `chainhash.Hash`, `meta.Data`, `spend.SpendingData`) and TeraSlab client types (`TxID`, `TxRecord`, `SpendItem`). All wire protocol details are handled by the Go client library.

## Connection URL

The factory registers the `teraslab` scheme. The URL format:

```
teraslab://host:port
teraslab://host:port?pool_size=32
teraslab://host:port?pool_size=16&cluster=host2:port2,host3:port3
```

Parameters:
- `pool_size` — max TCP connections per node (default: 16)
- `cluster` — comma-separated additional seed nodes for cluster mode
- `cluster_secret` — shared secret to HMAC-sign inter-node bootstrap
  (`OP_GET_PARTITION_MAP`) when the cluster runs with a secret

### Cluster mode

All batch operations fan out across shards (split by txid, parallel sub-batches,
results merged in original order), and the pruner/iterator queries
(`QueryOldUnminedTransactions`, `GetConflictingTxIterator`) fan out to every node
and return the deduplicated union. The client follows shard-table-versioned
redirects with loop detection and retries transient cluster errors
(migration-in-progress, stale-epoch, no-quorum) with bounded backoff.

Note: iterators still load all matching txids into memory upfront — TeraSlab has
no streaming iteration — so very large mempools can be memory-heavy.

## File Layout

| File | Purpose |
|------|---------|
| `teraslab.go` | `Store` struct, `New()` constructor, `Health`, block height/median time |
| `convert.go` | Type conversion + field mask mapping between Teranode and TeraSlab types |
| `errors.go` | Maps TeraSlab error codes to Teranode error types |
| `batcher.go` | Batch item types and flush handlers for store/get/spend/locked batchers |
| `create.go` | `Create`, `Delete`, duplicate-after-spend detection |
| `get.go` | `Get`, `GetMeta`, `GetSpend` |
| `spend.go` | `Spend`, `Unspend` |
| `mining.go` | `SetMinedMulti`, `MarkTransactionsOnLongestChain` |
| `alert.go` | `FreezeUTXOs`, `UnFreezeUTXOs`, `ReAssignUTXO` |
| `conflicting.go` | `SetConflicting`, `GetCounterConflicting`, `GetConflictingChildren` |
| `locked.go` | `SetLocked` |
| `batch_decorate.go` | `BatchDecorate`, `PreviousOutputsDecorate` |
| `pruner.go` | `QueryOldUnminedTransactions`, `PreserveTransactions`, `ProcessExpiredPreservations` |
| `iterator.go` | `GetUnminedTxIterator`, `GetPrunableUnminedTxIterator` |

## Batching

The store uses `go-batcher` to aggregate individual operations into batch wire requests. Each batcher collects items from concurrent goroutines and flushes them as a single client call when the batch size or duration threshold is reached.

| Batcher | Collects | Flushes via | Settings |
|---------|----------|-------------|----------|
| `storeBatcher` | `Create` calls | `client.CreateBatch()` | `StoreBatcherSize`, `StoreBatcherDurationMillis` |
| `getBatcher` | `Get` / `GetMeta` calls | `client.GetRecordBatch()` | `GetBatcherSize`, `GetBatcherDurationMillis` |
| `spendBatcher` | (reserved) | `client.SpendBatch()` | `SpendBatcherSize`, `SpendBatcherDurationMillis` |
| `lockedBatcher` | (reserved) | `client.SetLockedBatch()` | `LockedBatcherSize`, `LockedBatcherDurationMillis` |

Each batch item includes a `done` channel. The caller blocks on this channel until the batch containing their item is flushed and the result is known.

The `Spend` method calls `client.SpendBatch()` directly (not through the batcher) because a single `Spend` call already produces a full batch of items from the transaction's inputs.

### Field-selective Get batching

The `getBatcher` carries a per-item `fieldMask` indicating which TeraSlab fields the caller needs. When the batch is flushed, the union of all field masks is used in the single `GetRecordBatch` call, so every item receives at least the fields it requested.

## Field Mask Mapping

TeraSlab uses a per-field bitmask to control which metadata fields the server returns. This package maps Teranode's `fields.FieldName` values to the appropriate TeraSlab bitmask:

| Teranode Field | TeraSlab Bits |
|----------------|---------------|
| `fields.Tx` | `FieldColdData \| FieldTxVersion \| FieldLocktime` |
| `fields.Inputs` | `FieldColdData \| FieldTxVersion \| FieldLocktime` |
| `fields.Outputs` | `FieldColdData \| FieldTxVersion \| FieldLocktime` |
| `fields.Fee` | `FieldFee` |
| `fields.SizeInBytes` | `FieldSizeInBytes` |
| `fields.IsCoinbase` | `FieldFlags` |
| `fields.Conflicting` | `FieldFlags` |
| `fields.Locked` | `FieldFlags` |
| `fields.Utxos` | `FieldUtxoSlots` |
| `fields.BlockIDs` | `FieldBlockEntries` |
| `fields.UnminedSince` | `FieldUnminedSince` |
| `fields.ConflictingChildren` | `FieldConflictingChildren` |
| `fields.TxInpoints` | `FieldColdData` |

Default behaviour:
- `Get(ctx, hash)` with no fields uses `utxo.MetaFieldsWithTx` (metadata + transaction data)
- `GetMeta(ctx, hash, data)` uses `utxo.MetaFields` (metadata only, no tx body)
- `BatchDecorate(ctx, items)` with no fields uses `utxo.MetaFieldsWithTx`

## Transaction Data Storage

Transaction inputs, outputs, and inpoints are stored as the server's "cold data". This package serializes and deserializes them using a length-prefixed format.

### Serialization (Create path)

Each input is serialized in extended format:
- Standard Bitcoin input bytes (`input.Bytes(false)`)
- `PreviousTxSatoshis` (8 bytes LE)
- `PreviousTxScript` (varint-prefixed)

Each output is serialized as `output.Bytes()`.

Both are stored as length-prefixed entries within their blob:
```
[count:4 LE][per-item: [len:4 LE][serialized bytes]] ...
```

Inpoints use the existing `subtree.TxInpoints.Serialize()` format.

The three blobs are sent to the server as the cold data section:
```
[inputs_blob_len:4][inputs_blob][outputs_blob_len:4][outputs_blob][inpoints_blob_len:4][inpoints_blob]
```

### Deserialization (Get path)

The Go client returns a `TxRecord` with a decoded `TxData` containing the three blobs. This package reconstructs a `bt.Tx` from:
- `TxVersion` and `Locktime` from metadata
- Inputs deserialized with `input.ReadFromExtended()`
- Outputs deserialized with `output.ReadFrom()`
- Inpoints deserialized with `subtree.NewTxInpointsFromBytes()`

## Error Mapping

TeraSlab error codes are mapped to Teranode error types in `errors.go`:

| TeraSlab Code | Teranode Error |
|---------------|----------------|
| `ErrCodeTxNotFound` | `errors.ErrTxNotFound` |
| `ErrCodeAlreadySpent` | `errors.ErrSpent` |
| `ErrCodeAlreadyExists` | `errors.ErrTxExists` |
| `ErrCodeFrozen` | `errors.ErrFrozen` |
| `ErrCodeConflicting` | `errors.ErrTxConflicting` |
| `ErrCodeLocked` | `errors.ErrTxLocked` |
| `ErrCodeCoinbaseImmature` | `errors.ErrNonFinal` |

### Duplicate-after-spend detection

TeraSlab returns `ErrCodeAlreadyExists` for all duplicate creates. The Teranode shared tests expect `ErrSpent` when re-creating a transaction whose UTXOs have been spent. The `Create` method handles this by checking UTXO slot states when it receives `ErrTxExists`:

```
Create(tx) → server returns AlreadyExists
           → client.GetRecordBatch(FieldUtxoSlots)
           → any slot has SlotSpent? → return ErrSpent
           → otherwise → return ErrTxExists
```

## GetSpend

`GetSpend` uses `GetRecordBatch` with `FieldUtxoSlots` instead of the wire-level `GetSpendBatch`. This is because `GetSpendBatch` does not return UTXO hashes, making it impossible to validate that the requested `UTXOHash` matches the stored hash (needed after reassignment when the hash changes).

The slot status is interpreted as:

| Slot Status | Condition | Teranode Status |
|-------------|-----------|-----------------|
| `SlotUnspent` | `spending_data[0:4] == 0` | `Status_OK` |
| `SlotUnspent` | `spending_data[0:4] > 0` | `Status_IMMATURE` (reassigned, not yet spendable) |
| `SlotSpent` | — | `Status_SPENT` |
| `SlotFrozen` | — | `Status_FROZEN` |

For frozen slots, `SpendingData` is constructed with `subtree.FrozenBytesTxHash` and the requested `Vout`.

## Spend

The `Spend` method builds `SpendItem` entries from the transaction's inputs. For each input:
1. Extract `PreviousTxIDChainHash` and `PreviousTxOutIndex`
2. Compute `UTXOHash` from the previous output data
3. Build `SpendingData` (spending txid + input index)
4. Send as a single `SpendBatch` call

`CurrentBlockHeight` in the spend params is set to the `blockHeight` parameter passed to `Spend()`, not `store.GetBlockHeight()`. This matches how Teranode passes the spend height and is needed for the reassignment spendable-after check.

On partial errors, the method:
- Maps each `BatchItemError` to the corresponding `utxo.Spend.Err`
- Extracts the conflicting txid from `AlreadySpent` error data (36-byte spending data)
- Returns `errors.ErrUtxoError` if any spend failed

## Conflicting Children

When creating a transaction with `WithConflicting(true)`, the store populates `ParentTxIDs` on the `CreateItem` from the transaction's inputs. The TeraSlab server uses these to update each parent record's conflicting children list.

The server also handles parent updates during `SetConflicting` by parsing the transaction's cold data to extract parent txids. This means the Go store does not need to manage parent-child relationships — it is handled server-side.

`GetConflictingChildren` and `GetCounterConflicting` read the `ConflictingChildren` field from the server via `Get` with `fields.ConflictingChildren`.

## Unmined Transaction Iterator

TeraSlab does not support streaming iteration. The iterator implementation:
1. Calls `QueryOldUnmined` to get all txids matching the cutoff
2. Fetches metadata in pages of 1000 via `GetRecordBatch`
3. Returns `UnminedTransaction` structs with `UnminedSince`, `CreatedAt`, `Locked`, and `BlockIDs`

## Testing

Tests run against a TeraSlab testcontainer:

```bash
# Run tests (pulls the published image automatically)
go test -v -timeout 600s ./stores/utxo/teraslab/...
```

The container helper:
- Uses image `ghcr.io/icellan/teraslab:latest` (override with `TERASLAB_IMAGE` env var)
- Exposes ports 3300 (wire) and 9100 (HTTP health)
- Waits for `/health/live` on port 9100
- Skips tests if the container fails to start (Docker not available, image not built)

### Test coverage

| Test File | Tests |
|-----------|-------|
| `teraslab_test.go` | Shared test suite: Store, Spend, Restore, Freeze, ReAssign, SetMined, Conflicting, Sanity |
| `create_test.go` | Create, duplicate, coinbase, conflicting, mined block info, delete, create-after-spend |
| `get_test.go` | Get metadata, fee, size, TxID, inputs, outputs, UTXO slots, spend statuses, hash validation |
| `spend_test.go` | Spend+GetSpend, double-spend with conflicting txid, unspend, spend-conflicting-fails |
| `alert_system_test.go` | Freeze/unfreeze, frozen status, SetLocked |
| `longest_chain_test.go` | MarkTransactionsOnLongestChain, SetMinedMulti, UnminedSince tracking |
| `batch_decorate_test.go` | BatchDecorate multiple txs, missing txs, empty slice |
| `pruner_test.go` | QueryOldUnmined, PreserveTransactions, ProcessExpiredPreservations |
| `lock_record_test.go` | BlockHeight, MedianBlockTime, BlockState, Health, interface compliance |

## Factory Registration

`stores/utxo/factory/teraslab.go` registers the store via `init()`:

```go
func init() {
    availableDatabases["teraslab"] = func(ctx, logger, settings, url) (utxo.Store, error) {
        return teraslab.New(ctx, logger, settings, url)
    }
}
```

## Settings

The store reads batch sizes and durations from the shared `settings.UtxoStore.*` fields. No TeraSlab-specific settings are needed — the connection parameters come from the URL.

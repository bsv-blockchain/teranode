# UTXO Store Reference Documentation

## Overview

The UTXO (Unspent Transaction Output) Store provides an interface for managing and querying UTXO data in a blockchain system.

## Core Types

### BlockState

An atomic snapshot of both block height and median block time, preventing race conditions when reading these values separately.

```go
type BlockState struct {
    Height     uint32 // Current block height
    MedianTime uint32 // Median time of recent blocks
}
```

### Spend

Represents a UTXO being spent.

```go
type Spend struct {
    // TxID is the transaction ID that created this UTXO
    TxID *chainhash.Hash `json:"txId"`

    // Vout is the output index in the creating transaction
    Vout uint32 `json:"vout"`

    // UTXOHash is the unique identifier of this UTXO
    UTXOHash *chainhash.Hash `json:"utxoHash"`

    // SpendingData contains information about the transaction that spends this UTXO
    // This will be nil if the UTXO is unspent
    SpendingData *spend.SpendingData `json:"spendingData,omitempty"`

    // ConflictingTxID is the transaction ID that conflicts with this UTXO
    ConflictingTxID *chainhash.Hash `json:"conflictingTxId,omitempty"`

    // BlockIDs is the list of blocks the transaction has been mined into
    BlockIDs []uint32 `json:"blockIDs,omitempty"`

    // error is the error that occurred during the spend operation
    Err error `json:"err,omitempty"`
}
```

The `Spend` struct also provides a `Clone()` method that creates a deep copy of the spend object.

### SpendResponse

Represents the response from a GetSpend operation.

```go
type SpendResponse struct {
    // Status indicates the current state of the UTXO
    Status int `json:"status"`

    // SpendingData contains information about the transaction that spent this UTXO, if any
    SpendingData *spend.SpendingData `json:"spendingData,omitempty"`

    // LockTime is the block height or timestamp until which this UTXO is locked
    LockTime uint32 `json:"lockTime,omitempty"`
}
```

`SpendResponse` provides serialization methods:

- `Bytes()`: Serializes the response to a byte slice
- `FromBytes(b []byte)`: Deserializes from a byte slice

### MinedBlockInfo

Contains information about a block where a transaction appears.

```go
type MinedBlockInfo struct {
    // BlockID is the unique identifier of the block
    BlockID     uint32

    // BlockHeight is the height of the block in the blockchain
    BlockHeight uint32

    // SubtreeIdx is the index of the subtree where the transaction appears
    SubtreeIdx  int

    // OnLongestChain indicates if this block is on the longest chain
    OnLongestChain bool

    // UnsetMined if true, the mined info will be removed from the tx
    UnsetMined  bool
}
```

### UnresolvedMetaData

Holds metadata for unresolved transactions.

```go
type UnresolvedMetaData struct {
    // Hash is the transaction hash
    Hash chainhash.Hash
    // Idx is the index in the original list of hashes passed to BatchDecorate
    Idx int
    // Data holds the fetched metadata, nil until fetched
    Data *meta.Data
    // Fields specifies which metadata fields should be fetched
    Fields []fields.FieldName
    // Err holds any error encountered while fetching the metadata
    Err error
}
```

### UnminedTransaction

Represents an unmined transaction in the UTXO store.

```go
type UnminedTransaction struct {
    // Hash is the transaction hash
    Hash       *chainhash.Hash
    // Fee is the transaction fee in satoshis
    Fee        uint64
    // Size is the serialized size of the transaction in bytes
    Size       uint64
    // TxInpoints contains the transaction inpoints
    TxInpoints subtree.TxInpoints
    // CreatedAt is the timestamp when the unmined transaction was first added
    CreatedAt  int
    // Locked indicates whether the transaction outputs are marked as locked
    Locked     bool
    // Skip indicates whether this transaction should be skipped during iteration
    Skip       bool
    // UnminedSince is the block height since when this transaction has been unmined
    UnminedSince int
    // BlockIDs is the list of blocks the transaction has been mined into
    BlockIDs   []uint32
}
```

### UnminedTxIterator

Provides an interface to iterate over unmined transactions efficiently.

```go
type UnminedTxIterator interface {
    // Next advances the iterator and returns the next unmined transaction, or nil if iteration is done
    Next(ctx context.Context) (*UnminedTransaction, error)
    // Err returns the first error encountered during iteration
    Err() error
    // Close releases any resources held by the iterator
    Close() error
}
```

### IgnoreFlags

Options for ignoring certain flags during UTXO operations.

```go
type IgnoreFlags struct {
    IgnoreConflicting bool
    IgnoreLocked bool
}
```

### CreateOptions

Options for creating a new UTXO entry.

```go
type CreateOptions struct {
    MinedBlockInfos []MinedBlockInfo
    TxID            *chainhash.Hash
    IsCoinbase      *bool
    Frozen          bool
    Conflicting     bool
    Locked          bool
}
```

## Store Interface

The `Store` interface defines the contract for UTXO storage operations. Implementations must be thread-safe as they will be accessed concurrently.

```go
type Store interface {
    // Health checks the health status of the UTXO store.
    // If checkLiveness is true, it performs additional liveness checks.
    // Returns status code, status message and any error encountered.
    Health(ctx context.Context, checkLiveness bool) (int, string, error)

    // Create stores a new transaction's outputs as UTXOs and returns associated metadata.
    // The blockHeight parameter is used to determine coinbase maturity.
    // Additional options can be specified using CreateOption functions.
    Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, error)

    // Get retrieves UTXO metadata for a given transaction hash.
    // The fields parameter can be used to specify which metadata fields to retrieve.
    // If fields is empty, all fields will be retrieved.
    Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error)

    // Delete removes a UTXO and its associated metadata from the store.
    Delete(ctx context.Context, hash *chainhash.Hash) error

    // GetSpend retrieves information about a UTXO's spend status.
    GetSpend(ctx context.Context, spend *Spend) (*SpendResponse, error)

    // GetMeta retrieves transaction metadata into the provided data object.
    GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error

    // Spend marks all the UTXOs of the transaction as spent.
    Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...IgnoreFlags) ([]*Spend, error)

    // Unspend reverses a previous spend operation, marking UTXOs as unspent.
    // This is used during blockchain reorganizations.
    Unspend(ctx context.Context, spends []*Spend, flagAsLocked ...bool) error

    // SetMinedMulti updates the block ID for multiple transactions that have been mined.
    // Returns a map of transaction hashes to block IDs where they were already mined,
    // enabling detection of duplicate transaction mining across different blocks.
    SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo MinedBlockInfo) (map[chainhash.Hash][]uint32, error)

    // BatchDecorate efficiently fetches metadata for multiple transactions.
    // The fields parameter specifies which metadata fields to retrieve.
    BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*UnresolvedMetaData, fields ...fields.FieldName) error

    // PreviousOutputsDecorate fetches information about transaction inputs' previous outputs.
    PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error

    // FreezeUTXOs marks UTXOs as frozen, preventing them from being spent.
    // This is used by the alert system to prevent spending of UTXOs.
    FreezeUTXOs(ctx context.Context, spends []*Spend, tSettings *settings.Settings) error

    // UnFreezeUTXOs removes the frozen status from UTXOs, allowing them to be spent again.
    UnFreezeUTXOs(ctx context.Context, spends []*Spend, tSettings *settings.Settings) error

    // ReAssignUTXO reassigns a UTXO to a new transaction output.
    // The UTXO will become spendable after ReAssignedUtxoSpendableAfterBlocks blocks.
    ReAssignUTXO(ctx context.Context, utxo *Spend, newUtxo *Spend, tSettings *settings.Settings) error

    // GetCounterConflicting returns the counter conflicting transactions for a given transaction hash.
    GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error)

    // GetConflictingChildren returns the children of the given conflicting transaction.
    GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error)

    // SetConflicting marks transactions as conflicting or not conflicting and returns the affected spends.
    SetConflicting(ctx context.Context, txHashes []chainhash.Hash, value bool) ([]*Spend, []chainhash.Hash, error)

    // SetLocked marks transactions as locked and not spendable.
    SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error

    // MarkTransactionsOnLongestChain marks transactions as being on the longest chain or not.
    // When onLongestChain is true, the unminedSince field is unset (transaction is mined).
    // When onLongestChain is false, the unminedSince field is set to the current block height.
    MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error

    // SetBlockHeight updates the current block height in the store.
    SetBlockHeight(height uint32) error

    // GetBlockHeight returns the current block height from the store.
    GetBlockHeight() uint32

    // SetMedianBlockTime updates the median block time in the store.
    SetMedianBlockTime(height uint32) error

    // GetMedianBlockTime returns the current median block time from the store.
    GetMedianBlockTime() uint32

    // GetBlockState returns an atomic snapshot of both block height and median block time.
    // This prevents race conditions that could occur when reading these values separately,
    // ensuring consistency during validation operations.
    GetBlockState() BlockState

    // GetUnminedTxIterator returns an iterator for all unmined transactions in the store.
    // This is used by the Block Assembly service to recover transactions on startup.
    // The fullScan parameter determines whether to perform a full scan of all transactions.
    GetUnminedTxIterator(fullScan bool) (UnminedTxIterator, error)

    // GetPrunableUnminedTxIterator returns a lightweight iterator optimized for the pruner's needs.
    // Unlike GetUnminedTxIterator, this iterator:
    // - Filters server-side for only unmined transactions with unminedSince <= cutoffBlockHeight
    // - Fetches only the bins needed by the pruner (txID, unminedSince, external, inputs)
    // This reduces bandwidth by 90-99%+ compared to the full iterator when the mempool is large.
    GetPrunableUnminedTxIterator(cutoffBlockHeight uint32) (UnminedTxIterator, error)

    // QueryOldUnminedTransactions returns transaction hashes for unmined transactions older than the cutoff height.
    // This method is used by the store-agnostic cleanup implementation to identify transactions for removal.
    QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error)

    // PreserveTransactions marks transactions to be preserved from deletion until a specific block height.
    // This clears any existing DeleteAtHeight and sets PreserveUntil to the specified height.
    // Used to protect parent transactions when cleaning up unmined transactions.
    PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error

    // ProcessExpiredPreservations handles transactions whose preservation period has expired.
    ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error

    // Note: Close method is not part of the Store interface in the current implementation
}
```

## Key Functions

- `Health`: Checks the health status of the UTXO store, optionally verifying liveness.
- `Create`: Creates new UTXO entries from a transaction's outputs with configurable options.
- `Get`: Retrieves UTXO metadata for specific fields with field-level filtering.
- `Delete`: Removes a UTXO entry and its associated metadata.
- `GetSpend`: Retrieves information about a UTXO's spend status, including spending transaction data.
- `Spend`: Marks UTXOs as spent by a transaction, with optional flags for handling conflicts.
- `Unspend`: Reverses spend operations during blockchain reorganization.
- `BatchDecorate`: Efficiently fetches metadata for multiple transactions in a single operation.
- `FreezeUTXOs`/`UnFreezeUTXOs`: Manages frozen status of UTXOs for the alert system.
- `SetConflicting`/`SetLocked`: Controls transaction conflict and spendability status.
- `MarkTransactionsOnLongestChain`: Marks transactions as being on the longest chain or not, managing the unminedSince field.
- `GetMeta`: Retrieves transaction metadata for a single transaction.
- `SetMinedMulti`: Updates block information for multiple mined transactions and returns a map of transaction hashes to block IDs.
- `PreviousOutputsDecorate`: Fetches information about transaction inputs' previous outputs from a transaction.
- `ReAssignUTXO`: Reassigns a UTXO to a new transaction output with safety measures.
- `GetCounterConflicting`/`GetConflictingChildren`: Manages conflict relationships between transactions.
- `SetBlockHeight`/`GetBlockHeight`/`SetMedianBlockTime`/`GetMedianBlockTime`: Manages blockchain state.
- `GetBlockState`: Returns an atomic snapshot of block height and median block time.
- `GetPrunableUnminedTxIterator`: Lightweight iterator optimized for pruner, reduces bandwidth by 90-99%+.
- `GetUnminedTxIterator`: Returns an iterator for efficiently accessing all unmined transactions.
- `QueryOldUnminedTransactions`: Identifies unmined transactions older than a specified block height for cleanup.
- `PreserveTransactions`: Protects transactions from deletion by setting a preservation period.
- `ProcessExpiredPreservations`: Handles cleanup of expired preservation markers.

## Create Options

- `WithMinedBlockInfo`: Sets the block information (ID, height, and subtree index) for a new UTXO entry. This replaces the deprecated `WithBlockIDs` option and provides more detailed tracking of where UTXOs appear in the blockchain.
- `WithTXID`: Sets the transaction ID for a new UTXO entry.
- `WithSetCoinbase`: Sets the coinbase flag for a new UTXO entry.
- `WithFrozen`: Sets the frozen status for a new UTXO entry.
- `WithConflicting`: Sets the conflicting status for a new UTXO entry.
- `WithLocked`: Sets the transaction as locked on creation.

## Constants

- `MetaFields`: Default fields for metadata retrieval.
- `MetaFieldsWithTx`: Metadata fields including the transaction.

## Mock Implementation

The `MockUtxostore` struct provides a mock implementation of the `Store` interface for testing purposes.

## Related Documents

- [UTXO Store Topic Guide](../../topics/stores/utxo.md)
- [UTXO Store Settings](../settings/stores/utxo_settings.md)

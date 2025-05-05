# UTXO Store Reference Documentation

## Overview

The UTXO (Unspent Transaction Output) Store provides an interface for managing and querying UTXO data in a blockchain system.

## Core Types

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

    // SpendingTxID is the transaction ID that spends this UTXO
    // This will be nil if the UTXO is unspent
    SpendingTxID *chainhash.Hash `json:"spendingTxId,omitempty"`

    // ConflictingTxID is the transaction ID that conflicts with this UTXO
    ConflictingTxID *chainhash.Hash `json:"conflictingTxId,omitempty"`

    // BlockIDs is the list of blocks the transaction has been mined into
    BlockIDs []uint32 `json:"blockIDs,omitempty"`

    // error is the error that occurred during the spend operation
    Err error `json:"err,omitempty"`
}
```

### SpendResponse

Represents the response from a GetSpend operation.

```go
type SpendResponse struct {
    // Status indicates the current state of the UTXO
    Status int `json:"status"`

    // SpendingTxID is the ID of the transaction that spent this UTXO, if any
    SpendingTxID *chainhash.Hash `json:"spendingTxId,omitempty"`

    // LockTime is the block height or timestamp until which this UTXO is locked
    LockTime uint32 `json:"lockTime,omitempty"`
}
```

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

### IgnoreFlags

Options for ignoring certain flags during UTXO operations.

```go
type IgnoreFlags struct {
    IgnoreConflicting bool
    IgnoreUnspendable bool
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
    Unspendable     bool
}
```

## Store Interface

The `Store` interface defines the contract for UTXO storage operations.

```go
type Store interface {
    // Health checks the health status of the UTXO store
    Health(ctx context.Context, checkLiveness bool) (int, string, error)

    // Create stores a new transaction's outputs as UTXOs and returns metadata
    Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, error)

    // Get retrieves UTXO metadata for a transaction hash
    Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error)

    // Delete removes a UTXO and its metadata from the store
    Delete(ctx context.Context, hash *chainhash.Hash) error

    // GetSpend retrieves information about a UTXO's spend status
    GetSpend(ctx context.Context, spend *Spend) (*SpendResponse, error)

    // GetMeta retrieves transaction metadata
    GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error)

    // Spend marks UTXOs as spent by a transaction
    Spend(ctx context.Context, tx *bt.Tx, ignoreFlags ...IgnoreFlags) ([]*Spend, error)

    // Unspend reverses a spend operation during blockchain reorganization
    Unspend(ctx context.Context, spends []*Spend, flagAsUnspendable ...bool) error

    // SetMinedMulti updates block information for mined transactions
    SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo MinedBlockInfo) error

    // BatchDecorate efficiently fetches metadata for multiple transactions
    BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*UnresolvedMetaData, fields ...fields.FieldName) error

    // PreviousOutputsDecorate fetches information about transaction inputs
    PreviousOutputsDecorate(ctx context.Context, outpoints []*meta.PreviousOutput) error

    // FreezeUTXOs marks UTXOs as frozen by the alert system
    FreezeUTXOs(ctx context.Context, spends []*Spend, tSettings *settings.Settings) error

    // UnFreezeUTXOs removes the frozen status from UTXOs
    UnFreezeUTXOs(ctx context.Context, spends []*Spend, tSettings *settings.Settings) error

    // ReAssignUTXO reassigns a UTXO to a new transaction output
    ReAssignUTXO(ctx context.Context, utxo *Spend, newUtxo *Spend, tSettings *settings.Settings) error

    // GetCounterConflicting returns counter conflicting transactions
    GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error)

    // GetConflictingChildren returns children of a conflicting transaction
    GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error)

    // SetConflicting marks transactions as conflicting or not conflicting
    SetConflicting(ctx context.Context, txHashes []chainhash.Hash, value bool) ([]*Spend, []chainhash.Hash, error)

    // SetUnspendable marks transactions as unspendable or spendable
    SetUnspendable(ctx context.Context, txHashes []chainhash.Hash, value bool) error

    // SetBlockHeight updates the current block height
    SetBlockHeight(height uint32) error

    // GetBlockHeight returns the current block height
    GetBlockHeight() uint32

    // SetMedianBlockTime updates the median block time
    SetMedianBlockTime(height uint32) error

    // GetMedianBlockTime returns the current median block time
    GetMedianBlockTime() uint32

    // Close closes the UTXO store and releases resources
    Close(ctx context.Context) error
}
```

## Key Functions

- `Health`: Checks the health status of the UTXO store.
- `Create`: Creates a new UTXO entry from a transaction.
- `Get`: Retrieves UTXO metadata for specific fields.
- `Delete`: Deletes a UTXO entry and its metadata.
- `GetSpend`: Retrieves information about a UTXO's spend status.
- `GetMeta`: Retrieves transaction metadata.
- `Spend`: Marks UTXOs as spent by a transaction.
- `Unspend`: Reverses the spent status of UTXOs during blockchain reorganization.
- `SetMinedMulti`: Updates block information for multiple transactions that have been mined.
- `BatchDecorate`: Efficiently fetches metadata for multiple transactions.
- `PreviousOutputsDecorate`: Fetches information about transaction inputs' previous outputs.
- `FreezeUTXOs`: Freezes UTXOs through the alert system.
- `UnFreezeUTXOs`: Unfreezes UTXOs that were previously frozen.
- `ReAssignUTXO`: Reassigns a UTXO to a new transaction output.
- `GetCounterConflicting`: Returns counter conflicting transactions for a transaction.
- `GetConflictingChildren`: Returns the children of a conflicting transaction.
- `SetConflicting`: Marks transactions as conflicting or not conflicting.
- `SetUnspendable`: Marks transactions as unspendable or spendable.
- `SetBlockHeight` / `GetBlockHeight`: Updates and retrieves the current block height.
- `SetMedianBlockTime` / `GetMedianBlockTime`: Updates and retrieves the median block time.
- `Close`: Closes the UTXO store and releases resources.

## Create Options

- `WithMinedBlockInfo`: Sets the block information (ID, height, and subtree index) for a new UTXO entry. This replaces the deprecated `WithBlockIDs` option and provides more detailed tracking of where UTXOs appear in the blockchain.
- `WithTXID`: Sets the transaction ID for a new UTXO entry.
- `WithSetCoinbase`: Sets the coinbase flag for a new UTXO entry.
- `WithFrozen`: Sets the frozen status for a new UTXO entry.
- `WithConflicting`: Sets the conflicting status for a new UTXO entry.
- `WithUnspendable`: Sets the transaction as unspendable on creation.

## Constants

- `MetaFields`: Default fields for metadata retrieval.
- `MetaFieldsWithTx`: Metadata fields including the transaction.

## Mock Implementation

The `MockUtxostore` struct provides a mock implementation of the `Store` interface for testing purposes.

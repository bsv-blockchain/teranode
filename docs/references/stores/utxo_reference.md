# UTXO Store Reference Documentation

## Overview

The UTXO (Unspent Transaction Output) Store provides an interface for managing and querying UTXO data in a blockchain system.

## Core Types

### Spend

Represents a UTXO being spent.

```go
type Spend struct {
TxID         *chainhash.Hash
Vout         uint32
UTXOHash     *chainhash.Hash
SpendingTxID *chainhash.Hash
}
```

### SpendResponse

Represents the response from a GetSpend operation.

```go
type SpendResponse struct {
Status       int
SpendingTxID *chainhash.Hash
LockTime     uint32
}
```

### UnresolvedMetaData

Holds metadata for unresolved transactions.

```go
type UnresolvedMetaData struct {
Hash   chainhash.Hash
Idx    int
Data   *meta.Data
Fields []string
Err    error
}
```

### CreateOptions

Options for creating a new UTXO entry.

```go
type CreateOptions struct {
BlockIDs   []uint32
TxID       *chainhash.Hash
IsCoinbase *bool
}
```

## Store Interface

The `Store` interface defines the contract for UTXO storage operations.

```go
type Store interface {
Health(ctx context.Context, checkLiveness bool) (int, string, error)
Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, error)
Get(ctx context.Context, hash *chainhash.Hash, fields ...[]string) (*meta.Data, error)
Delete(ctx context.Context, hash *chainhash.Hash) error
GetSpend(ctx context.Context, spend *Spend) (*SpendResponse, error)
GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error)
Spend(ctx context.Context, spends []*Spend, blockHeight uint32) error
UnSpend(ctx context.Context, spends []*Spend) error
SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error
BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*UnresolvedMetaData, fields ...string) error
PreviousOutputsDecorate(ctx context.Context, outpoints []*meta.PreviousOutput) error
FreezeUTXOs(ctx context.Context, spends []*Spend) error
UnFreezeUTXOs(ctx context.Context, spends []*Spend) error
ReAssignUTXO(ctx context.Context, utxo *Spend, newUtxo *Spend) error
SetBlockHeight(height uint32) error
GetBlockHeight() uint32
SetMedianBlockTime(height uint32) error
GetMedianBlockTime() uint32
}
```

## Key Functions

- `Health`: Checks the health status of the UTXO store.
- `Create`: Creates a new UTXO entry.
- `Get`: Retrieves UTXO metadata.
- `Delete`: Deletes a UTXO entry.
- `Spend`: Marks UTXOs as spent.
- `UnSpend`: Reverses the spent status of UTXOs.
- `BatchDecorate`: Decorates a batch of unresolved metadata.
- `FreezeUTXOs`: Freezes specified UTXOs.
- `UnFreezeUTXOs`: Unfreezes specified UTXOs.
- `ReAssignUTXO`: Reassigns a UTXO to a new owner.

## Create Options

- `WithBlockIDs`: Sets the block IDs for a new UTXO entry.
- `WithTXID`: Sets the transaction ID for a new UTXO entry.
- `WithSetCoinbase`: Sets the coinbase flag for a new UTXO entry.

## Constants

- `MetaFields`: Default fields for metadata retrieval.
- `MetaFieldsWithTx`: Metadata fields including the transaction.

## Mock Implementation

The `MockUtxostore` struct provides a mock implementation of the `Store` interface for testing purposes.

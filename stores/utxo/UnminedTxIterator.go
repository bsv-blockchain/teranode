package utxo

import (
	"context"

	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/libsv/go-bt/v2/chainhash"
)

// UnminedTransaction represents an unmined transaction in the UTXO store.
type UnminedTransaction struct {
	Hash       *chainhash.Hash
	Fee        uint64
	Size       uint64
	TxInpoints meta.TxInpoints
}

// UnminedTxIterator provides an interface to iterate over unmined transactions efficiently.
type UnminedTxIterator interface {
	// Next advances the iterator and returns the next unmined transaction, or nil if iteration is done. Returns an error if one occurred.
	Next(ctx context.Context) (*UnminedTransaction, error)
	// Err returns the first error encountered during iteration.
	Err() error
	// Close releases any resources held by the iterator.
	Close() error
}

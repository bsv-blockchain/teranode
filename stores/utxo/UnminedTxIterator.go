// Package utxo provides UTXO (Unspent Transaction Output) management for the Bitcoin SV Teranode implementation.
//
// This file defines the UnminedTxIterator interface and UnminedTransaction struct for efficiently
// iterating over transactions that have not yet been included in a block.
package utxo

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
)

// UnminedTransaction represents an unmined transaction in the UTXO store.
// It contains metadata about transactions that have been validated but not yet included in a block.
type UnminedTransaction struct {
	Hash       *chainhash.Hash
	Fee        uint64
	Size       uint64
	TxInpoints subtree.TxInpoints
	CreatedAt  int
	Locked     bool
}

// UnminedTxIterator provides an interface to iterate over unmined transactions efficiently.
// It enables streaming access to large sets of unmined transactions without loading them all into memory.
// Implementations should be safe for concurrent use and handle context cancellation appropriately.
type UnminedTxIterator interface {
	// Next advances the iterator and returns the next unmined transaction, or nil if iteration is done.
	// Returns an error if one occurred during iteration or data retrieval.
	Next(ctx context.Context) (*UnminedTransaction, error)
	// Err returns the first error encountered during iteration.
	// Should be called after Next returns nil to check for iteration errors.
	Err() error
	// Close releases any resources held by the iterator.
	// Must be called when iteration is complete to prevent resource leaks.
	Close() error
}

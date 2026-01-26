// Package utxo provides UTXO (Unspent Transaction Output) management for the Bitcoin SV Teranode implementation.
//
// This file defines the UnminedTxIterator interface and UnminedTransaction struct for efficiently
// iterating over transactions that have not yet been included in a block.
package utxo

import (
	"context"

	"github.com/bsv-blockchain/go-subtree"
)

// UnminedTransaction represents an unmined transaction in the UTXO store.
// It contains metadata about transactions that have been validated but not yet included in a block.
type UnminedTransaction struct {
	*subtree.Node
	TxInpoints   *subtree.TxInpoints
	CreatedAt    int
	Locked       bool
	Skip         bool
	UnminedSince int
	BlockIDs     []uint32
}

// UnminedTxIterator provides an interface to iterate over unmined transactions efficiently.
// It enables streaming access to large sets of unmined transactions without loading them all into memory.
// Implementations should be safe for concurrent use and handle context cancellation appropriately.
type UnminedTxIterator interface {
	// Next advances the iterator and returns a batch of unmined transactions, or nil if iteration is done.
	// The batch size is implementation-dependent and optimized for performance.
	// Returns an empty slice when iteration is complete, or an error if one occurred during iteration or data retrieval.
	Next(ctx context.Context) ([]*UnminedTransaction, error)
	// Err returns the first error encountered during iteration.
	// Should be called after Next returns nil to check for iteration errors.
	Err() error
	// Close releases any resources held by the iterator.
	// Must be called when iteration is complete to prevent resource leaks.
	Close() error
}

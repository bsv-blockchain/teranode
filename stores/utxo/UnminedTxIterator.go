// Package utxo provides UTXO (Unspent Transaction Output) management for the BSV Blockchain Teranode implementation.
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
	*subtree.Node
	TxInpoints   *subtree.TxInpoints
	CreatedAt    int
	Locked       bool
	Skip         bool
	UnminedSince int
	BlockIDs     []uint32
}

// InconsistentTxRecord is a lightweight record used by the consistency scan.
// It contains only the fields needed to detect unmined_since inconsistencies:
// transactions that have block_ids on the main chain but unmined_since still set.
type InconsistentTxRecord struct {
	Hash         chainhash.Hash
	BlockIDs     []uint32
	UnminedSince int
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

// ConsistencyScanIterator provides a lightweight scan over all records to detect
// unmined_since inconsistencies. Unlike UnminedTxIterator, it only fetches
// txid, block_ids, and unmined_since — no TxInpoints or heavy data.
type ConsistencyScanIterator interface {
	Next(ctx context.Context) ([]*InconsistentTxRecord, error)
	TotalScanned() int64
	Err() error
	Close() error
}

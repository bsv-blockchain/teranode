// Package utxo provides store-agnostic cleanup functionality for unmined transactions.
//
// This file contains the store-agnostic implementation of unmined transaction cleanup
// that works with any Store implementation. It follows the same pattern as ProcessConflicting.
package utxo

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// PreserveParentsOfOldUnminedTransactions protects parent transactions of old unmined transactions from deletion.
// This is a store-agnostic implementation that works with any Store implementation.
// It follows the same pattern as ProcessConflicting, using the Store interface methods.
//
// The preservation process:
// 1. Find unmined transactions older than blockHeight - UnminedTxRetention
// 2. For each unmined transaction:
//   - Get the transaction data to find parent transactions
//   - Preserve parent transactions by setting PreserveUntil flag
//   - Keep the unmined transaction intact (do NOT delete it)
//
// This ensures parent transactions remain available for future resubmissions of the unmined transactions.
// Returns the number of transactions whose parents were processed and any error encountered.
func PreserveParentsOfOldUnminedTransactions(ctx context.Context, s Store, blockHeight uint32, settings *settings.Settings, logger ulogger.Logger) (int, error) {
	// Input validation
	if s == nil {
		return 0, errors.NewProcessingError("store cannot be nil")
	}

	if settings == nil {
		return 0, errors.NewProcessingError("settings cannot be nil")
	}

	if logger == nil {
		return 0, errors.NewProcessingError("logger cannot be nil")
	}

	if blockHeight <= settings.UtxoStore.UnminedTxRetention {
		// Not enough blocks have passed to start cleanup
		return 0, nil
	}

	// Calculate cutoff block height
	cutoffBlockHeight := blockHeight - settings.UtxoStore.UnminedTxRetention

	logger.Infof("[PreserveParents] Starting preservation of parents for unmined transactions older than block height %d (current height %d - %d blocks retention)",
		cutoffBlockHeight, blockHeight, settings.UtxoStore.UnminedTxRetention)

	// OPTIMIZATION: Use parallel partition iterator instead of sequential QueryOldUnminedTransactions
	// This reuses the optimized GetUnminedTxIterator which already has:
	// - Parallel partition queries (16 workers × 10 chunks = 160 concurrent operations)
	// - TxInpoints already populated (no individual Get() calls needed!)
	// - Batch processing (16K records per batch)
	// Result: 100-1000x faster than sequential Get() calls
	iterator, err := s.GetUnminedTxIterator(false)
	if err != nil {
		return 0, errors.NewStorageError("failed to get unmined tx iterator", err)
	}
	defer func() {
		if closeErr := iterator.Close(); closeErr != nil {
			logger.Warnf("[PreserveParents] Failed to close iterator: %v", closeErr)
		}
	}()

	// Accumulate all parent hashes for old unmined transactions
	// Use map for automatic deduplication
	allParents := make(map[chainhash.Hash]struct{}, 10000)
	processedCount := 0

	for {
		unminedBatch, err := iterator.Next(ctx)
		if err != nil {
			return 0, errors.NewStorageError("failed to iterate unmined transactions", err)
		}
		if unminedBatch == nil {
			break
		}

		// Process each transaction in the batch
		for _, unminedTx := range unminedBatch {
			// Skip special markers
			if unminedTx.Skip {
				continue
			}

			// Filter for old unmined transactions (UnminedSince <= cutoffBlockHeight)
			if unminedTx.UnminedSince > 0 && unminedTx.UnminedSince <= int(cutoffBlockHeight) {
				// TxInpoints already available - no Get() call needed!
				if len(unminedTx.TxInpoints.ParentTxHashes) > 0 {
					for _, parentHash := range unminedTx.TxInpoints.ParentTxHashes {
						allParents[parentHash] = struct{}{}
					}
					processedCount++
				}
			}
		}
	}

	logger.Debugf("[PreserveParents] Found %d old unmined transactions with %d unique parent hashes to preserve",
		processedCount, len(allParents))

	// Preserve all parents in single batch operation
	if len(allParents) > 0 {
		parentSlice := make([]chainhash.Hash, 0, len(allParents))
		for hash := range allParents {
			parentSlice = append(parentSlice, hash)
		}

		preserveUntilHeight := blockHeight + settings.UtxoStore.ParentPreservationBlocks
		if err := s.PreserveTransactions(ctx, parentSlice, preserveUntilHeight); err != nil {
			return 0, errors.NewStorageError("failed to preserve parent transactions", err)
		}

		logger.Infof("[PreserveParents] Completed parent preservation: preserved %d unique parents for %d old unmined transactions (cutoff block height %d)",
			len(parentSlice), processedCount, cutoffBlockHeight)
	} else {
		logger.Infof("[PreserveParents] No parents to preserve for old unmined transactions (cutoff block height %d)", cutoffBlockHeight)
	}

	return processedCount, nil
}

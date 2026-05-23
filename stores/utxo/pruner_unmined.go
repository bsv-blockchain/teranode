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
func PreserveParentsOfOldUnminedTransactions(ctx context.Context, s Store, blockHeight uint32, blockHashStr string, settings *settings.Settings, logger ulogger.Logger) (int, error) {
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

	logger.Infof("[pruner][%s:%d] phase 1: starting parent preservation (cutoff: %d)", blockHashStr, blockHeight, cutoffBlockHeight)

	// OPTIMIZATION: Use pruner-specific lightweight iterator that:
	// - Filters server-side: only unmined txs with unminedSince in [1, cutoffBlockHeight]
	// - Fetches minimal bins: txID, unminedSince, external, inputs (4 bins vs 9-11)
	// - Uses parallel partition queries for throughput
	// Result: 90-99%+ bandwidth reduction vs full iterator when mempool is large
	iterator, err := s.GetPrunableUnminedTxIterator(cutoffBlockHeight)
	if err != nil {
		return 0, errors.NewStorageError("failed to get unmined tx iterator", err)
	}
	defer func() {
		if closeErr := iterator.Close(); closeErr != nil {
			logger.Warnf("[pruner][%s:%d] phase 1: failed to close iterator: %v", blockHashStr, blockHeight, closeErr)
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
		// Server-side filter already ensures UnminedSince is in [1, cutoffBlockHeight]
		for _, unminedTx := range unminedBatch {
			if unminedTx.Skip {
				continue
			}

			// TxInpoints already available from the iterator
			if unminedTx.TxInpoints != nil && len(unminedTx.TxInpoints.ParentTxHashes) > 0 {
				for _, parentHash := range unminedTx.TxInpoints.ParentTxHashes {
					allParents[parentHash] = struct{}{}
				}
				processedCount++
			}
		}
	}

	logger.Debugf("[pruner][%s:%d] phase 1: scanned %d unmined transactions, found %d unique parents",
		blockHashStr, blockHeight, processedCount, len(allParents))

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

		logger.Infof("[pruner][%s:%d] phase 1: preserved %d unique parents for %d old unmined transactions",
			blockHashStr, blockHeight, len(parentSlice), processedCount)
	} else {
		logger.Infof("[pruner][%s:%d] phase 1: no parents to preserve", blockHashStr, blockHeight)
	}

	return processedCount, nil
}

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
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const (
	// Error message templates for better consistency and maintainability
	errFailedToGetTransaction = "failed to get transaction %s"
	errNoTransactionData      = "transaction %s has no inpoints"
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
	cleanupCount := 0

	logger.Infof("[PreserveParents] Starting preservation of parents for unmined transactions older than block height %d (current height %d - %d blocks retention)",
		cutoffBlockHeight, blockHeight, settings.UtxoStore.UnminedTxRetention)

	// Query for old unmined transactions using the store's QueryOldUnminedTransactions method
	oldUnminedTxHashes, err := s.QueryOldUnminedTransactions(ctx, cutoffBlockHeight)
	if err != nil {
		return 0, errors.NewStorageError("failed to query old unmined transactions", err)
	}

	// Log if we found zero old unmined txs - may indicate timing issue during/after catchup
	if len(oldUnminedTxHashes) == 0 {
		logger.Infof("[PreserveParents] Found 0 old unmined transactions at cutoff height %d (this is normal during steady-state operation)", cutoffBlockHeight)
	} else {
		logger.Infof("[PreserveParents] Found %d old unmined transactions to process for parent preservation", len(oldUnminedTxHashes))
	}

	// Process each transaction for parent preservation
	for _, txHash := range oldUnminedTxHashes {
		if err := preserveSingleUnminedTransactionParents(ctx, s, &txHash, blockHeight, settings, logger); err != nil {
			logger.Errorf("[PreserveParents] Failed to preserve parents for transaction %s: %v", txHash.String(), err)
			continue
		}

		cleanupCount++

		logger.Debugf("[PreserveParents] Preserved parents for unmined transaction %s", txHash.String())
	}

	logger.Infof("[PreserveParents] Completed parent preservation for %d old unmined transactions (cutoff block height %d)", cleanupCount, cutoffBlockHeight)

	return cleanupCount, nil
}

// preserveSingleUnminedTransactionParents handles the preservation of parent transactions for a single unmined transaction.
// It preserves parent transactions by setting PreserveUntil flag. The unmined transaction itself is NOT deleted.
func preserveSingleUnminedTransactionParents(ctx context.Context, s Store, txHash *chainhash.Hash, blockHeight uint32, settings *settings.Settings, logger ulogger.Logger) error {
	// Get the transaction data to identify parent UTXOs (similar to ProcessConflicting)
	txMeta, err := s.Get(ctx, txHash, fields.TxInpoints)
	if err != nil {
		return errors.NewProcessingError(errFailedToGetTransaction, txHash.String(), err)
	}

	if len(txMeta.TxInpoints.ParentTxHashes) == 0 {
		return errors.NewProcessingError(errNoTransactionData, txHash.String())
	}

	// Preserve parent transactions (best effort)
	if err := preserveParentTransactions(ctx, s, txMeta.TxInpoints.ParentTxHashes, txHash, blockHeight, settings, logger); err != nil {
		// Log error but continue - preservation is best effort
		logger.Errorf("[PreserveParents] Failed to preserve parent transactions for unmined tx %s: %v",
			txHash.String(), err)
		return err
	}

	// NOTE: We do NOT delete the unmined transaction - it remains available for future resubmission
	logger.Debugf("[PreserveParents] Successfully preserved parent transactions for unmined tx %s (transaction preserved)", txHash.String())

	return nil
}

// preserveParentTransactions sets the PreserveUntil flag on parent transactions.
// This is separated to reduce cognitive complexity and improve testability.
func preserveParentTransactions(ctx context.Context, s Store, parentTxHashes []chainhash.Hash, txHash *chainhash.Hash, blockHeight uint32, settings *settings.Settings, logger ulogger.Logger) error {
	if len(parentTxHashes) == 0 {
		return nil
	}

	preserveUntilHeight := blockHeight + settings.UtxoStore.ParentPreservationBlocks
	logger.Debugf("[PreserveParents] Preserving %d parent transactions until height %d for unmined tx %s",
		len(parentTxHashes), preserveUntilHeight, txHash.String())

	return s.PreserveTransactions(ctx, parentTxHashes, preserveUntilHeight)
}

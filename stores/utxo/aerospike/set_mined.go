// //go:build aerospike

// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
// It offers high performance, distributed storage capabilities with support for large-scale
// UTXO sets and complex operations like freezing, reassignment, and batch processing.
//
// # Architecture
//
// The implementation uses a combination of Aerospike Key-Value store and Lua scripts
// for atomic operations. Transactions are stored with the following structure:
//   - Main Record: Contains transaction metadata and up to 20,000 UTXOs
//   - Pagination Records: Additional records for transactions with >20,000 outputs
//   - External Storage: Optional blob storage for large transactions
//
// # Features
//
//   - Efficient UTXO lifecycle management (create, spend, unspend)
//   - Support for batched operations with LUA scripting
//   - Automatic cleanup of spent UTXOs through DAH
//   - Alert system integration for freezing/unfreezing UTXOs
//   - Metrics tracking via Prometheus
//   - Support for large transactions through external blob storage
//
// # Usage
//
//	store, err := aerospike.New(ctx, logger, settings, &url.URL{
//	    Scheme: "aerospike",
//	    Host:   "localhost:3000",
//	    Path:   "/test/utxos",
//	    RawQuery: "expiration=3600&set=txmeta",
//	})
//
// # Database Structure
//
// Normal Transaction:
//   - inputs: Transaction input data
//   - outputs: Transaction output data
//   - utxos: List of UTXO hashes
//   - totalUtxos: Total number of UTXOs in the transaction
//   - recordUtxos: Total number of UTXO in this record
//   - spentUtxos: Number of spent UTXOs in this record
//   - blockIDs: Block references
//   - isCoinbase: Coinbase flag
//   - spendingHeight: Coinbase maturity height
//   - frozen: Frozen status
//
// Large Transaction with External Storage:
//   - Same as normal but with external=true
//   - Transaction data stored in blob storage
//   - Multiple records for >20k outputs
//
// # Thread Safety
//
// The implementation is fully thread-safe and supports concurrent access through:
//   - Atomic operations via Lua scripts
//   - Batched operations for better performance
//   - Lock-free reads with optimistic concurrency
package aerospike

import (
	"context"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// SetMinedMulti updates the block references for multiple transactions in batch.
// This operation marks transactions as mined in a specific block using Lua scripts
// for atomic updates.
//
// # Operation Details
//
// For each transaction:
//  1. Creates a batch UDF operation to update the block reference
//  2. Executes all updates in a single batch operation
//  3. Handles DAH settings for record expiration
//  4. Tracks metrics for successful and failed updates
//
// The operation is idempotent and handles cases where:
//   - Transaction doesn't exist (silently continues)
//   - Transaction is already mined in the block
//   - Partial batch failures occur
//
// # Error Handling
//
// The function aggregates errors across the batch and:
//   - Continues on KEY_NOT_FOUND errors (transaction deleted)
//   - Counts successful and failed updates
//   - Provides detailed error context per transaction
//   - Updates error metrics for monitoring
//
// # Performance
//
// Uses batch operations to optimize performance:
//   - Single network round trip for multiple updates
//   - Concurrent processing via Lua scripts
//   - No read-modify-write cycle required
//   - Efficient error handling without transaction rollback
//
// Parameters:
//   - ctx: Context for tracing and cancellation
//   - hashes: Array of transaction hashes to update
//   - blockID: Block height where transactions were mined
//
// Returns:
//   - error: Aggregated errors from batch operation or nil if successful
//
// Example:
//
//	hashes := []*chainhash.Hash{tx1Hash, tx2Hash, tx3Hash}
//	err := store.SetMinedMulti(ctx, hashes, blockHeight)
//	if err != nil {
//	    // Handle errors, some updates may have succeeded
//	}
//
// Metrics:
//   - prometheusTxMetaAerospikeMapSetMinedBatch: Batch operation count
//   - prometheusTxMetaAerospikeMapSetMinedBatchN: Successful updates
//   - prometheusTxMetaAerospikeMapSetMinedBatchErrN: Failed updates
func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	_, _, deferFn := tracing.Tracer("aerospike").Start(ctx, "aerospike:SetMinedMulti2")
	defer deferFn()

	thisBlockHeight := s.blockHeight.Load() + 1

	// Prepare batch records
	batchRecords, err := s.prepareBatchRecordsForSetMined(hashes, minedBlockInfo, thisBlockHeight)
	if err != nil {
		return nil, err
	}

	// Execute batch operation
	if err = s.executeBatchOperation(batchRecords); err != nil {
		return nil, err
	}

	// Process batch results
	return s.processBatchResultsForSetMined(ctx, batchRecords, hashes, thisBlockHeight, minedBlockInfo)
}

// prepareBatchRecordsForSetMined creates batch records for the setMined operation
func (s *Store) prepareBatchRecordsForSetMined(hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo, thisBlockHeight uint32) ([]aerospike.BatchRecordIfc, error) {
	batchRecords := make([]aerospike.BatchRecordIfc, len(hashes))
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()

	for idx, hash := range hashes {
		key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
		if err != nil {
			return nil, errors.NewProcessingError("aerospike NewKey error", err)
		}

		batchRecords[idx] = aerospike.NewBatchUDF(
			batchUDFPolicy,
			key,
			LuaPackage,
			"setMined",
			aerospike.NewValue(minedBlockInfo.BlockID),
			aerospike.NewValue(minedBlockInfo.BlockHeight),
			aerospike.NewValue(minedBlockInfo.SubtreeIdx),
			aerospike.NewValue(thisBlockHeight),
			aerospike.NewValue(s.settings.GetUtxoStoreBlockHeightRetention()),
			aerospike.BoolValue(minedBlockInfo.OnLongestChain),
			aerospike.BoolValue(minedBlockInfo.UnsetMined),
		)
	}

	return batchRecords, nil
}

// executeBatchOperation performs the batch operation and increments metrics
func (s *Store) executeBatchOperation(batchRecords []aerospike.BatchRecordIfc) error {
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		return errors.NewStorageError("aerospike BatchOperate error", err)
	}

	prometheusTxMetaAerospikeMapSetMinedBatch.Inc()

	return nil
}

// processBatchResultsForSetMined processes the results of the batch operation
func (s *Store) processBatchResultsForSetMined(ctx context.Context, batchRecords []aerospike.BatchRecordIfc,
	hashes []*chainhash.Hash, thisBlockHeight uint32, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	var errs error
	okUpdates := 0
	nrErrors := 0
	blockIDs := make(map[chainhash.Hash][]uint32, len(hashes))

	// Collect follow-up actions to execute in batches after the loop
	extraRecords := make([]*chainhash.Hash, 0, len(hashes))
	dahSetItems := make([]struct {
		TxID           *chainhash.Hash
		ChildCount     int
		DeleteAtHeight uint32
	}, 0)
	dahUnsetItems := make([]struct {
		TxID           *chainhash.Hash
		ChildCount     int
		DeleteAtHeight uint32
	}, 0)
	// DAH timing assumption:
	// - thisBatch operates under a fixed block-processing context.
	// - thisBlockHeight and retention are immutable for the duration of SetMinedMulti() execution.
	// Therefore, computing DeleteAtHeight (DAH) once is safe and consistent across all signals in this batch.
	// If this assumption ever changes (e.g., SetBlockHeight or retention can mutate concurrently),
	// DAH must be computed per-record at signal time to avoid stale values.
	//
	// Only calculate DAH if BlockHeightRetention is configured (> 0)
	// When retention is 0, it means "don't use automatic retention"
	retention := s.settings.GetUtxoStoreBlockHeightRetention()
	var dahHeight uint32
	if retention > 0 {
		dahHeight = thisBlockHeight + retention
	}

	for idx, batchRecord := range batchRecords {
		result, res, err := s.processSingleBatchRecord(ctx, batchRecord, hashes[idx], thisBlockHeight, minedBlockInfo)
		if err != nil {
			if errs == nil {
				errs = err
			} else {
				errs = errors.Join(errs, err)
			}

			nrErrors++
		} else if result {
			okUpdates++
		}

		if res != nil && res.BlockIDs != nil {
			blockIDsUint32 := make([]uint32, len(res.BlockIDs))
			for i, bID := range res.BlockIDs {
				bID32, err := safeconversion.IntToUint32(bID)
				if err != nil {
					errs = errors.Join(errs, errors.NewProcessingError("aerospike SetMinedMulti blockID conversion error", err))
					nrErrors++
					continue
				}

				blockIDsUint32[i] = bID32
			}

			blockIDs[*hashes[idx]] = blockIDsUint32
		}

		// Aggregate signals for batched follow-up work
		if res != nil && res.Signal != "" {
			switch res.Signal {
			case LuaSignalAllSpent:
				extraRecords = append(extraRecords, hashes[idx])
			case LuaSignalDAHSet:
				// Only set DAH if retention is configured
				if retention > 0 {
					dahSetItems = append(dahSetItems, struct {
						TxID           *chainhash.Hash
						ChildCount     int
						DeleteAtHeight uint32
					}{TxID: hashes[idx], ChildCount: res.ChildCount, DeleteAtHeight: dahHeight})
					// External store DAH is disabled - lifecycle managed by pruner service
				}
			case LuaSignalDAHUnset:
				dahUnsetItems = append(dahUnsetItems, struct {
					TxID           *chainhash.Hash
					ChildCount     int
					DeleteAtHeight uint32
				}{TxID: hashes[idx], ChildCount: res.ChildCount, DeleteAtHeight: 0})
				// External store DAH is disabled - lifecycle managed by pruner service
			}
		}
	}

	prometheusTxMetaAerospikeMapSetMinedBatchN.Add(float64(okUpdates))

	if errs != nil || nrErrors > 0 {
		prometheusTxMetaAerospikeMapSetMinedBatchErrN.Add(float64(nrErrors))
		return blockIDs, errors.NewError("aerospike batchRecord errors", errs)
	}

	// Execute aggregated follow-ups in batches
	var postErr error

	if len(extraRecords) > 0 {
		if err := s.IncrementSpentRecordsMulti(extraRecords, 1); err != nil {
			postErr = errors.Join(postErr, err)
		}
	}

	if len(dahSetItems) > 0 {
		if err := s.SetDAHForChildRecordsMulti(dahSetItems); err != nil {
			postErr = errors.Join(postErr, err)
		}
	}

	if len(dahUnsetItems) > 0 {
		if err := s.SetDAHForChildRecordsMulti(dahUnsetItems); err != nil {
			postErr = errors.Join(postErr, err)
		}
	}

	// External store DAH is disabled - lifecycle managed by pruner service
	// setDAHExternalTransactionMulti removed as it would no-op

	if postErr != nil {
		return blockIDs, errors.NewError("aerospike setMined follow-up batch errors", postErr)
	}

	return blockIDs, nil
}

// processSingleBatchRecord processes a single batch record result
func (s *Store) processSingleBatchRecord(ctx context.Context, batchRecord aerospike.BatchRecordIfc, hash *chainhash.Hash,
	thisBlockHeight uint32, minedBlockInfo utxo.MinedBlockInfo) (bool, *LuaMapResponse, error) {
	batchErr := batchRecord.BatchRec().Err
	if batchErr != nil {
		return false, nil, s.handleBatchRecordError(batchErr, hash)
	}

	response := batchRecord.BatchRec().Record
	if response == nil || response.Bins == nil || response.Bins[LuaSuccess.String()] == nil {
		return false, nil, errors.NewError("missing SUCCESS bin in aerospike response for transaction %s", hash.String())
	}

	res, parseErr := s.ParseLuaMapResponse(response.Bins[LuaSuccess.String()])
	if parseErr != nil {
		return false, nil, errors.NewError("aerospike batchRecord %s ParseLuaMapResponse error", hash.String(), parseErr)
	}

	if res.Status != LuaStatusOK {
		if res.ErrorCode == LuaErrorCodeTxNotFound {
			if minedBlockInfo.UnsetMined {
				// This is not an error for us, just a no-op, if we are unsetting mined status on a tx that does not exist
				return true, res, nil
			}

			return false, res, errors.NewTxNotFoundError("transaction not found: %s", hash.String())
		}

		return false, res, errors.NewError("aerospike batchRecord %s error: %s", hash.String(), res.Message)
	}

	return true, res, nil
}

// handleBatchRecordError handles errors from batch records
func (s *Store) handleBatchRecordError(err error, hash *chainhash.Hash) error {
	var aErr *aerospike.AerospikeError
	if errors.As(err, &aErr) && aErr != nil && aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
		// the tx Meta does not exist anymore, so we do not have to set the mined status
		return nil
	}
	return errors.NewStorageError("aerospike batchRecord error", hash.String(), err)
}

// handleSetMinedSignal handles signals from the setMined operation
func (s *Store) handleSetMinedSignal(ctx context.Context, signal LuaSignal, hash *chainhash.Hash, childCount int, thisBlockHeight uint32) error {
	var errs error

	switch signal {
	case LuaSignalAllSpent:
		if err := s.handleExtraRecords(ctx, hash, 1); err != nil {
			errs = errors.Join(errs, err)
		}

	case LuaSignalDAHSet:
		// Only set DAH if BlockHeightRetention is configured (> 0)
		// When retention is 0, it means "don't use automatic retention"
		if retention := s.settings.GetUtxoStoreBlockHeightRetention(); retention > 0 {
			dahHeight := thisBlockHeight + retention

			if err := s.SetDAHForChildRecords(hash, childCount, dahHeight); err != nil {
				errs = errors.Join(errs, err)
			}
			// External store DAH is disabled - lifecycle managed by pruner service
		}

	case LuaSignalDAHUnset:
		if err := s.SetDAHForChildRecords(hash, childCount, 0); err != nil {
			errs = errors.Join(errs, err)
		}
		// External store DAH is disabled - lifecycle managed by pruner service
	}

	return errs
}

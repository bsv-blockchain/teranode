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
//   - totalUtxos: Total number of UTXOs
//   - spentUtxos: Number of spent UTXOs
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
	"strings"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/spend"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
)

// FreezeUTXOs marks UTXOs as frozen by setting their spending transaction ID to FF...FF.
// Frozen UTXOs cannot be spent until unfrozen or reassigned.
//
// The operation is performed atomically via a Lua script that:
//   - Verifies the UTXO exists and matches the provided hash
//   - Checks the UTXO is not already spent or frozen
//   - Sets the spending transaction ID to FF...FF to mark as frozen
//
// Parameters:
//   - ctx: Context for cancellation/timeout
//   - spends: Array of UTXOs to freeze
//
// Returns error if any UTXO:
//   - Doesn't exist
//   - Is already spent
//   - Is already frozen
//   - Fails to freeze
func (s *Store) FreezeUTXOs(_ context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(spends))

	sUtxoBatchSizeUint32, err := safeconversion.IntToUint32(s.utxoBatchSize)
	if err != nil {
		return err
	}

	for _, spend := range spends {
		keySource := uaerospike.CalculateKeySource(spend.TxID, spend.Vout/sUtxoBatchSizeUint32)

		aeroKey, aErr := aerospike.NewKey(s.namespace, s.setName, keySource)
		if aErr != nil {
			return aErr
		}

		batchRecords = append(batchRecords, aerospike.NewBatchUDF(batchUDFPolicy, aeroKey, LuaPackage, "freeze",
			aerospike.NewValue(s.calculateOffsetForOutput(spend.Vout)),
			aerospike.NewValue(spend.UTXOHash[:]),
		))
	}

	batchID := s.batchID.Add(1)

	batchPolicy := util.GetAerospikeBatchPolicy(tSettings)
	if err = s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		return errors.NewStorageError("[FREEZE_BATCH_LUA][%d] failed to batch freeze %d aerospike utxos", batchID, len(spends), err)
	}

	// check the return value of the batch operation
	errorsThrown := make([]error, 0, len(spends))

	for _, record := range batchRecords {
		if record.BatchRec().Err != nil {
			errorsThrown = append(errorsThrown, errors.NewStorageError("[FREEZE_BATCH_LUA][%d] failed to batch freeze %d aerospike utxos", batchID, len(spends), record.BatchRec().Err))
		} else {
			// check the return value of the batch operation
			response := record.BatchRec().Record
			if response != nil && response.Bins != nil && response.Bins[LuaSuccess.String()] != nil {
				res, err := s.ParseLuaMapResponse(response.Bins[LuaSuccess.String()])
				if err != nil {
					errorsThrown = append(errorsThrown, errors.NewStorageError("[FREEZE_BATCH_LUA][%d] failed to parse response", batchID, err))
				} else if res.Status == LuaStatusError {
					if res.ErrorCode == LuaErrorCodeSpent {
						// Extract spending data from error message
						hexData := strings.TrimPrefix(res.Message, "SPENT:")
						if spendingData, parseErr := spend.NewSpendingDataFromString(hexData); parseErr == nil {
							errorsThrown = append(errorsThrown, errors.NewStorageError("[FREEZE_BATCH_LUA][%d] failed to freeze aerospike utxo because it's already SPENT by %v", batchID, spendingData))
						} else {
							errorsThrown = append(errorsThrown, errors.NewStorageError("[FREEZE_BATCH_LUA][%d] failed to freeze aerospike utxo: %s", batchID, res.Message))
						}
					}
				}
			}
		}
	}

	if len(errorsThrown) > 0 {
		return errors.NewStorageError("[FREEZE_BATCH_LUA][%d] failed to batch freeze %d aerospike utxos: %v", batchID, len(spends), errorsThrown)
	}

	return nil
}

// UnFreezeUTXOs removes the frozen status from UTXOs by clearing the frozen spending transaction ID.
// This re-enables normal spending of the UTXOs.
//
// The operation is performed atomically via a Lua script that:
//   - Verifies the UTXO exists and matches the provided hash
//   - Checks the UTXO is currently frozen
//   - Clears the frozen spending transaction ID (the frozen spendingTxID)
//
// Parameters:
//   - ctx: Context for cancellation/timeout
//   - spends: Array of UTXOs to unfreeze
//
// Returns error if any UTXO:
//   - Doesn't exist
//   - Is not frozen
//   - Fails to unfreeze
func (s *Store) UnFreezeUTXOs(_ context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(spends))

	sUtxoBatchSizeUint32, err := safeconversion.IntToUint32(s.utxoBatchSize)
	if err != nil {
		return err
	}

	for _, spend := range spends {
		keySource := uaerospike.CalculateKeySource(spend.TxID, spend.Vout/sUtxoBatchSizeUint32)

		aeroKey, aErr := aerospike.NewKey(s.namespace, s.setName, keySource)
		if aErr != nil {
			return aErr
		}

		batchRecords = append(batchRecords, aerospike.NewBatchUDF(batchUDFPolicy, aeroKey, LuaPackage, "unfreeze",
			aerospike.NewValue(s.calculateOffsetForOutput(spend.Vout)),
			aerospike.NewValue(spend.UTXOHash[:]),
		))
	}

	batchID := s.batchID.Add(1)

	batchPolicy := util.GetAerospikeBatchPolicy(tSettings)
	if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		return errors.NewStorageError("[UNFREEZE_BATCH_LUA][%d] failed to batch freeze %d aerospike utxos", batchID, len(spends), err)
	}

	// check the return value of the batch operation
	errorsThrown := make([]error, 0, len(spends))

	for _, record := range batchRecords {
		if record.BatchRec().Err != nil {
			errorsThrown = append(errorsThrown, errors.NewStorageError("[UNFREEZE_BATCH_LUA][%d] failed to batch unfreeze %d aerospike utxos", batchID, len(spends), record.BatchRec().Err))
		} else {
			// check the return value of the batch operation
			response := record.BatchRec().Record
			if response != nil && response.Bins != nil && response.Bins[LuaSuccess.String()] != nil {
				res, err := s.ParseLuaMapResponse(response.Bins[LuaSuccess.String()])
				if err != nil {
					errorsThrown = append(errorsThrown, errors.NewStorageError("[UNFREEZE_BATCH_LUA][%d] failed to parse response", batchID, err))
				} else if res.Status == LuaStatusError {
					errorsThrown = append(errorsThrown, errors.NewStorageError("[UNFREEZE_BATCH_LUA][%d] failed to unfreeze aerospike utxo: %s", batchID, res.Message))
				}
			}
		}
	}

	if len(errorsThrown) > 0 {
		return errors.NewStorageError("[UNFREEZE_BATCH_LUA][%d] failed to batch unfreeze %d aerospike utxos: %v", batchID, len(spends), errorsThrown)
	}

	return nil
}

// ReAssignUTXO reassigns a frozen UTXO to a new transaction output.
// The UTXO must be frozen before it can be reassigned.
//
// The reassignment process:
//   - Verifies the UTXO exists and is frozen
//   - Updates the UTXO hash to the new value
//   - Sets spendable block height to current + ReAssignedUtxoSpendableAfterBlocks
//   - Logs the reassignment for audit purposes
//
// Parameters:
//   - ctx: Context for cancellation/timeout
//   - oldUtxo: The frozen UTXO to reassign
//   - newUtxo: The new UTXO details
//
// Returns error if:
//   - Original UTXO doesn't exist
//   - Original UTXO is not frozen
//   - Reassignment fails
func (s *Store) ReAssignUTXO(_ context.Context, oldUtxo *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	sUtxoBatchSizeUint32, err := safeconversion.IntToUint32(s.utxoBatchSize)
	if err != nil {
		return err
	}

	keySource := uaerospike.CalculateKeySource(oldUtxo.TxID, oldUtxo.Vout/sUtxoBatchSizeUint32)

	aeroKey, aErr := aerospike.NewKey(s.namespace, s.setName, keySource)
	if aErr != nil {
		return aErr
	}

	batchUDFPolicy := aerospike.NewBatchUDFPolicy()

	batchRecords := []aerospike.BatchRecordIfc{
		aerospike.NewBatchUDF(batchUDFPolicy, aeroKey, LuaPackage, "reassign",
			aerospike.NewValue(s.calculateOffsetForOutput(oldUtxo.Vout)),
			aerospike.NewValue(oldUtxo.UTXOHash[:]),
			aerospike.NewValue(newUtxo.UTXOHash[:]),
			aerospike.NewIntegerValue(int(s.blockHeight.Load())),
			aerospike.NewIntegerValue(utxo.ReAssignedUtxoSpendableAfterBlocks),
		),
	}

	batchPolicy := util.GetAerospikeBatchPolicy(tSettings)
	if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		return errors.NewStorageError("[REASSIGN_BATCH_LUA] failed to reassign aerospike utxo", err)
	}

	// check whether an error was thrown
	if batchRecords[0].BatchRec().Err != nil {
		return errors.NewStorageError("[REASSIGN_BATCH_LUA] failed to reassign aerospike utxo", batchRecords[0].BatchRec().Err)
	}

	// check the return value of the batch operation
	response := batchRecords[0].BatchRec().Record
	if response != nil && response.Bins != nil && response.Bins[LuaSuccess.String()] != nil {
		res, err := s.ParseLuaMapResponse(response.Bins[LuaSuccess.String()])
		if err != nil {
			return errors.NewStorageError("[REASSIGN_BATCH_LUA] failed to parse response: %s", err)
		} else if res.Status == LuaStatusError {
			return errors.NewStorageError("[REASSIGN_BATCH_LUA] failed to reassign aerospike utxo: %s", res.Message)
		}
	}

	return nil
}

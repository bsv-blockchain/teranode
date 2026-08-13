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
	"fmt"
	"strings"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
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

	for _, spend := range spends {
		keySource := uaerospike.CalculateKeySource(spend.TxID, spend.Vout, s.utxoBatchSize)

		aeroKey, aErr := aerospike.NewKey(s.namespace, s.setName, keySource)
		if aErr != nil {
			return aErr
		}

		batchRecords = append(batchRecords, s.teranodeBatchRecord(
			batchUDFPolicy, LuaPackage, aeroKey, subOpFreeze, "freeze",
			s.calculateOffsetForOutput(spend.Vout),
			spend.UTXOHash[:],
		))
	}

	batchID := s.batchID.Add(1)

	batchPolicy := util.GetAerospikeBatchPolicy(tSettings)
	if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		return errors.NewStorageError("[freeze][%d] failed to batch freeze %d aerospike utxos: %s", batchID, len(spends), err.Error(), err)
	}

	// check the return value of the batch operation
	errorsThrown := make([]error, 0, len(spends))

	for idx, record := range batchRecords {
		spendDesc := describeUTXOSpend(spends[idx])

		res, err := s.teranodeBatchRecordResponse(fmt.Sprintf("[freeze][%d][%s]", batchID, spendDesc), record)
		if err != nil {
			// The UDF path ignores TX_NOT_FOUND on freeze (only SPENT is
			// reported below); keep the native path's KEY_NOT_FOUND — the same
			// condition under UPDATE_ONLY — equally silent.
			if !errors.Is(err, errors.ErrTxNotFound) {
				errorsThrown = append(errorsThrown, err)
			}
			continue
		}

		if res.Status == LuaStatusError && res.ErrorCode == LuaErrorCodeSpent {
			// Extract spending data from error message
			hexData := strings.TrimPrefix(res.Message, "SPENT:")
			if spendingData, parseErr := spendpkg.NewSpendingDataFromString(hexData); parseErr == nil {
				errorsThrown = append(errorsThrown, errors.NewStorageError("[freeze][%d][%s] failed to freeze aerospike utxo because it's already SPENT by %v", batchID, spendDesc, spendingData))
			} else {
				errorsThrown = append(errorsThrown, errors.NewStorageError("[freeze][%d][%s] failed to freeze aerospike utxo: %s", batchID, spendDesc, res.Message))
			}
		}
	}

	if len(errorsThrown) > 0 {
		return errors.NewStorageError("[freeze][%d] failed to batch freeze %d aerospike utxos: %v", batchID, len(spends), errorsThrown)
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

	for _, spend := range spends {
		keySource := uaerospike.CalculateKeySource(spend.TxID, spend.Vout, s.utxoBatchSize)

		aeroKey, aErr := aerospike.NewKey(s.namespace, s.setName, keySource)
		if aErr != nil {
			return aErr
		}

		batchRecords = append(batchRecords, s.teranodeBatchRecord(
			batchUDFPolicy, LuaPackage, aeroKey, subOpUnfreeze, "unfreeze",
			s.calculateOffsetForOutput(spend.Vout),
			spend.UTXOHash[:],
		))
	}

	batchID := s.batchID.Add(1)

	batchPolicy := util.GetAerospikeBatchPolicy(tSettings)
	if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		return errors.NewStorageError("[unfreeze][%d] failed to batch unfreeze %d aerospike utxos: %s", batchID, len(spends), err.Error(), err)
	}

	// check the return value of the batch operation
	errorsThrown := make([]error, 0, len(spends))

	for idx, record := range batchRecords {
		spendDesc := describeUTXOSpend(spends[idx])

		res, err := s.teranodeBatchRecordResponse(fmt.Sprintf("[unfreeze][%d][%s]", batchID, spendDesc), record)
		if err != nil {
			errorsThrown = append(errorsThrown, err)
			continue
		}

		if res.Status == LuaStatusError {
			errorsThrown = append(errorsThrown, errors.NewStorageError("[unfreeze][%d][%s] failed to unfreeze aerospike utxo: %s", batchID, spendDesc, res.Message))
		}
	}

	if len(errorsThrown) > 0 {
		return errors.NewStorageError("[unfreeze][%d] failed to batch unfreeze %d aerospike utxos: %v", batchID, len(spends), errorsThrown)
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
	keySource := uaerospike.CalculateKeySource(oldUtxo.TxID, oldUtxo.Vout, s.utxoBatchSize)

	aeroKey, aErr := aerospike.NewKey(s.namespace, s.setName, keySource)
	if aErr != nil {
		return aErr
	}

	batchUDFPolicy := aerospike.NewBatchUDFPolicy()

	batchRecords := []aerospike.BatchRecordIfc{
		s.teranodeBatchRecord(
			batchUDFPolicy, LuaPackage, aeroKey, subOpReassign, "reassign",
			s.calculateOffsetForOutput(oldUtxo.Vout),
			oldUtxo.UTXOHash[:],
			newUtxo.UTXOHash[:],
			int(s.GetBlockHeight()),
			utxo.ReAssignedUtxoSpendableAfterBlocks,
		),
	}

	batchPolicy := util.GetAerospikeBatchPolicy(tSettings)
	if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		return errors.NewStorageError("[reassign][%s] failed to reassign aerospike utxo: %s", describeUTXOSpend(oldUtxo), err.Error(), err)
	}

	// check whether an error was thrown
	res, err := s.teranodeBatchRecordResponse(fmt.Sprintf("[reassign][%s]", describeUTXOSpend(oldUtxo)), batchRecords[0])
	if err != nil {
		return err
	}

	if res.Status == LuaStatusError {
		return errors.NewStorageError("[reassign][%s] failed to reassign aerospike utxo: %s", describeUTXOSpend(oldUtxo), res.Message)
	}

	return nil
}

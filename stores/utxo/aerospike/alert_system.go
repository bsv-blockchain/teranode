// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
// It offers high performance, distributed storage capabilities with support for large-scale
// UTXO sets and complex operations like freezing, reassignment, and batch processing.
//
// # Architecture
//
// The implementation uses a combination of Aerospike Key-Value store and Lua scripts
// for atomic operations. Transactions are stored with the following structure:
//   - Main Record: Contains transaction metadata and up to utxostore_utxoBatchSize UTXOs (default 128)
//   - Pagination Records: Additional records for transactions with more outputs than utxostore_utxoBatchSize (default 128)
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
//   - Multiple records when outputs exceed utxostore_utxoBatchSize
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

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
)

// luaMsgFreezeRecordedOnSpent is the message the freeze UDF returns, with STATUS_OK, when
// it recorded the consensus window on an output that was already spent by a real
// transaction. Keep in sync with MSG_FREEZE_RECORDED_ON_SPENT in teranode.lua.
const luaMsgFreezeRecordedOnSpent = "freeze recorded on spent output"

// FreezeUTXOs records the alert system's freeze on each output: the policy marker — the
// spending-data slot set to FF...FF, only while the output is unspent — and the consensus
// record in the utxoFreezeFrom/Until/Exp bins, which is written whether or not the output
// is spent because it is a property of the outpoint, not of this node's spent-state for
// it (issue #1422). See utxo.Store.FreezeUTXOs for the guarantees.
//
// The operation is performed atomically via a Lua script that:
//   - Verifies the UTXO exists and matches the provided hash
//   - Sets the spending transaction ID to FF...FF if the output is unspent
//   - Records the freeze window and policy-expiry flag per output offset
//
// A repeat freeze for an already-frozen output updates its record; only a repeat asking
// for exactly the record already stored is reported as already frozen.
//
// Parameters:
//   - ctx: Context for cancellation/timeout
//   - spends: Array of UTXOs to freeze, each carrying its window and policy-expiry flag
//
// Returns error if any UTXO:
//   - Doesn't exist
//   - Is already frozen with the same record
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
			int(spend.FreezeFrom),
			int(spend.FreezeUntil),
			spend.FreezePolicyExpires,
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
			// The UDF path ignores TX_NOT_FOUND on freeze; keep the native path's
			// KEY_NOT_FOUND — the same condition under UPDATE_ONLY — equally silent.
			if !errors.Is(err, errors.ErrTxNotFound) {
				errorsThrown = append(errorsThrown, err)
			}
			continue
		}

		if res.Status == LuaStatusError {
			switch res.ErrorCode {
			case LuaErrorCodeTxNotFound:
				// Missing records are a deliberate no-op on freeze, matching the native
				// path's silent KEY_NOT_FOUND above.
			case LuaErrorCodeAlreadyFrozen:
				// A repeat freeze asking for exactly the record already stored. Reported
				// the same way the SQL store reports it, so a duplicate alert reads as
				// NotProcessed on every backend rather than as a change on this one.
				errorsThrown = append(errorsThrown, errors.NewUtxoFrozenError("[freeze][%d][%s] aerospike utxo already frozen with this window", batchID, spendDesc))
			default:
				errorsThrown = append(errorsThrown, errors.NewStorageError("[freeze][%d][%s] failed to freeze aerospike utxo: %s", batchID, spendDesc, res.Message))
			}

			continue
		}

		// The consensus record is a property of the outpoint and is recorded on a spent
		// output too (issue #1422); the UDF says so, and it is worth an operator seeing.
		if res.Message == luaMsgFreezeRecordedOnSpent {
			s.logger.Infof("[freeze][%d][%s] freeze recorded on spent output for heights [%d, %d)", batchID, spendDesc, spends[idx].FreezeFrom, spends[idx].FreezeUntil)
		}
	}

	if len(errorsThrown) > 0 {
		// The first per-record error is the wrapped cause, so callers can classify with
		// errors.Is (already-frozen surfaces as ErrFrozen, as on SQL); the full list still
		// renders in the message.
		return errors.NewStorageError("[freeze][%d] failed to batch freeze %d aerospike utxos: %v", batchID, len(spends), errorsThrown, errorsThrown[0])
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
//   - Clears the recorded enforceAtHeight window, so nothing is inherited by a re-freeze
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
			switch res.ErrorCode {
			case LuaErrorCodeUtxoNotFrozen:
				// Neither the policy marker nor a consensus record: reported the same way
				// the SQL store reports it, so callers classify with errors.Is on both.
				errorsThrown = append(errorsThrown, errors.NewUtxoFrozenError("[unfreeze][%d][%s] aerospike utxo is not frozen", batchID, spendDesc))
			default:
				errorsThrown = append(errorsThrown, errors.NewStorageError("[unfreeze][%d][%s] failed to unfreeze aerospike utxo: %s", batchID, spendDesc, res.Message))
			}
		}
	}

	if len(errorsThrown) > 0 {
		return errors.NewStorageError("[unfreeze][%d] failed to batch unfreeze %d aerospike utxos: %v", batchID, len(spends), errorsThrown, errorsThrown[0])
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

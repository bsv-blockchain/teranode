// //go:build aerospike

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
//   - recordUtxos: Number of UTXOs in this record
//   - spentUtxos: Number of spent UTXOs in this record
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

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
)

// Delete removes a transaction and its associated UTXOs from the store.
// If the transaction doesn't exist, the operation is considered successful.
//
// The operation:
//   - Removes the main transaction record
//   - Does not cascade to external storage
//   - Does not affect pagination records
//
// Note: This is a partial deletion that only removes the main transaction record.
// For complete cleanup, additional steps may be needed:
//   - Remove external transaction data (.tx file)
//   - Remove external output data (.outputs file)
//   - Remove pagination records
//   - Clean up cross-references
//
// Parameters:
//   - ctx: Context for cancellation (currently unused)
//   - hash: Transaction hash to delete
//
// Returns:
//   - nil if deletion successful or record not found
//   - error if deletion fails for other reasons
//
// Examples:
//
//	// Delete a transaction
//	err := store.Delete(ctx, txHash)
//	if err != nil {
//	    if errors.Is(err, aerospike.ErrKeyNotFound) {
//	        // Handle not found case
//	    } else {
//	        // Handle other errors
//	    }
//	}
//
// Metrics:
//   - prometheusUtxoMapDelete: Incremented on successful deletion
//   - prometheusUtxoMapErrors: Incremented on deletion errors
func (s *Store) Delete(_ context.Context, hash *chainhash.Hash) error {
	policy := util.GetAerospikeWritePolicy(s.settings, 0)

	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return errors.NewProcessingError("error in aerospike NewKey", err)
	}

	_, err = s.client.Delete(policy, key)
	if err != nil {
		// if the key is not found, we don't need to delete, it's not there anyway
		if errors.Is(err, aerospike.ErrKeyNotFound) {
			return nil
		}

		if e, ok := err.(*aerospike.AerospikeError); ok {
			prometheusUtxoMapErrors.WithLabelValues("Delete", e.ResultCode.String()).Inc()
		} else {
			prometheusUtxoMapErrors.WithLabelValues("Delete", "unknown").Inc()
		}

		return errors.NewStorageError("error in aerospike delete key", err)
	}

	prometheusUtxoMapDelete.Inc()

	return nil
}

// DeleteComplete removes a transaction and all of its associated records: the
// master record, every pagination (child) record counted by TotalExtraRecs, and
// any external blob(s) (.tx and .outputs). Delete only removes the master record
// (a partial deletion — see Delete), which leaves the pagination records of a
// paginated transaction behind with locked=true; a descendant spending a
// high-numbered output then addresses a surviving locked record and gets
// TX_LOCKED. DeleteComplete leaves nothing behind, so a descendant of any output
// gets the clean missing-parent answer instead.
//
// It is idempotent: an absent master (ErrKeyNotFound), absent child records, and
// absent blobs are all treated as success, so a retry of a partially-completed
// cascade converges to nil.
//
// Ordering and failure semantics: children and blob(s) are removed first, and the
// master is deleted last. The master is the record verifyRecordDeleted reads, so
// deleting it last means any genuine failure in the earlier steps leaves the
// master present and returns an error — the shed unwind then fails closed (inputs
// stay spent, record present + locked, recovered by the unmined reload) rather
// than unspending against a half-deleted record.
func (s *Store) DeleteComplete(ctx context.Context, hash *chainhash.Hash) error {
	policy := aerospike.NewPolicy()

	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return errors.NewProcessingError("error in aerospike NewKey", err)
	}

	// Read TotalExtraRecs from the master to discover how many pagination children exist.
	masterRecord, aErr := s.client.Get(policy, key, fields.TotalExtraRecs.String())
	if aErr != nil {
		// The master is deleted last, so its absence means the cascade already
		// completed (or the tx never existed): nothing left to do.
		if errors.Is(aErr, aerospike.ErrKeyNotFound) {
			return nil
		}

		return errors.NewStorageError("error reading master record for complete delete", aErr)
	}

	childCount := 0
	if masterRecord != nil && masterRecord.Bins != nil {
		if v, ok := masterRecord.Bins[fields.TotalExtraRecs.String()].(int); ok {
			childCount = v
		}
	}

	// 1. Delete pagination children (records 1..childCount).
	if childCount > 0 {
		if err := s.deleteChildRecords(hash, childCount); err != nil {
			return err
		}
	}

	// 2. Delete external blob(s). Del treats a missing blob as success, so this is idempotent.
	if s.externalStore != nil {
		if err := s.externalStore.Del(ctx, hash[:], fileformat.FileTypeTx); err != nil {
			return errors.NewStorageError("error deleting external tx blob for complete delete", err)
		}

		if err := s.externalStore.Del(ctx, hash[:], fileformat.FileTypeOutputs); err != nil {
			return errors.NewStorageError("error deleting external outputs blob for complete delete", err)
		}
	}

	// 3. Delete the master record last, so any earlier failure leaves it present.
	return s.Delete(ctx, hash)
}

// deleteChildRecords batch-deletes the pagination records 1..childCount for a
// transaction. It follows the child-key walk precedent used by
// SetDAHForChildRecordsMulti (children start at index 1). A missing child record
// is not an error — the cascade is idempotent.
func (s *Store) deleteChildRecords(hash *chainhash.Hash, childCount int) error {
	batchRecords := make([]aerospike.BatchRecordIfc, 0, childCount)
	deletePolicy := aerospike.NewBatchDeletePolicy()

	for i := uint32(1); i <= uint32(childCount); i++ { // nolint: gosec // children start at 1
		keySource := uaerospike.CalculateKeySourceInternal(hash, i)

		childKey, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			return errors.NewProcessingError("error creating pagination key %d for complete delete", i, err)
		}

		batchRecords = append(batchRecords, aerospike.NewBatchDelete(deletePolicy, childKey))
	}

	if err := s.batchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords); err != nil {
		return errors.NewStorageError("error batch deleting pagination records for complete delete", err)
	}

	var aggErr error

	for _, br := range batchRecords {
		if recErr := br.BatchRec().Err; recErr != nil {
			// A missing child is not an error — the cascade is idempotent.
			if errors.Is(recErr, aerospike.ErrKeyNotFound) {
				continue
			}

			aggErr = errors.Join(aggErr, recErr)
		}
	}

	return aggErr
}

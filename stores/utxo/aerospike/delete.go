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
	"time"

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

// deleteCompleteChildAttempts and deleteCompleteChildBackoff bound the retry of
// the pagination-child pass. The master is already gone by then, so a failure
// here leaves orphan children that no later DeleteComplete can enumerate — the
// retry is the only in-band chance to finish the job. It shares the caller's
// budget (validator_shedUnwindTimeout on the shed path), so the wait selects on
// ctx.Done() rather than sleeping blind.
const (
	deleteCompleteChildAttempts = 2
	deleteCompleteChildBackoff  = 5 * time.Millisecond
)

// DeleteComplete removes a transaction and all of its associated records: the
// master record, every pagination (child) record counted by TotalExtraRecs, and
// any external blob(s) (.tx and .outputs). Delete only removes the master record
// (a partial deletion — see Delete), which leaves the pagination records of a
// paginated transaction behind with locked=true; a descendant spending a
// high-numbered output then addresses a surviving locked record and gets
// TX_LOCKED. On success DeleteComplete leaves nothing behind, so a descendant of
// any output gets the clean missing-parent answer instead. What a FAILED cascade
// can leave is described under the ordering section below.
//
// It is idempotent in the sense a caller needs: an absent master (ErrKeyNotFound),
// absent child records and absent blobs are all treated as success, so a retry
// never errors on work an earlier attempt already did. It is not a repair — once
// the master is gone the child count is gone with it, so a retry returns nil at
// the master read without reaching any orphan child or blob a failed cascade left
// behind.
//
// Ordering and failure semantics: the MASTER record is deleted FIRST, then the
// pagination children, then the blob(s). The master is the only record that can
// be read back by Get, enumerated by the unmined iterator or lifted into a mining
// template, so it is the first thing that must go — and it is the record
// verifyRecordDeleted reads, so a failure of this first step leaves the whole
// transaction intact (present, Locked, inputs spent), which the shed unwind then
// treats exactly as it always did: fail closed, unmined reload as the backstop.
//
// A failure AFTER the master is gone leaves orphan pagination children (and
// possibly a blob). They are locked, therefore unspendable, and they are no
// longer enumerable, because the child count lived on the master — which is why
// the child pass is retried here (deleteCompleteChildAttempts) rather than left
// to a later DeleteComplete, whose master read would return nil immediately. A
// later create of the same txid adopts them: it writes every record CREATE_ONLY,
// so the master is recreated, the surviving children report KEY_EXISTS, and the
// unmined reload's SetLocked then clears their stale locked bin.
//
// The order children-first was rejected: every failure it can take leaves a
// master whose outputs live on records that no longer exist — a transaction this
// node will mine and then answer TX_NOT_FOUND for on every output above
// utxostore_utxoBatchSize, with nothing in the node to recreate a pagination
// child. Unreachable residue is preferred over wrongly reachable residue.
func (s *Store) DeleteComplete(ctx context.Context, hash *chainhash.Hash) error {
	policy := aerospike.NewPolicy()

	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return errors.NewProcessingError("error in aerospike NewKey", err)
	}

	// Read TotalExtraRecs from the master to discover how many pagination children exist.
	masterRecord, aErr := s.client.Get(policy, key, fields.TotalExtraRecs.String())
	if aErr != nil {
		// Nothing addressable is left: the child count lived on the master, so an
		// absent master means the tx never existed, the cascade completed, or it
		// failed after step 1 and left orphans this call can no longer enumerate.
		// Returning nil keeps the cascade idempotent, and a failed READ leaves the
		// record intact, which is why it is separated from the not-found case.
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

	// 1. Delete the master record first, so any failure from here on leaves nothing
	// that can be read back, enumerated or mined.
	if err := s.Delete(ctx, hash); err != nil {
		return err
	}

	// 2. Delete pagination children (records 1..childCount).
	if childCount > 0 {
		if err := s.deleteChildRecordsWithRetry(ctx, hash, childCount); err != nil {
			return errors.NewStorageError("complete delete removed the master record for %s but the pagination child pass did not complete; some of the %d child record(s) it covers may survive as locked, unspendable orphans until a create of the same transaction adopts them, so a manual repair must walk records 1..%d", hash.String(), childCount, childCount, err)
		}
	}

	// 3. Delete external blob(s). Del treats a missing blob as success, so this is
	// idempotent. One attempt only — an orphan blob carries no UTXO semantics, and a
	// later create of the same transaction tolerates it rather than rewriting it: the
	// external Set in create.go moves on when it gets ErrBlobAlreadyExists, so the
	// surviving blob is kept. Same txid means same bytes, so keeping it is harmless.
	if s.externalStore != nil {
		if err := s.externalStore.Del(ctx, hash[:], fileformat.FileTypeTx); err != nil {
			return errors.NewStorageError("error deleting external tx blob for complete delete", err)
		}

		if err := s.externalStore.Del(ctx, hash[:], fileformat.FileTypeOutputs); err != nil {
			return errors.NewStorageError("error deleting external outputs blob for complete delete", err)
		}
	}

	return nil
}

// deleteChildRecordsWithRetry runs deleteChildRecords up to
// deleteCompleteChildAttempts times. The master is already gone when this runs,
// so this is the last point at which the children can be found by count; the
// wait between attempts selects on ctx.Done() so the caller's budget really cuts
// it short.
func (s *Store) deleteChildRecordsWithRetry(ctx context.Context, hash *chainhash.Hash, childCount int) error {
	var lastErr error

	for attempt := 0; attempt < deleteCompleteChildAttempts; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(deleteCompleteChildBackoff)

			select {
			case <-ctx.Done():
				timer.Stop()

				return ctx.Err()
			case <-timer.C:
			}
		}

		if err := s.deleteChildRecords(hash, childCount); err != nil {
			lastErr = err
			continue
		}

		return nil
	}

	return lastErr
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

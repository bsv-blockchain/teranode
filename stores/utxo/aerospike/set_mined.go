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
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
)

// batchRecordsPool pools slices of aerospike.BatchRecordIfc to reduce allocations
// during SetMinedMulti batch processing.
var batchRecordsPool = sync.Pool{}

// getBatchRecordsSlice returns a slice from the pool or allocates a new one with the given capacity.
func getBatchRecordsSlice(capacity int) *[]aerospike.BatchRecordIfc {
	if v := batchRecordsPool.Get(); v != nil {
		s := v.(*[]aerospike.BatchRecordIfc)
		if cap(*s) >= capacity {
			*s = (*s)[:capacity] // set length to capacity, we'll overwrite all elements
			return s
		}
		// Capacity too small, discard and allocate new
	}
	s := make([]aerospike.BatchRecordIfc, capacity)
	return &s
}

// putBatchRecordsSlice returns a slice to the pool for reuse.
func putBatchRecordsSlice(s *[]aerospike.BatchRecordIfc) {
	if s == nil {
		return
	}
	// Clear the slice to allow GC of the BatchRecordIfc objects
	for i := range *s {
		(*s)[i] = nil
	}
	*s = (*s)[:0]
	batchRecordsPool.Put(s)
}

// getBatchKeysSlice returns a key slice from the Store's per-instance key pool,
// preserving the underlying *aerospike.Key entries so their namespace, setName,
// and digest backing array can be reused across batches. The pool is scoped to
// the Store so that a Key allocated for (ns, set) is never reused with a
// different (ns, set) — Key only exposes SetValue, which recomputes digest from
// the existing setName.
//
// Workload assumption: SetMinedMulti batch sizes are relatively uniform within
// a single Store's lifetime (Store lifetime == application lifetime, ~one Store
// per deployment). Under that assumption the pool's per-Store scoping is the
// right trade-off: a large batch seeds the pool with a large backing array
// that subsequent batches reuse, amortizing allocation. sync.Pool's per-GC
// drainage ages out the slice if the workload permanently shifts to small
// batches. If multi-tenant or highly variable batch sizes become common,
// switch to size-class bucketing.
//
// On return, the slice length equals `capacity`. Entries from a freshly allocated
// slice are nil; the caller must initialize each via aerospike.NewKey before use.
// Entries retained from a prior batch are non-nil and must be reset via SetValue.
func (s *Store) getBatchKeysSlice(capacity int) *[]*aerospike.Key {
	if v := s.batchKeysPool.Get(); v != nil {
		ks := v.(*[]*aerospike.Key)
		if cap(*ks) >= capacity {
			*ks = (*ks)[:capacity]
			return ks
		}
		// Capacity too small; let the old slice GC and allocate fresh.
	}
	ks := make([]*aerospike.Key, capacity)
	return &ks
}

// putBatchKeysSlice returns a key slice to the Store's per-instance pool.
// The Aerospike client has finished using the keys by the time the batch call
// returns, so it is safe to mutate them on the next get.
func (s *Store) putBatchKeysSlice(ks *[]*aerospike.Key) {
	if ks == nil {
		return
	}
	// Retain *Key entries so SetValue can mutate digest in place next time.
	*ks = (*ks)[:cap(*ks)]
	s.batchKeysPool.Put(ks)
}

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

	thisBlockHeight := s.GetBlockHeight() + 1

	if !minedBlockInfo.UnsetMined && s.settings.Aerospike.EnableSetMinedFilterExpressions {
		return s.SetMinedMultiWithExpressions(ctx, hashes, minedBlockInfo)
	}

	// Get batch records slice from pool
	batchRecordsPtr := getBatchRecordsSlice(len(hashes))
	defer putBatchRecordsSlice(batchRecordsPtr)
	batchRecords := *batchRecordsPtr

	// Get the key slice from the per-Store pool. Entries may be nil (fresh slice)
	// or pre-initialized *Key from a previous batch (digest backing reused).
	batchKeysPtr := s.getBatchKeysSlice(len(hashes))
	defer s.putBatchKeysSlice(batchKeysPtr)
	batchKeys := *batchKeysPtr

	// Prepare batch records
	if err := s.prepareBatchRecordsForSetMined(batchRecords, batchKeys, hashes, minedBlockInfo, thisBlockHeight); err != nil {
		return nil, err
	}

	// Execute batch operation
	if err := s.executeBatchOperation(batchRecords); err != nil {
		return nil, err
	}

	// Process batch results
	blockIDs, work, err := s.processBatchResultsForSetMined(ctx, batchRecords, hashes, minedBlockInfo)

	// #1037: clear the lock on the pagination records of successfully-mined
	// external txs. Done here (not inside the result processor) so the processor
	// stays free of follow-up I/O. Runs even when err != nil so a tx mined
	// successfully is fully unlocked despite a sibling failure in the same batch.
	if clearErr := s.applyLockClearWork(ctx, work); clearErr != nil {
		// errors.Join(nil, clearErr) would flatten clearErr into a plain error and drop its code.
		if err == nil {
			err = clearErr
		} else {
			err = errors.Join(err, clearErr)
		}
	}

	return blockIDs, err
}

// prepareBatchRecordsForSetMined populates batch records for the setMined operation.
// Both batchRecords and batchKeys must have len(hashes) capacity. batchKeys may
// contain *Key entries from a previous batch (which are mutated in place via
// SetValue) or nil entries (which are initialized via aerospike.NewKey).
func (s *Store) prepareBatchRecordsForSetMined(batchRecords []aerospike.BatchRecordIfc, batchKeys []*aerospike.Key,
	hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo, thisBlockHeight uint32) error {
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()

	usePackage := LuaPackage
	if s.settings.Aerospike.UseSeparateUDFMinedModule {
		usePackage = LuaPackageMined
	}

	// setMined uses currentBlockHeight only to stamp unmined_since. When a block is invalidated
	// (UnsetMined) the stamp is backdated like MarkTransactionsOnLongestChain(false) does (issue
	// 1768): the parent's deleteAtHeight keeps counting from the original spend, so the pruner's
	// parent-preservation scan must see this tx on its next cycle.
	currentBlockHeight := thisBlockHeight
	if minedBlockInfo.UnsetMined {
		currentBlockHeight = utxo.BackdatedUnminedSince(s.GetBlockHeight(), s.settings.UtxoStore.UnminedTxRetention)
	}

	for idx, hash := range hashes {
		key := batchKeys[idx]
		if key == nil {
			newKey, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
			if err != nil {
				return errors.NewProcessingError("aerospike NewKey error", err)
			}
			batchKeys[idx] = newKey
			key = newKey
		} else {
			// Reuse the previously allocated Key by re-setting its user value.
			// SetValue recomputes the digest in place using the stored setName,
			// avoiding allocation of a new Key struct + digest backing.
			if err := key.SetValue(aerospike.NewValue(hash[:])); err != nil {
				return errors.NewProcessingError("aerospike Key.SetValue error", err)
			}
		}

		batchRecords[idx] = s.teranodeBatchRecord(
			batchUDFPolicy, usePackage, key, subOpSetMined, "setMined",
			minedBlockInfo.BlockID,
			minedBlockInfo.BlockHeight,
			minedBlockInfo.SubtreeIdx,
			currentBlockHeight,
			s.settings.GetUtxoStoreSpentDeleteRetention(),
			minedBlockInfo.OnLongestChain,
			minedBlockInfo.UnsetMined,
		)
	}

	return nil
}

// executeBatchOperation performs the batch operation and increments metrics
func (s *Store) executeBatchOperation(batchRecords []aerospike.BatchRecordIfc) error {
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		return errors.NewStorageError("aerospike BatchOperate error for setMined batch with %d records: %s", len(batchRecords), err.Error(), err)
	}

	prometheusTxMetaAerospikeMapSetMinedBatch.Inc()

	return nil
}

// processBatchResultsForSetMined processes the results of the batch operation.
// It returns the per-tx blockID map and the set of records whose `locked` flag
// must be cleared (#1037). The lock-clearing I/O is performed by the caller
// (SetMinedMulti) so this function stays free of follow-up writes for the
// lock-clear path — keeping it unit-testable without a live client.
func (s *Store) processBatchResultsForSetMined(ctx context.Context, batchRecords []aerospike.BatchRecordIfc,
	hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, lockClearWork, error) {
	var errs error
	okUpdates := 0
	nrErrors := 0
	blockIDs := make(map[chainhash.Hash][]uint32, len(hashes))

	// Collect follow-up actions to execute in batches after the loop
	extraRecords := make([]*chainhash.Hash, 0, len(hashes))
	// Child (pagination) record DAH follow-ups, last state per txid wins.
	var childDAH childDAHFollowUps
	// #1037: pagination/extra records of successfully-mined external txs whose
	// `locked` flag must be cleared (the master-keyed setMined only clears the
	// master). Populated from the setMined response's childCount (= totalExtraRecs).
	var work lockClearWork
	// DAH is relative to the height of the block the tx is mined into
	// (minedBlockInfo.BlockHeight), NOT the store's cached chain tip — the cached
	// tip lags behind the block being validated during catchup/sync, which would
	// stamp the DAH too low and let the pruner delete the record before the
	// retention window elapsed. Computed once for the batch (block height and
	// retention are fixed for the duration of SetMinedMulti). It is only the fallback
	// for the child/pagination records: the setMined UDF floors the master stamp at its
	// highest mined height, so the master's reported DAH is used when present.
	dahHeight, dahEnabled := s.deleteAtHeightFor(minedBlockInfo.BlockHeight)

	// Reuse a single LuaMapResponse across all per-record parses in this batch.
	// The struct's fields (status, signal, BlockIDs cap, Errors map buckets) are
	// preserved across iterations via Reset, eliminating per-record allocation.
	pooledRes := getLuaMapResponse()
	defer putLuaMapResponse(pooledRes)

	for idx, batchRecord := range batchRecords {
		pooledRes.Reset()
		result, res, err := s.processSingleBatchRecordPooled(ctx, batchRecord, hashes[idx], minedBlockInfo, pooledRes)
		if err != nil {
			if errs == nil {
				errs = err
			} else {
				errs = errors.Join(errs, err)
			}

			nrErrors++
		} else if result {
			okUpdates++

			// #1037: a freshly-mined external tx may still have `locked` set on its
			// pagination records. Clear them (the master's lock is cleared by the
			// UDF itself). childCount == totalExtraRecs is surfaced by setMined on a
			// mine; UnsetMined never reaches here as a clear (childCount only on mine).
			if res != nil && hashes[idx] != nil && !minedBlockInfo.UnsetMined && res.ChildCount > 0 {
				work.items = append(work.items, lockClearItem{txID: hashes[idx], childCount: res.ChildCount})
			}
		}

		if res != nil && res.BlockIDs != nil {
			// The caller-owned map retains ownership of this slice across calls,
			// so a fresh allocation is required here.
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

			if hashes[idx] == nil {
				errs = errors.Join(errs, errors.NewProcessingError("aerospike SetMinedMulti returned blockIDs for nil hash at index %d", idx))
				nrErrors++

				continue
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
				if dahEnabled {
					childDAHHeight := dahHeight
					if reported, ok := reportedDeleteAtHeight(res); ok {
						childDAHHeight = reported
					}

					childDAH.addSet(hashes[idx], res.ChildCount, childDAHHeight, reportedDAHVersion(res))
					// External store DAH is disabled - lifecycle managed by pruner service
				}
			case LuaSignalDAHUnset:
				childDAH.addUnset(hashes[idx], res.ChildCount, reportedDAHVersion(res))
				// External store DAH is disabled - lifecycle managed by pruner service
			}
		}
	}

	prometheusTxMetaAerospikeMapSetMinedBatchN.Add(float64(okUpdates))

	// Execute aggregated follow-ups in batches. These run BEFORE the sibling-error return
	// below: the records that did succeed have already been mined, and a retry would hit the
	// "block already present" no-op and never reconcile their child-record DAH.
	var postErr error

	if len(extraRecords) > 0 {
		// Assigned, not joined: errors.Join(nil, err) would drop the error's storage code.
		postErr = s.IncrementSpentRecordsMulti(ctx, extraRecords, 1, minedBlockInfo.BlockHeight)
	}

	applied := s.applyChildDAHFollowUps(ctx, &childDAH)
	postErr = errors.JoinCapped(10, postErr, applied.unsetErr, applied.setErr)

	// External store DAH is disabled - lifecycle managed by pruner service
	// setDAHExternalTransactionMulti removed as it would no-op

	if errs != nil || nrErrors > 0 {
		if postErr != nil {
			errs = errors.Join(errs, postErr)
		}

		prometheusTxMetaAerospikeMapSetMinedBatchErrN.Add(float64(nrErrors))
		return blockIDs, work, errors.NewError("aerospike batchRecord errors", errs)
	}

	if postErr != nil {
		return blockIDs, work, errors.NewError("aerospike setMined follow-up batch errors", postErr)
	}

	return blockIDs, work, nil
}

// processSingleBatchRecord processes a single batch record result, allocating a
// fresh LuaMapResponse. Retained for callers outside the hot path; SetMinedMulti
// uses processSingleBatchRecordPooled to share a pooled response.
func (s *Store) processSingleBatchRecord(ctx context.Context, batchRecord aerospike.BatchRecordIfc, hash *chainhash.Hash,
	minedBlockInfo utxo.MinedBlockInfo) (bool, *LuaMapResponse, error) {
	res := &LuaMapResponse{}
	ok, returnedRes, err := s.processSingleBatchRecordPooled(ctx, batchRecord, hash, minedBlockInfo, res)
	if returnedRes == nil {
		return ok, nil, err
	}
	return ok, returnedRes, err
}

// processSingleBatchRecordPooled is the pool-friendly variant of
// processSingleBatchRecord. The caller supplies a freshly-reset *LuaMapResponse
// in `res`. The function returns either nil (parse not performed, e.g. on batch
// error) or `res` itself. Callers must not retain the returned *LuaMapResponse
// past the next Reset/Put of `res`.
func (s *Store) processSingleBatchRecordPooled(ctx context.Context, batchRecord aerospike.BatchRecordIfc, hash *chainhash.Hash,
	minedBlockInfo utxo.MinedBlockInfo, res *LuaMapResponse) (bool, *LuaMapResponse, error) {
	if batchRecord == nil {
		return false, nil, errors.NewError("missing setMined batch record for transaction %s; %s", describeChainHash(hash), describeAerospikeBatchRecord(batchRecord))
	}

	batchRec := batchRecord.BatchRec()
	if batchRec == nil {
		return false, nil, errors.NewError("missing setMined batch record for transaction %s; %s", describeChainHash(hash), describeAerospikeBatchRecord(batchRecord))
	}

	batchErr := batchRec.Err
	if batchErr != nil {
		return false, nil, s.handleBatchRecordError(batchRecord, batchErr, hash, minedBlockInfo.UnsetMined)
	}

	response := batchRec.Record
	if response == nil || response.Bins == nil || response.Bins[LuaSuccess.String()] == nil {
		return false, nil, errors.NewError("missing %q bin in aerospike setMined response for transaction %s; %s", LuaSuccess.String(), describeChainHash(hash), describeAerospikeBatchRecord(batchRecord))
	}

	rawResponse := response.Bins[LuaSuccess.String()]
	if parseErr := s.parseLuaMapResponseInto(rawResponse, res); parseErr != nil {
		return false, nil, errors.NewError("aerospike setMined batchRecord %s ParseLuaMapResponse error for bin %q (value %s); %s: %s", describeChainHash(hash), LuaSuccess.String(), describeAerospikeValue(rawResponse), describeAerospikeBatchRecord(batchRecord), parseErr.Error(), parseErr)
	}

	if res.Status != LuaStatusOK {
		if res.ErrorCode == LuaErrorCodeTxNotFound {
			if minedBlockInfo.UnsetMined {
				// This is not an error for us, just a no-op, if we are unsetting mined status on a tx that does not exist
				return true, res, nil
			}

			return false, res, errors.NewTxNotFoundError("transaction not found: %s", describeChainHash(hash))
		}

		return false, res, errors.NewError("aerospike batchRecord %s error: %s", describeChainHash(hash), res.Message)
	}

	return true, res, nil
}

// handleBatchRecordError handles errors from batch records.
// For unset-mined operations a missing record is a no-op (the tx is already gone).
// For normal set-mined a missing record is a hard error: the txmeta must exist and
// be tagged with the block ID, otherwise the mined-invariant is violated.
func (s *Store) handleBatchRecordError(batchRecord aerospike.BatchRecordIfc, err error, hash *chainhash.Hash, unsetMined bool) error {
	var aErr *aerospike.AerospikeError
	if errors.As(err, &aErr) && aErr != nil && aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
		if unsetMined {
			return nil
		}
		return errors.NewTxNotFoundError("transaction not found: %s", describeChainHash(hash))
	}
	s.demoteNativeOnUnsupported(err)
	return errors.NewStorageError("aerospike setMined batchRecord error for transaction %s; %s: %s", describeChainHash(hash), describeAerospikeBatchRecord(batchRecord), err.Error(), err)
}

// lockClearItem identifies a transaction whose pagination/extra records must
// have the `locked` flag and the `creating` bin cleared as part of marking the
// transaction mined. When the setMined UDF or expression write runs, it clears
// the master's `locked` and `creating` itself. On the expression path's
// FILTERED_OUT branch that write is skipped, so an item whose master still has
// `creating` sets master, and the master loses it here once its extra records
// are cleared. Its `locked` is left to SetLocked(false).
type lockClearItem struct {
	txID       *chainhash.Hash
	childCount int  // number of pagination/extra records, cleared at indices 1..childCount
	master     bool // also delete creating on the master record, index 0
}

// lockClearWork is the lock-clearing follow-up that a setMined result processor
// hands back to its public caller (#1037). The processors stay free of follow-up
// I/O so they remain unit-testable without a live client.
type lockClearWork struct {
	// items: pagination records to unlock directly (childCount known from the
	// setMined response / totalExtraRecs bin; the master is already unlocked).
	items []lockClearItem
	// fullUnlock: transactions whose record state is unknown from the batch (the
	// expression write was FILTERED_OUT, so the child count and even the master's
	// lock state were not returned). applyLockClearWork reads both and deletes
	// `creating` on every extra record. It deletes it on the master only when
	// every extra record was cleared. SetLocked(false) clears `locked` on every
	// record.
	fullUnlock []chainhash.Hash
}

// applyLockClearWork performs the lock-clearing follow-up I/O collected by a
// setMined result processor. Safe to call with empty work (no-op).
func (s *Store) applyLockClearWork(ctx context.Context, work lockClearWork) error {
	// Assign the first non-nil error directly rather than errors.Join(nil, err),
	// which would wrap it in a stdlib joinError and drop the rich *errors.Error type.
	var err error

	items := work.items

	// SetLocked below clears `locked` on the fullUnlock transactions but never
	// touches `creating`, so their records also go through the clear here. The
	// FILTERED_OUT write returned neither the child count nor whether the master
	// still has `creating`, so read both. A transaction whose read fails gets no
	// item, and its master keeps `creating`.
	if len(work.fullUnlock) > 0 {
		extraItems, readErr := s.readExtraRecordCounts(work.fullUnlock)
		if readErr != nil {
			err = readErr
		}

		items = append(items, extraItems...)
	}

	if clearErr := s.clearLockedOnRecordsMulti(items); clearErr != nil {
		if err == nil {
			err = clearErr
		} else {
			err = errors.Join(err, clearErr)
		}
	}

	if len(work.fullUnlock) > 0 {
		if unlockErr := s.SetLocked(ctx, work.fullUnlock, false); unlockErr != nil {
			// SetLocked reports a failed batch as a ProcessingError; here it is a cleanup fault of this
			// node's store, so give it the storage code that keeps the block for a retry.
			unlockErr = errors.NewStorageError("[applyLockClearWork] failed to unlock %d FILTERED_OUT txs", len(work.fullUnlock), unlockErr)

			if err == nil {
				err = unlockErr
			} else {
				err = errors.Join(err, unlockErr)
			}
		}
	}

	return err
}

// readExtraRecordCounts reads the extra record count and the `creating` bin from
// the master record of each transaction, and returns a lockClearItem for each one
// with extra records or with `creating` on its master. A master that no longer
// exists has nothing to clear.
func (s *Store) readExtraRecordCounts(txIDs []chainhash.Hash) ([]lockClearItem, error) {
	readPolicy := aerospike.NewBatchReadPolicy()
	records := make([]aerospike.BatchRecordIfc, 0, len(txIDs))
	ids := make([]*chainhash.Hash, 0, len(txIDs))

	for i := range txIDs {
		txID := &txIDs[i]

		key, err := aerospike.NewKey(s.namespace, s.setName, txID[:])
		if err != nil {
			return nil, errors.NewProcessingError("[readExtraRecordCounts][%s] failed to create key", txID.String(), err)
		}

		records = append(records, aerospike.NewBatchRead(readPolicy, key, []string{fields.TotalExtraRecs.String(), fields.Creating.String()}))
		ids = append(ids, txID)
	}

	if err := s.batchOperate(util.GetAerospikeBatchPolicy(s.settings), records); err != nil {
		return nil, errors.NewStorageError("[readExtraRecordCounts] failed to read the extra record count of %d txs", len(ids), err)
	}

	return extraRecordItems(records, ids)
}

// extraRecordItems turns the batch read of readExtraRecordCounts into one
// lockClearItem per transaction with extra records or a `creating` master. A master that comes back
// KEY_NOT_FOUND is skipped. Any other read error, or a record that came back
// with neither data nor an error, is reported, so a transaction whose extra
// records were not cleared is never dropped silently.
func extraRecordItems(records []aerospike.BatchRecordIfc, ids []*chainhash.Hash) ([]lockClearItem, error) {
	var (
		items   []lockClearItem
		readErr []error
	)

	for i, rec := range records {
		br := rec.BatchRec()

		switch {
		case br.Err != nil && isKeyNotFound(br.Err):
			continue
		case br.Err != nil:
			readErr = append(readErr, errors.NewStorageError("[readExtraRecordCounts][%s] failed to read the extra record count", ids[i].String(), br.Err))
			continue
		case br.Record == nil:
			readErr = append(readErr, errors.NewStorageError("[readExtraRecordCounts][%s] no record and no error returned for the extra record count", ids[i].String()))
			continue
		}

		extraRecs, _ := br.Record.Bins[fields.TotalExtraRecs.String()].(int)
		_, masterCreating := br.Record.Bins[fields.Creating.String()]

		if extraRecs > 0 || masterCreating {
			items = append(items, lockClearItem{txID: ids[i], childCount: max(extraRecs, 0), master: masterCreating})
		}
	}

	return items, errors.JoinCapped(10, readErr...)
}

// clearLockedOnRecordsMulti clears the `locked` flag and deletes the `creating`
// bin on the extra records of each item, then deletes `creating` on the master
// of each item that sets master.
//
// Why this exists (issue #1037): a transaction created WithLocked(true) — by the
// block-validation quick-validate pipeline (services/blockvalidation/quick_validate.go)
// or the validator's 2-phase commit — has the `locked` flag set on EVERY record:
// the master record AND every pagination/extra record of an external (paginated)
// transaction. The mined-status update (the setMined UDF and the expression batch)
// is keyed on the master record only, so it clears the master's lock but never the
// pagination records'. If the 2PC / quick-validate unlock then fails to run (its
// documented "locked UTXOs remain ... rolled back on error" path), the pagination
// records stay locked forever. Because mining a transaction makes ALL of its
// outputs spendable, SetMinedMulti must clear the lock on every record — otherwise
// a child spending an output that lives on a pagination record (vout >=
// utxoBatchSize) fails permanently with TX_LOCKED, wedging legacy sync.
//
// The `creating` bin has the same gap. Create sets it on every record and
// clearCreatingFlag clears the extra records in one batch and the master last,
// and skips the master if any extra record fails, which leaves `creating` on the
// master and on each extra record that failed. setMined then deletes it from the
// master only, so the master looks complete and nothing re-runs the create,
// while every output on a stuck extra record is refused with CREATING. Deleting
// the bin here, in the same write as the lock clear, makes those outputs
// spendable once the transaction is mined. Create writes each record with its
// full data in one put and this write is UPDATE_ONLY, so a cleared extra record
// is never half-written. A record the create never wrote stays missing and comes
// back KEY_NOT_FOUND below; this clear does not repair it.
//
// A master without `creating` marks its transaction complete, so the master
// item follows clearCreatingFlag's order: it is written in a second batch, and
// only when every extra record of its transaction came back cleared. A failed or
// missing extra record leaves `creating` on the master.
//
// A KEY_NOT_FOUND is not an error: a record that does not exist cannot be holding
// a lock. On an extra record it still withholds the master clear.
func (s *Store) clearLockedOnRecordsMulti(items []lockClearItem) error {
	batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	clearLockedOp := aerospike.PutOp(aerospike.NewBin(fields.Locked.String(), false))
	// A nil value deletes the bin, matching clearCreatingFlag and setMined.
	clearCreatingOp := aerospike.PutOp(aerospike.NewBin(fields.Creating.String(), nil))

	recordKey := func(txID *chainhash.Hash, idx uint32) (*aerospike.Key, error) {
		key, err := aerospike.NewKey(s.namespace, s.setName, uaerospike.CalculateKeySourceInternal(txID, idx))
		if err != nil {
			return nil, errors.NewProcessingError("[clearLockedOnRecordsMulti][%s] failed to create key for record %d", txID.String(), idx, err)
		}

		return key, nil
	}

	// childRef names the item and record index of each extra record write, so a
	// failure is reported against them and withholds that item's master clear.
	type childRef struct {
		item int
		idx  uint32
	}

	var (
		childRecords []aerospike.BatchRecordIfc
		childRefs    []childRef
	)

	for n, it := range items {
		// Guard against a non-positive childCount: uint32(negative) would wrap to
		// ~4e9 and blow up the batch.
		if it.childCount <= 0 {
			continue
		}

		for i := uint32(1); i <= uint32(it.childCount); i++ { // nolint:gosec
			key, err := recordKey(it.txID, i)
			if err != nil {
				return err
			}

			childRecords = append(childRecords, aerospike.NewBatchWrite(batchWritePolicy, key, clearLockedOp, clearCreatingOp))
			childRefs = append(childRefs, childRef{item: n, idx: i})
		}
	}

	var (
		failed    []failedClear
		otherErrs []error
	)

	incomplete := make([]bool, len(items))

	if len(childRecords) > 0 {
		// A non-nil top-level error here is a transport/batch-level failure, surfaced
		// hard, and no master is cleared. The benign per-record KEY_NOT_FOUND tolerance
		// below relies on Aerospike reporting missing records per-record
		// (BatchRecord.Err), not as a top-level error.
		if err := s.batchOperate(util.GetAerospikeBatchPolicy(s.settings), childRecords); err != nil {
			return errors.NewStorageError("[clearLockedOnRecordsMulti] failed to clear locked flag", err)
		}

		for i, br := range childRecords {
			recErr := br.BatchRec().Err
			if recErr == nil {
				continue
			}

			ref := childRefs[i]
			incomplete[ref.item] = true

			if isKeyNotFound(recErr) {
				if items[ref.item].master {
					s.logger.Warnf("[clearLockedOnRecordsMulti][%s] extra record %d is missing, so the master keeps its creating bin", items[ref.item].txID.String(), ref.idx)
				}

				continue
			}

			failed = append(failed, failedClear{txID: items[ref.item].txID, idx: ref.idx, cause: recErr})
		}
	}

	// The master write deletes only `creating`, and only when the bin is there, so
	// a master without it is not rewritten and comes back FILTERED_OUT.
	masterWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	masterWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	masterWritePolicy.FilterExpression = aerospike.ExpBinExists(fields.Creating.String())

	var (
		masterRecords []aerospike.BatchRecordIfc
		masterItems   []int
	)

	for n, it := range items {
		if !it.master || incomplete[n] {
			continue
		}

		key, err := recordKey(it.txID, 0)
		if err != nil {
			otherErrs = append(otherErrs, err)
			continue
		}

		masterRecords = append(masterRecords, aerospike.NewBatchWrite(masterWritePolicy, key, clearCreatingOp))
		masterItems = append(masterItems, n)
	}

	if len(masterRecords) > 0 {
		if err := s.batchOperate(util.GetAerospikeBatchPolicy(s.settings), masterRecords); err != nil {
			otherErrs = append(otherErrs, errors.NewStorageError("[clearLockedOnRecordsMulti] failed to clear creating on %d master records", len(masterRecords), err))
		} else {
			for i, br := range masterRecords {
				recErr := br.BatchRec().Err

				// A FILTERED_OUT master has no `creating` bin to delete.
				var aErr *aerospike.AerospikeError
				if recErr == nil || (errors.As(recErr, &aErr) && aErr != nil && aErr.Matches(types.KEY_NOT_FOUND_ERROR, types.FILTERED_OUT)) {
					continue
				}

				failed = append(failed, failedClear{txID: items[masterItems[i]].txID, idx: 0, cause: recErr})
			}
		}
	}

	errs := otherErrs
	if len(failed) > 0 {
		errs = append([]error{s.failedClearsError(failed)}, otherErrs...)
	}

	if len(errs) == 0 {
		return nil
	}

	return errors.Join(errs...)
}

// failedClear is one record whose clear came back with an error.
type failedClear struct {
	txID  *chainhash.Hash
	idx   uint32
	cause error
}

// maxFailedClearTxs bounds how many transactions a clear error names, so a
// cluster-wide failure cannot build a multi-megabyte error string.
const maxFailedClearTxs = 100

// failedClearsError names the failed records in one message: one entry per
// transaction, its record indexes as ranges per result code. Error() prints at
// most 20 links of a chain, so a link per record would lose names once a caller
// joins other errors in front. Past maxFailedClearTxs transactions the error
// gives a count, and the remaining entries are logged as warnings.
func (s *Store) failedClearsError(failed []failedClear) error {
	type reasonIdxs struct {
		reason string
		idxs   []uint32
	}

	var order []*chainhash.Hash

	byTx := make(map[chainhash.Hash][]*reasonIdxs)

	for _, f := range failed {
		groups, seen := byTx[*f.txID]
		if !seen {
			order = append(order, f.txID)
		}

		reason := clearFailureReason(f.cause)

		var group *reasonIdxs

		for _, g := range groups {
			if g.reason == reason {
				group = g
				break
			}
		}

		if group == nil {
			group = &reasonIdxs{reason: reason}
			byTx[*f.txID] = append(groups, group)
		}

		group.idxs = append(group.idxs, f.idx)
	}

	entries := make([]string, 0, len(order))

	for _, txID := range order {
		parts := make([]string, 0, len(byTx[*txID]))
		for _, g := range byTx[*txID] {
			parts = append(parts, indexRanges(g.idxs)+" ("+g.reason+")")
		}

		entries = append(entries, txID.String()+" records "+strings.Join(parts, ", "))
	}

	if len(entries) <= maxFailedClearTxs {
		return errors.NewStorageError("[clearLockedOnRecordsMulti] failed to clear %d records: %s", len(failed), strings.Join(entries, "; "))
	}

	rest := entries[maxFailedClearTxs:]
	for start := 0; start < len(rest); start += maxFailedClearTxs {
		s.logger.Warnf("[clearLockedOnRecordsMulti] failed clears not named in the returned error: %s", strings.Join(rest[start:min(start+maxFailedClearTxs, len(rest))], "; "))
	}

	return errors.NewStorageError("[clearLockedOnRecordsMulti] failed to clear %d records: %s; and %d more transactions, logged as warnings", len(failed), strings.Join(entries[:maxFailedClearTxs], "; "), len(rest))
}

// indexRanges writes ascending record indexes as ranges: 1-3,5.
func indexRanges(idxs []uint32) string {
	sorted := slices.Clone(idxs)
	slices.Sort(sorted)

	var b strings.Builder

	for i := 0; i < len(sorted); {
		j := i
		for j+1 < len(sorted) && sorted[j+1] == sorted[j]+1 {
			j++
		}

		if b.Len() > 0 {
			b.WriteString(",")
		}

		b.WriteString(strconv.FormatUint(uint64(sorted[i]), 10))

		if j > i {
			b.WriteString("-")
			b.WriteString(strconv.FormatUint(uint64(sorted[j]), 10))
		}

		i = j + 1
	}

	return b.String()
}

// clearFailureReason is the Aerospike result code of a failed clear, or the
// error text when it is not an Aerospike error.
func clearFailureReason(err error) string {
	var aErr *aerospike.AerospikeError
	if errors.As(err, &aErr) && aErr != nil {
		return "ResultCode: " + aErr.ResultCode.String()
	}

	return err.Error()
}

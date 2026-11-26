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

	// Roll forward any still-"creating" record being mined, on every call and whatever
	// UseCreateFirstOrder says: the consumer-side guards are unconditional, so turning the
	// flag off with creating records on disk must not strand them. The roll-forward spends the
	// inputs and clears the flag, so the outputs become spendable on the normal mined path.
	//
	// Correctness rests on this pre-flight, which fails closed, not on the write paths
	// preserving the creating bin. There are three of them: the Lua write, the filter-expression
	// write, and the native sub-op (subOpSetMined, dispatched to the out-of-tree server module
	// by useNativeForSubOp). The first two preserve the bin; how the native path treats it is
	// unknown from this repo, which is why the pre-flight runs before it on every call.
	// Block validation re-runs setMined for the block if the roll-forward cannot complete.
	//
	// blockHeight is the height of the block being marked mined (not the store's cached
	// tip+1), which is the correct input for the coinbase-maturity/DAH arithmetic in Spend
	// when the store's height lags during catchup.
	if !minedBlockInfo.UnsetMined {
		if err := s.rollForwardCreatingBeforeSetMined(ctx, hashes, minedBlockInfo.BlockHeight, minedBlockInfo.OnLongestChain); err != nil {
			return nil, err
		}
	}

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
		err = errors.Join(err, clearErr)
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
		if err := s.IncrementSpentRecordsMulti(ctx, extraRecords, 1, minedBlockInfo.BlockHeight); err != nil {
			postErr = errors.Join(postErr, err)
		}
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
// FILTERED_OUT branch the master's `locked` is cleared by SetLocked(false), and
// its `creating` is not cleared.
type lockClearItem struct {
	txID       *chainhash.Hash
	childCount int // number of pagination/extra records, cleared at indices 1..childCount
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
	// lock state were not returned). SetLocked(false) reads the child count from
	// the master and clears every record (master + all pagination records).
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
	// touches `creating`, so their extra records also go through the clear here.
	// Their child count was not returned by the FILTERED_OUT write, so read it.
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
			if err == nil {
				err = unlockErr
			} else {
				err = errors.Join(err, unlockErr)
			}
		}
	}

	return err
}

// readExtraRecordCounts reads the extra record count from the master record of
// each transaction and returns a lockClearItem for each one that has extra
// records. A master that no longer exists has no extra records to clear.
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

		records = append(records, aerospike.NewBatchRead(readPolicy, key, []string{fields.TotalExtraRecs.String()}))
		ids = append(ids, txID)
	}

	if err := s.batchOperate(util.GetAerospikeBatchPolicy(s.settings), records); err != nil {
		return nil, errors.NewStorageError("[readExtraRecordCounts] failed to read the extra record count of %d txs", len(ids), err)
	}

	return extraRecordItems(records, ids)
}

// extraRecordItems turns the batch read of readExtraRecordCounts into one
// lockClearItem per transaction with extra records. A master that comes back
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

		if extraRecs, ok := br.Record.Bins[fields.TotalExtraRecs.String()].(int); ok && extraRecs > 0 {
			items = append(items, lockClearItem{txID: ids[i], childCount: extraRecs})
		}
	}

	return items, errors.JoinCapped(10, readErr...)
}

// clearLockedOnRecordsMulti clears the `locked` flag and deletes the `creating`
// bin on the requested records in a single unfiltered BatchOperate.
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
// spendable once the transaction is mined. Deleting the bin never exposes a
// half-written record, since create writes each record with its full data in one
// put and this write is UPDATE_ONLY. A record the create never wrote stays
// missing and comes back KEY_NOT_FOUND below; this clear does not repair it.
//
// A KEY_NOT_FOUND on any record is treated as benign: a record that does not exist
// cannot be holding a lock.
func (s *Store) clearLockedOnRecordsMulti(items []lockClearItem) error {
	total := 0
	for _, it := range items {
		if it.childCount > 0 {
			total += it.childCount
		}
	}

	if total == 0 {
		return nil
	}

	batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	clearLockedOp := aerospike.PutOp(aerospike.NewBin(fields.Locked.String(), false))
	// A nil value deletes the bin, matching clearCreatingFlag and setMined.
	clearCreatingOp := aerospike.PutOp(aerospike.NewBin(fields.Creating.String(), nil))

	batchRecords := make([]aerospike.BatchRecordIfc, 0, total)

	appendRecord := func(txID *chainhash.Hash, idx uint32) error {
		keySource := uaerospike.CalculateKeySourceInternal(txID, idx)

		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			return errors.NewProcessingError("[clearLockedOnRecordsMulti][%s] failed to create key for record %d", txID.String(), idx, err)
		}

		batchRecords = append(batchRecords, aerospike.NewBatchWrite(batchWritePolicy, key, clearLockedOp, clearCreatingOp))

		return nil
	}

	for _, it := range items {
		// Guard against a non-positive childCount: uint32(negative) would wrap to
		// ~4e9 and blow up the batch. Both producers only ever append childCount > 0,
		// so this is belt-and-suspenders for self-consistency with the sizing loop.
		if it.childCount <= 0 {
			continue
		}

		for i := uint32(1); i <= uint32(it.childCount); i++ { // nolint:gosec
			if err := appendRecord(it.txID, i); err != nil {
				return err
			}
		}
	}

	// A non-nil top-level error here is a transport/batch-level failure, surfaced
	// hard. The benign per-record KEY_NOT_FOUND tolerance below relies on Aerospike
	// reporting missing records per-record (BatchRecord.Err), not as a top-level
	// error — consistent with the rest of this package's batch handling.
	if err := s.batchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords); err != nil {
		return errors.NewStorageError("[clearLockedOnRecordsMulti] failed to clear locked flag", err)
	}

	var aggErr error

	for _, br := range batchRecords {
		recErr := br.BatchRec().Err
		if recErr == nil {
			continue
		}

		// A missing record cannot be holding a lock — tolerate it.
		var aErr *aerospike.AerospikeError
		if errors.As(recErr, &aErr) && aErr != nil && aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
			continue
		}

		aggErr = errors.Join(aggErr, recErr)
	}

	return aggErr
}

// rollForwardCreatingBeforeSetMined completes any still-"creating" record among the
// to-be-mined hashes BEFORE setMined can clear the creating/unminedSince selectors the
// pruner sweeper relies on. setMined clears unminedSince on the longest chain, and the
// native write path's handling of the creating bin is unknown, so once a write has run a
// mid-flight record may never be found again — a tx whose inputs were never spent would be
// mined and finalized, its parents' outputs left permanently unspent (the #1214 shape). Running the roll-forward first, and FAILING CLOSED on any
// record it cannot complete, keeps every crash point safe: before finalize the selectors
// are intact and the sweeper still finds the record; after finalize the record is a
// correctly-finalized unmined tx and the next block-validation pass re-runs setMined.
//
// The pre-flight does per-tx work only for the few records that are actually mid-flight:
// creating records are rare and transient. Its fixed cost is one batch read of the creating
// state of every hash of the call, before the write. It pays that on every call, whatever
// utxostore_useCreateFirstOrder says, because the healers are not gated on the setting. The
// read is not negligible beside the mined write itself, most of all on the Lua write path;
// BenchmarkSetMinedMulti measures the whole call.
func (s *Store) rollForwardCreatingBeforeSetMined(ctx context.Context, hashes []*chainhash.Hash, blockHeight uint32, onLongestChain bool) error {
	_, err := s.rollForwardCreatingRecords(ctx, hashes, blockHeight, false, onLongestChain)

	return err
}

// rollForwardCreatingRecords is the pre-flight rule for any write that clears unminedSince of a
// transaction that is mined: it completes the creating records among hashes. With tolerant false it
// stops at the first record it cannot complete and returns that error. With tolerant true it goes on,
// and returns the error of each record it could not complete, by hash, so the caller leaves those
// records as they are and still handles the rest. A record whose state could not be read is one of
// them: it is not known to be complete. An error that is not about one record, a failed clear of the
// tokenless records, is returned as the second result in both modes.
//
// The read of the creating state covers every hash, so it is as wide as the write it comes before.
// It runs in batches of utxostore_maxMinedBatchSize on up to utxostore_maxMinedRoutines workers, and
// reads three bins into records that are reused from batch to batch (readCreatingRecords). Only the
// records it finds creating, which are few, cost a read of their own.
//
// blockHeight is the height the caller names for the roll-forward: the height of the block that is
// being marked mined, or the tip of the store. It is the least a record is rolled forward at. A
// record that carries the height of a block it is in is rolled forward at that height when it is
// higher, so that a tip that lags the block does not make a mature coinbase parent look immature
// or stamp a parent for deletion too early.
//
// onLongestChain says that the caller is marking the transactions as mined on the longest chain. A
// creating record among them is then the record of a transaction that is in a best-chain block. It
// is stamped with the markOwed bin before it is rolled forward (stampMarkOwed), so that no rollback
// can claim it from then on and the sweep does not announce it to block assembly: a record that
// this call cannot complete is left for the sweep, and a rollback of it would free outputs that a
// mined transaction spent.
func (s *Store) rollForwardCreatingRecords(ctx context.Context, hashes []*chainhash.Hash, blockHeight uint32, tolerant, onLongestChain bool) (map[chainhash.Hash]error, error) {
	failed := make(map[chainhash.Hash]error)

	creating, unread := s.readCreatingRecords(ctx, hashes)

	for hash, readErr := range unread {
		wrapped := errors.NewProcessingError("[setMined] create-first pre-flight: read of the creating state of %s failed", hash.String(), readErr)

		if !tolerant {
			return failed, wrapped
		}

		failed[hash] = wrapped
	}

	var (
		legacy    []*chainhash.Hash
		tentative []*creatingRecord
	)

	for i := range creating {
		rec := &creating[i]

		// A creating record with no attempt token was not written by a create-first attempt. It
		// is a record whose clearCreatingFlag failed, and the write that made it did not defer
		// the spend of its inputs. There is nothing to roll forward: mining clears the bin.
		if len(rec.attempt) == 0 && !rec.rollingBack {
			legacy = append(legacy, &rec.hash)

			continue
		}

		tentative = append(tentative, rec)
	}

	if onLongestChain {
		if stampErr := s.stampMarkOwed(tentative); stampErr != nil {
			// The records are rolled forward all the same. One that is not completed is then open to
			// a rollback, as it was before the stamp existed, so the failure is not hidden.
			s.logger.Errorf("[setMined] create-first pre-flight: the creating records of transactions on the longest chain could not be stamped, a rollback can still claim one that is not completed: %v", stampErr)
		}
	}

	for _, rec := range tentative {
		if rfErr := s.rollForwardCreatingRecord(ctx, rec, blockHeight); rfErr != nil {
			if !tolerant {
				return failed, rfErr
			}

			failed[rec.hash] = rfErr
		}
	}

	return failed, s.clearCreatingWithoutAttempt(legacy)
}

// creatingRecord is a master record that the pre-flight read found with the creating bin.
type creatingRecord struct {
	hash        chainhash.Hash
	attempt     []byte
	rollingBack bool
}

// creatingReadBins are the bins the pre-flight reads from every record.
var creatingReadBins = []string{fields.Creating.String(), fields.RollingBack.String(), fields.Attempt.String()}

// readCreatingRecords reads the creating state of every hash and returns the records that carry the
// creating bin, and the error of each hash whose state it could not read. A record that does not
// exist is neither: there is nothing to roll forward. A read that fails for one batch fails only the
// hashes of that batch, and the other batches are still read.
//
// The hashes are divided over the workers as the mark divides them, and each worker reads its share
// in sequential batches through one set of slots, so a transaction costs no key, batch record or
// metadata allocation of its own (see longestChainSlots).
func (s *Store) readCreatingRecords(ctx context.Context, hashes []*chainhash.Hash) ([]creatingRecord, map[chainhash.Hash]error) {
	if len(hashes) == 0 {
		return nil, nil
	}

	batchSize := max(s.settings.UtxoStore.MaxMinedBatchSize, 1)
	numChunks := (len(hashes) + batchSize - 1) / batchSize
	numWorkers := max(min(s.settings.UtxoStore.MaxMinedRoutines, numChunks), 1)
	rangeSize := (len(hashes) + numWorkers - 1) / numWorkers

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	readPolicy := aerospike.NewBatchReadPolicy()

	var (
		mu       sync.Mutex
		creating []creatingRecord
		unread   map[chainhash.Hash]error
		wg       sync.WaitGroup
	)

	noteUnread := func(batch []chainhash.Hash, err error) {
		mu.Lock()
		defer mu.Unlock()

		if unread == nil {
			unread = make(map[chainhash.Hash]error)
		}

		for _, h := range batch {
			unread[h] = err
		}
	}

	for w := 0; w < numWorkers && w*rangeSize < len(hashes); w++ {
		workerHashes := hashes[w*rangeSize : min((w+1)*rangeSize, len(hashes))]

		wg.Add(1)

		go func() {
			defer wg.Done()

			slots := newBatchReadSlots(min(batchSize, len(workerHashes)), readPolicy, creatingReadBins)

			for start := 0; start < len(workerHashes); start += batchSize {
				chunk := workerHashes[start:min(start+batchSize, len(workerHashes))]

				n := 0

				for _, h := range chunk {
					if h == nil {
						continue
					}

					if err := slots.set(n, h, s.namespace, s.setName); err != nil {
						noteUnread([]chainhash.Hash{*h}, err)

						continue
					}

					n++
				}

				if n == 0 {
					continue
				}

				// slots.hashes is overwritten by the next batch, so what leaves this one is copied.
				if err := ctx.Err(); err != nil {
					noteUnread(append([]chainhash.Hash(nil), slots.hashes[:n]...), err)

					continue
				}

				if err := s.batchOperate(batchPolicy, slots.records[:n]); err != nil {
					noteUnread(append([]chainhash.Hash(nil), slots.hashes[:n]...), err)

					continue
				}

				for j, record := range slots.records[:n] {
					br := record.BatchRec()

					if br.Err != nil {
						if !isKeyNotFound(br.Err) {
							noteUnread([]chainhash.Hash{slots.hashes[j]}, br.Err)
						}

						continue
					}

					if br.Record == nil {
						continue
					}

					if _, isCreating := br.Record.Bins[fields.Creating.String()]; !isCreating {
						continue
					}

					rec := creatingRecord{hash: slots.hashes[j]}
					rec.attempt, _ = br.Record.Bins[fields.Attempt.String()].([]byte)
					rec.rollingBack, _ = br.Record.Bins[fields.RollingBack.String()].(bool)

					mu.Lock()
					creating = append(creating, rec)
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	return creating, unread
}

// rollForwardCreatingRecord completes one creating record that a create-first attempt wrote: it
// reads the transaction and rolls it forward (re-spend and finalize). It returns nil for a record
// that has been completed or is gone since the batch read, and for one that the roll-forward
// completed as conflicting (utxo.IsEndedConflicting): that record is final, and the write that
// follows applies to it as to any final record.
func (s *Store) rollForwardCreatingRecord(ctx context.Context, rec *creatingRecord, blockHeight uint32) error {
	md, err := s.Get(ctx, &rec.hash, fields.Tx, fields.Creating, fields.RollingBack, fields.Attempt, fields.BlockIDs, fields.BlockHeights)
	if err != nil {
		if errors.Is(err, errors.ErrTxNotFound) {
			return nil
		}

		return errors.NewProcessingError("[setMined] create-first pre-flight: get tx %s failed", rec.hash.String(), err)
	}

	if !md.Creating {
		return nil // finalized between the batch read and this get
	}

	if md.RollingBack {
		// Its owner is undoing it, and the hold it leaves carries no transaction at all, so it
		// must not be completed or mined against. Fail closed: the block is retried once the
		// rollback completes or the hold expires.
		return errors.NewProcessingError("[setMined] create-first pre-flight: tx %s is rolling back", rec.hash.String())
	}

	if md.Tx == nil {
		return nil // finalized between the batch read and this get
	}

	height := blockHeight

	for _, minedAt := range md.BlockHeights {
		height = max(height, minedAt)
	}

	// WithIgnoreLocked: a mined tx's parents may still be locked (e.g. their own
	// create-first tentative lock); the roll-forward completes an already-committed
	// spend, so a locked parent must not fail-close setMined for the whole block.
	if _, _, err := utxo.RollForwardCreating(ctx, s.logger, s, md.Tx, md.Attempt, height, utxo.WithIgnoreLocked(true)); err != nil && !utxo.IsEndedConflicting(err) {
		return errors.NewProcessingError("[setMined] create-first pre-flight: roll-forward of %s failed", rec.hash.String(), err)
	}

	return nil
}

// stampMarkOwed sets the markOwed bin on the creating records of transactions that are in a block of
// the longest chain, before the pre-flight rolls them forward. BeginCreateFirstRollback refuses a
// master that carries the bin, and both are single-record writes on the master, so the server
// orders them: from the stamp on no rollback begins. A record whose rollback began first is not
// stamped, because that rollback cannot be stopped, and neither is one that has been completed
// since the read. The bin is inert on a record that is no longer creating.
func (s *Store) stampMarkOwed(records []*creatingRecord) error {
	if len(records) == 0 {
		return nil
	}

	writePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	writePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	writePolicy.FilterExpression = aerospike.ExpAnd(
		aerospike.ExpBinExists(fields.Creating.String()),
		aerospike.ExpNot(aerospike.ExpBinExists(fields.RollingBack.String())),
	)

	stampOp := aerospike.PutOp(aerospike.NewBin(fields.MarkOwed.String(), true))
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	batchSize := max(s.settings.UtxoStore.MaxMinedBatchSize, 1)

	for start := 0; start < len(records); start += batchSize {
		chunk := records[start:min(start+batchSize, len(records))]
		writes := make([]aerospike.BatchRecordIfc, 0, len(chunk))

		for _, rec := range chunk {
			key, err := aerospike.NewKey(s.namespace, s.setName, rec.hash[:])
			if err != nil {
				return errors.NewProcessingError("[setMined] failed to create key for %s", rec.hash.String(), err)
			}

			writes = append(writes, aerospike.NewBatchWrite(writePolicy, key, stampOp))
		}

		if err := s.batchOperate(batchPolicy, writes); err != nil {
			return errors.NewStorageError("[setMined] failed to stamp %d creating records as owed the longest-chain mark", len(chunk), err)
		}

		for i, write := range writes {
			recErr := write.BatchRec().Err
			if recErr == nil || isKeyNotFound(recErr) || isFilteredOut(recErr) {
				continue
			}

			return errors.NewStorageError("[setMined] failed to stamp %s as owed the longest-chain mark", chunk[i].hash.String(), recErr)
		}
	}

	return nil
}

// clearCreatingWithoutAttempt deletes the creating bin from the master records that carry no
// attempt token and are not rolling back. The setMined writes cleared this bin on every master
// before create-first existed, and they no longer do, because they cannot tell such a record from
// a tentative one. The filter tells them apart on the server, so a record that a create-first
// attempt owns is never cleared here. The extra records are cleared by clearLockedOnRecordsMulti.
func (s *Store) clearCreatingWithoutAttempt(hashes []*chainhash.Hash) error {
	if len(hashes) == 0 {
		return nil
	}

	writePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	writePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	writePolicy.FilterExpression = finalizeFilter(nil)

	clearOp := aerospike.PutOp(aerospike.NewBin(fields.Creating.String(), nil))
	records := make([]aerospike.BatchRecordIfc, 0, len(hashes))

	for _, h := range hashes {
		key, err := aerospike.NewKey(s.namespace, s.setName, h[:])
		if err != nil {
			return errors.NewProcessingError("[setMined] failed to create key for %s", h.String(), err)
		}

		records = append(records, aerospike.NewBatchWrite(writePolicy, key, clearOp))
	}

	if err := s.batchOperate(util.GetAerospikeBatchPolicy(s.settings), records); err != nil {
		return errors.NewStorageError("[setMined] failed to clear the creating bin of %d records with no attempt token", len(hashes), err)
	}

	for i, record := range records {
		recErr := record.BatchRec().Err
		if recErr == nil || isKeyNotFound(recErr) || isFilteredOut(recErr) {
			continue
		}

		return errors.NewStorageError("[setMined] failed to clear the creating bin of %s", hashes[i].String(), recErr)
	}

	return nil
}

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
// switch to size-class bucketing analogous to model.GetTxMap.
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

	thisBlockHeight := s.blockHeight.Load() + 1

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
	blockIDs, work, err := s.processBatchResultsForSetMined(ctx, batchRecords, hashes, thisBlockHeight, minedBlockInfo)

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

		batchRecords[idx] = aerospike.NewBatchUDF(
			batchUDFPolicy,
			key,
			usePackage,
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

	return nil
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

// processBatchResultsForSetMined processes the results of the batch operation.
// It returns the per-tx blockID map and the set of records whose `locked` flag
// must be cleared (#1037). The lock-clearing I/O is performed by the caller
// (SetMinedMulti) so this function stays free of follow-up writes for the
// lock-clear path — keeping it unit-testable without a live client.
func (s *Store) processBatchResultsForSetMined(ctx context.Context, batchRecords []aerospike.BatchRecordIfc,
	hashes []*chainhash.Hash, thisBlockHeight uint32, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, lockClearWork, error) {
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
	// #1037: pagination/extra records of successfully-mined external txs whose
	// `locked` flag must be cleared (the master-keyed setMined only clears the
	// master). Populated from the setMined response's childCount (= totalExtraRecs).
	var work lockClearWork
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

	// Reuse a single LuaMapResponse across all per-record parses in this batch.
	// The struct's fields (status, signal, BlockIDs cap, Errors map buckets) are
	// preserved across iterations via Reset, eliminating per-record allocation.
	pooledRes := getLuaMapResponse()
	defer putLuaMapResponse(pooledRes)

	for idx, batchRecord := range batchRecords {
		pooledRes.Reset()
		result, res, err := s.processSingleBatchRecordPooled(ctx, batchRecord, hashes[idx], thisBlockHeight, minedBlockInfo, pooledRes)
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
			if res != nil && !minedBlockInfo.UnsetMined && res.ChildCount > 0 {
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

	// work (pagination records of successfully-mined external txs) is returned to
	// the caller for clearing, so a tx that was mined successfully is fully
	// unlocked even if a sibling record in the same batch errored below.
	if errs != nil || nrErrors > 0 {
		prometheusTxMetaAerospikeMapSetMinedBatchErrN.Add(float64(nrErrors))
		return blockIDs, work, errors.NewError("aerospike batchRecord errors", errs)
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
		return blockIDs, work, errors.NewError("aerospike setMined follow-up batch errors", postErr)
	}

	return blockIDs, work, nil
}

// processSingleBatchRecord processes a single batch record result, allocating a
// fresh LuaMapResponse. Retained for callers outside the hot path; SetMinedMulti
// uses processSingleBatchRecordPooled to share a pooled response.
func (s *Store) processSingleBatchRecord(ctx context.Context, batchRecord aerospike.BatchRecordIfc, hash *chainhash.Hash,
	thisBlockHeight uint32, minedBlockInfo utxo.MinedBlockInfo) (bool, *LuaMapResponse, error) {
	res := &LuaMapResponse{}
	ok, returnedRes, err := s.processSingleBatchRecordPooled(ctx, batchRecord, hash, thisBlockHeight, minedBlockInfo, res)
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
	thisBlockHeight uint32, minedBlockInfo utxo.MinedBlockInfo, res *LuaMapResponse) (bool, *LuaMapResponse, error) {
	batchErr := batchRecord.BatchRec().Err
	if batchErr != nil {
		return false, nil, s.handleBatchRecordError(batchErr, hash, minedBlockInfo.UnsetMined)
	}

	response := batchRecord.BatchRec().Record
	if response == nil || response.Bins == nil || response.Bins[LuaSuccess.String()] == nil {
		return false, nil, errors.NewError("missing SUCCESS bin in aerospike response for transaction %s", hash.String())
	}

	if parseErr := s.parseLuaMapResponseInto(response.Bins[LuaSuccess.String()], res); parseErr != nil {
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

// handleBatchRecordError handles errors from batch records.
// For unset-mined operations a missing record is a no-op (the tx is already gone).
// For normal set-mined a missing record is a hard error: the txmeta must exist and
// be tagged with the block ID, otherwise the mined-invariant is violated.
func (s *Store) handleBatchRecordError(err error, hash *chainhash.Hash, unsetMined bool) error {
	var aErr *aerospike.AerospikeError
	if errors.As(err, &aErr) && aErr != nil && aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
		if unsetMined {
			return nil
		}
		return errors.NewTxNotFoundError("transaction not found: %s", hash.String())
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

// lockClearItem identifies a transaction whose pagination/extra records must
// have the `locked` flag cleared as part of marking the transaction mined. The
// master record's lock is cleared by the setMined UDF / expression write itself.
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

	if clearErr := s.clearLockedOnRecordsMulti(work.items); clearErr != nil {
		err = clearErr
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

// clearLockedOnRecordsMulti clears the `locked` flag on the requested records in
// a single unfiltered BatchOperate.
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
	clearOp := aerospike.PutOp(aerospike.NewBin(fields.Locked.String(), false))

	batchRecords := make([]aerospike.BatchRecordIfc, 0, total)

	appendRecord := func(txID *chainhash.Hash, idx uint32) error {
		keySource := uaerospike.CalculateKeySourceInternal(txID, idx)

		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			return errors.NewProcessingError("[clearLockedOnRecordsMulti][%s] failed to create key for record %d", txID.String(), idx, err)
		}

		batchRecords = append(batchRecords, aerospike.NewBatchWrite(batchWritePolicy, key, clearOp))

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

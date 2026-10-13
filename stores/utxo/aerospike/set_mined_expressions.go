// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
package aerospike

import (
	"context"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// childDAHItem is one master record whose pagination records need their deleteAtHeight set
// (DeleteAtHeight > 0) or cleared (DeleteAtHeight == 0) by SetDAHForChildRecordsMulti.
type childDAHItem = struct {
	TxID           *chainhash.Hash
	ChildCount     int
	DeleteAtHeight uint32
}

// collectChildDAHFollowUp compares the master's DAH before and after the write and, for an
// external master with pagination records, queues the matching child-record update: a set or
// raised DAH is copied to the children, a cleared DAH clears them (the Lua DAHSET/DAHUNSET
// signals). It is a no-op for non-external records and when the DAH did not change.
func collectChildDAHFollowUp(hash *chainhash.Hash, state SetMinedState, setItems, unsetItems *[]childDAHItem) {
	if !state.External || state.TotalExtraRecs == nil {
		return
	}

	switch {
	case state.NewDeleteAtHeight != nil && (state.OldDeleteAtHeight == nil || *state.OldDeleteAtHeight != *state.NewDeleteAtHeight):
		*setItems = append(*setItems, childDAHItem{TxID: hash, ChildCount: *state.TotalExtraRecs, DeleteAtHeight: *state.NewDeleteAtHeight})
	case state.NewDeleteAtHeight == nil && state.OldDeleteAtHeight != nil:
		*unsetItems = append(*unsetItems, childDAHItem{TxID: hash, ChildCount: *state.TotalExtraRecs, DeleteAtHeight: 0})
	}
}

// SetMinedState contains the state returned from setMined operations.
// Only fields needed for external transaction follow-up (child record and blob storage DAH updates) are included.
type SetMinedState struct {
	// BlockIDs is the list of block IDs this transaction is mined in
	BlockIDs []uint32
	// TotalExtraRecs is the total number of extra (pagination) records, nil for non-master records
	TotalExtraRecs *int
	// External indicates if this is an external (large) transaction
	External bool
	// OldDeleteAtHeight is the block height before modifications, nil if not set
	OldDeleteAtHeight *uint32
	// NewDeleteAtHeight is the block height after modifications, nil if not set
	NewDeleteAtHeight *uint32
}

// parseSetMinedState extracts the SetMinedState from Aerospike bins.
// Only parses fields needed for external transaction follow-up.
func (s *Store) parseSetMinedState(bins aerospike.BinMap) (SetMinedState, error) {
	state := SetMinedState{}

	// Parse blockIDs
	// Note: When using BatchWrite with ListAppendWithPolicyOp followed by GetBinOp on the same bin,
	// the result is a compound value like [count, [values]]. We need to extract just the list values.
	if blockIDs, ok := bins[fields.BlockIDs.String()]; ok && blockIDs != nil {
		switch v := blockIDs.(type) {
		case aerospike.OpResults:
			// OpResults is a []interface{} alias returned by batch operations
			state.BlockIDs = s.parseBlockIDsFromSlice([]interface{}(v))
		case []interface{}:
			state.BlockIDs = s.parseBlockIDsFromSlice(v)
		case []uint32:
			state.BlockIDs = v
		}
	}

	// Parse totalExtraRecs (needed for external transaction child count)
	if totalExtraRecs, ok := bins[fields.TotalExtraRecs.String()]; ok && totalExtraRecs != nil {
		if v, ok := totalExtraRecs.(int); ok {
			state.TotalExtraRecs = &v
		}
	}

	// Parse external (needed to know if we need child record DAH updates)
	if external, ok := bins[fields.External.String()]; ok && external != nil {
		if v, ok := external.(bool); ok {
			state.External = v
		}
	}

	// Parse deleteAtHeight values (old and new)
	// Since we read DAH twice (before and after modifications), we get an array of results
	if deleteAtHeightResults, ok := bins[fields.DeleteAtHeight.String()]; ok && deleteAtHeightResults != nil {
		switch v := deleteAtHeightResults.(type) {
		case aerospike.OpResults:
			// OpResults is a []interface{} alias returned by batch operations
			s.parseDAHFromSlice([]interface{}(v), &state)
		case []interface{}:
			// Array of results: [oldValue, ..., newValue]
			s.parseDAHFromSlice(v, &state)
		case int:
			// Single value - treat as both old and new (shouldn't happen with our ops structure)
			dah := uint32(v)
			state.OldDeleteAtHeight = &dah
			state.NewDeleteAtHeight = &dah
		}
	}

	return state, nil
}

// parseDAHFromSlice extracts old and new deleteAtHeight values from a slice.
// First element is the old value (read before modifications), last element is the new value (read after modifications).
func (s *Store) parseDAHFromSlice(v []interface{}, state *SetMinedState) {
	// First element is old value (read before modifications)
	if len(v) >= 1 && v[0] != nil {
		if oldDAH, ok := v[0].(int); ok {
			old := uint32(oldDAH)
			state.OldDeleteAtHeight = &old
		}
	}
	// Last element is new value (read after modifications)
	if len(v) >= 2 && v[len(v)-1] != nil {
		if newDAH, ok := v[len(v)-1].(int); ok {
			newVal := uint32(newDAH)
			state.NewDeleteAtHeight = &newVal
		}
	}
}

// parseBlockIDsFromSlice extracts blockIDs from a slice that may be a compound result.
// When using BatchWrite with ListAppendWithPolicyOp followed by GetBinOp on the same bin,
// the result is a compound value like [count, [values]]. This function handles both formats.
func (s *Store) parseBlockIDsFromSlice(v []interface{}) []uint32 {
	var result []uint32

	// Check if this is a compound result from ListAppend + GetBinOp
	// Format: [count, [list values]]
	if len(v) >= 2 {
		// Check if first element is an int (count from list operation)
		if _, isCount := v[0].(int); isCount {
			// Look for the nested list in remaining elements
			for _, elem := range v[1:] {
				if innerList, isList := elem.([]interface{}); isList {
					// Found the nested list - extract values
					for _, id := range innerList {
						if idInt, ok := id.(int); ok {
							result = append(result, uint32(idInt)) // gosec:nolint
						}
					}
					return result
				}
			}
		}
	}

	// If not a compound result or couldn't extract, parse as regular list
	for _, id := range v {
		if idInt, ok := id.(int); ok {
			result = append(result, uint32(idInt)) // gosec:nolint
		}
	}
	return result
}

// buildDeleteAtHeightOps returns the two ExpWriteOps that give a record the same
// deleteAtHeight as the Lua setDeleteAtHeight function (teranode.lua), branch by branch.
// They read the CURRENT deleteAtHeight bin, so callers must not null the bin first.
//
// Visibility: an ExpWriteOp sees the in-flight record, i.e. writes made by EARLIER ops in the
// same operate call (a Put-nil, a ListAppend) are visible to it; it does not read the
// pre-operate record. Pinned against a real server by TestExpressionSeesInFlightOperateChanges.
//
// The first op's expression returns:
//   - newDeleteHeight (int): DAH should be set (or raised)
//   - ExpUnknown: no change needed (with ExpWriteFlagEvalNoFail this is a no-op)
//
// It never deletes the bin: Aerospike rejects a conditional that mixes int and nil branches
// (PARAMETER_ERROR), so clearing a stale DAH (step 5) is the second op's expression, which
// returns ExpNilValue (deleting the bin with ExpWriteFlagAllowDelete) or ExpUnknown.
//
// Logic implemented (same order as Lua):
//  1. If preserveUntil is set -> no change
//  2. If conflicting -> set DAH if there is none, otherwise no change
//  3. If pagination record (totalExtraRecs not set) -> no change (Lua only signals here)
//  4. If allSpent AND hasBlocks AND isOnLongestChain -> set DAH if none, raise it if lower;
//     never lower an existing higher DAH
//  5. Otherwise, clear an existing DAH
//
// The retention == 0 guard is the caller's: the ops are only added when retention > 0.
// The Lua DAHSET/DAHUNSET signals cannot be emitted from an expression; callers detect a DAH
// change by reading the bin before and after (see processBatchResultsForSetMinedExpressions).
//
// The lastSpentState logic for pagination records is handled separately via ExpWriteOp for that bin.
//
// The expired-preservation caller clears preserveUntil with an earlier op in the same operate;
// the guard in step 1 sees that in-flight clear, so no bypass is needed there.
//
// Accepted divergence from Lua: SetMinedMultiWithExpressions skips a record whose blockID is
// already present (FILTERED_OUT), so none of this runs on a re-mine of the same block, whereas
// Lua re-evaluates setDeleteAtHeight and unminedSince every time. Not fixed.
//
// Not retry-safe: the child-record DAH follow-ups run after the batch write. If one fails, the
// master already carries the new DAH and a retry is a no-op (FILTERED_OUT / old == new), so the
// child keeps the old value until something else touches the master. The Lua path has the same
// property.
func (s *Store) buildDeleteAtHeightOps(currentBlockHeight uint32, blockHeightRetention uint32, onLongestChain bool) []*aerospike.Operation {
	return s.buildDeleteAtHeightOpsFor(aerospike.ExpIntVal(int64(currentBlockHeight)+int64(blockHeightRetention)), onLongestChain)
}

// buildDeleteAtHeightOpsFromRecordHeight is buildDeleteAtHeightOps for callers that have no mined
// block height (MarkTransactionsOnLongestChain). The DAH is stamped from
// max(currentBlockHeight, the record's highest blockHeights entry) + retention, evaluated on the
// server, so it is the value setMined would have stamped or later. A record without block heights
// uses currentBlockHeight.
func (s *Store) buildDeleteAtHeightOpsFromRecordHeight(currentBlockHeight uint32, blockHeightRetention uint32, onLongestChain bool) []*aerospike.Operation {
	heights := aerospike.ExpListBin(fields.BlockHeights.String())

	highestMined := aerospike.ExpCond(
		aerospike.ExpAnd(
			aerospike.ExpBinExists(fields.BlockHeights.String()),
			aerospike.ExpGreater(aerospike.ExpListSize(heights), aerospike.ExpIntVal(0)),
		), aerospike.ExpListGetByRank(aerospike.ListReturnTypeValue, aerospike.ExpTypeINT, aerospike.ExpIntVal(-1), heights),
		aerospike.ExpIntVal(0),
	)

	return s.buildDeleteAtHeightOpsFor(aerospike.ExpNumAdd(
		aerospike.ExpMax(aerospike.ExpIntVal(int64(currentBlockHeight)), highestMined),
		aerospike.ExpIntVal(int64(blockHeightRetention)),
	), onLongestChain)
}

func (s *Store) buildDeleteAtHeightOpsFor(newDAH *aerospike.Expression, onLongestChain bool) []*aerospike.Operation {
	setExp, clearExp := s.buildDeleteAtHeightExpressionsFor(newDAH, onLongestChain)
	flags := aerospike.ExpWriteFlagAllowDelete | aerospike.ExpWriteFlagEvalNoFail

	return []*aerospike.Operation{
		aerospike.ExpWriteOp(fields.DeleteAtHeight.String(), setExp, flags),
		// A stale DAH on a no-longer-eligible master is deleted by a second expression (see above).
		aerospike.ExpWriteOp(fields.DeleteAtHeight.String(), clearExp, flags),
	}
}

// buildDeleteAtHeightExpressions builds the set/raise expression and the clear expression
// from the same branch conditions so the two can never disagree.
func (s *Store) buildDeleteAtHeightExpressions(currentBlockHeight uint32, blockHeightRetention uint32, onLongestChain bool) (*aerospike.Expression, *aerospike.Expression) {
	return s.buildDeleteAtHeightExpressionsFor(aerospike.ExpIntVal(int64(currentBlockHeight+blockHeightRetention)), onLongestChain)
}

// buildDeleteAtHeightExpressionsFor is buildDeleteAtHeightExpressions with the new DAH given as an
// expression, so it can be computed from the record.
func (s *Store) buildDeleteAtHeightExpressionsFor(newDAH *aerospike.Expression, onLongestChain bool) (*aerospike.Expression, *aerospike.Expression) {

	// Helper expressions for readability
	conflictingBin := aerospike.ExpBoolBin(fields.Conflicting.String())
	totalExtraRecsBin := aerospike.ExpIntBin(fields.TotalExtraRecs.String())
	spentExtraRecsBin := aerospike.ExpIntBin(fields.SpentExtraRecs.String())
	spentUtxosBin := aerospike.ExpIntBin(fields.SpentUtxos.String())
	recordUtxosBin := aerospike.ExpIntBin(fields.RecordUtxos.String())
	blockIDsBin := aerospike.ExpListBin(fields.BlockIDs.String())

	// Condition: preserveUntil is NOT set (bin doesn't exist)
	preserveUntilNotSet := aerospike.ExpNot(aerospike.ExpBinExists(fields.PreserveUntil.String()))

	// Condition: conflicting is true. Lua treats a missing bin as false, so guard the read.
	isConflicting := aerospike.ExpCond(
		aerospike.ExpBinExists(fields.Conflicting.String()), aerospike.ExpEq(conflictingBin, aerospike.ExpBoolVal(true)),
		aerospike.ExpBoolVal(false),
	)

	// Condition: existing DAH is set / NOT set
	dahExists := aerospike.ExpBinExists(fields.DeleteAtHeight.String())
	dahNotExists := aerospike.ExpNot(dahExists)
	existingDAH := aerospike.ExpIntBin(fields.DeleteAtHeight.String())

	// Condition: is master record (totalExtraRecs bin exists)
	isMasterRecord := aerospike.ExpBinExists(fields.TotalExtraRecs.String())

	// Condition: all UTXOs are spent (including extra records)
	// spentExtraRecs might not exist, so use ExpCond to default to 0
	spentExtraRecsOrZero := aerospike.ExpCond(
		aerospike.ExpBinExists(fields.SpentExtraRecs.String()), spentExtraRecsBin,
		aerospike.ExpIntVal(0),
	)
	// spentUtxos == recordUtxos with Lua nil semantics: both missing is equal, one missing is not.
	// An unguarded read of a missing bin makes the whole expression unknown, which is a silent
	// no-op under EvalNoFail (see TestExpressionMissingBinSemantics), so each read is guarded.
	spentExists := aerospike.ExpBinExists(fields.SpentUtxos.String())
	recordExists := aerospike.ExpBinExists(fields.RecordUtxos.String())
	utxoCountsEqual := aerospike.ExpCond(
		aerospike.ExpAnd(spentExists, recordExists), aerospike.ExpEq(spentUtxosBin, recordUtxosBin),
		aerospike.ExpAnd(aerospike.ExpNot(spentExists), aerospike.ExpNot(recordExists)), aerospike.ExpBoolVal(true),
		aerospike.ExpBoolVal(false),
	)
	allSpent := aerospike.ExpAnd(
		aerospike.ExpEq(totalExtraRecsBin, spentExtraRecsOrZero),
		utxoCountsEqual,
	)

	// Condition: has blocks (blockIDs list size > 0); a missing list is empty (Lua: nil)
	hasBlocks := aerospike.ExpCond(
		aerospike.ExpBinExists(fields.BlockIDs.String()), aerospike.ExpGreater(
			aerospike.ExpListSize(blockIDsBin),
			aerospike.ExpIntVal(0),
		),
		aerospike.ExpBoolVal(false),
	)

	// Condition: is on longest chain - we know this from the input parameter
	// If onLongestChain is true, we cleared unminedSince, so it won't exist
	// If onLongestChain is false, we need to check if unminedSince doesn't exist
	var isOnLongestChainExp *aerospike.Expression
	if onLongestChain {
		// We just cleared unminedSince, so it's on longest chain
		isOnLongestChainExp = aerospike.ExpBoolVal(true)
	} else {
		// Check if unminedSince doesn't exist (meaning it's on longest chain)
		isOnLongestChainExp = aerospike.ExpNot(aerospike.ExpBinExists(fields.UnminedSince.String()))
	}

	// Condition: should set DAH for master record (master-ness is already established by the
	// pagination-record branch preceding it in the conditional below)
	shouldSetDAHMaster := aerospike.ExpAnd(
		allSpent,
		hasBlocks,
		isOnLongestChainExp,
	)

	// Build the main conditional expression, in the same order as Lua setDeleteAtHeight.
	// ExpCond evaluates its conditions lazily and in order, so bins are only read once the
	// preceding branches have established they exist.
	setExp := aerospike.ExpCond(
		// If preserveUntil is set, no change
		aerospike.ExpNot(preserveUntilNotSet), aerospike.ExpUnknown(),

		// Conflicting: set DAH if none, otherwise leave it alone
		isConflicting, aerospike.ExpCond(
			dahNotExists, newDAH,
			aerospike.ExpUnknown(),
		),

		// Pagination record: no DAH change
		aerospike.ExpNot(isMasterRecord), aerospike.ExpUnknown(),

		// Master record, eligible: set if none, raise if lower, never lower
		shouldSetDAHMaster, aerospike.ExpCond(
			dahNotExists, newDAH,
			aerospike.ExpLess(existingDAH, newDAH), newDAH,
			aerospike.ExpUnknown(),
		),

		// Default (including a stale DAH on an ineligible master): no change
		aerospike.ExpUnknown(),
	)

	// Clear: a master record, not preserved, not conflicting, not eligible, with a DAH present.
	clearExp := aerospike.ExpCond(
		aerospike.ExpNot(preserveUntilNotSet), aerospike.ExpUnknown(),
		isConflicting, aerospike.ExpUnknown(),
		aerospike.ExpNot(isMasterRecord), aerospike.ExpUnknown(),
		shouldSetDAHMaster, aerospike.ExpUnknown(),
		dahExists, aerospike.ExpNilValue(),
		aerospike.ExpUnknown(),
	)

	return setExp, clearExp
}

// SetMinedMultiWithExpressions is the batch version of SetMinedWithExpressions.
// It processes multiple transactions in a single batch operation to Aerospike.
// This replaces the Lua UDF-based SetMinedMulti with expression-based operations.
func (s *Store) SetMinedMultiWithExpressions(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if len(hashes) == 0 {
		return nil, nil
	}

	thisBlockHeight := s.GetBlockHeight() + 1

	// Build filter expression: only proceed if blockID doesn't exist in blockIDs list
	blockIDNotExists := aerospike.ExpEq(
		aerospike.ExpListGetByValue(
			aerospike.ListReturnTypeCount,
			aerospike.ExpIntVal(int64(minedBlockInfo.BlockID)),
			aerospike.ExpListBin(fields.BlockIDs.String()),
		),
		aerospike.ExpIntVal(0),
	)

	// Create batch write policy with filter expression
	batchWritePolicy := aerospike.NewBatchWritePolicy()
	batchWritePolicy.FilterExpression = blockIDNotExists
	batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	listPolicy := aerospike.DefaultListPolicy()

	// Build operations for each transaction
	// Read old values first (before any modifications) for change detection
	ops := []*aerospike.Operation{
		aerospike.GetBinOp(fields.DeleteAtHeight.String()), // Read old DAH value before modifications
		aerospike.GetBinOp(fields.External.String()),
		aerospike.GetBinOp(fields.TotalExtraRecs.String()),
	}

	// Then perform modifications
	ops = append(ops,
		// Append to all three lists (filter expression gates these)
		aerospike.ListAppendWithPolicyOp(listPolicy, fields.BlockIDs.String(), int(minedBlockInfo.BlockID)),
		aerospike.ListAppendWithPolicyOp(listPolicy, fields.BlockHeights.String(), int(minedBlockInfo.BlockHeight)),
		aerospike.ListAppendWithPolicyOp(listPolicy, fields.SubtreeIdxs.String(), minedBlockInfo.SubtreeIdx),
		// Clear locked flag
		aerospike.PutOp(aerospike.NewBin(fields.Locked.String(), false)),
		// Delete creating bin
		aerospike.PutOp(aerospike.NewBin(fields.Creating.String(), nil)),
	)

	// Clear unminedSince if on longest chain
	if minedBlockInfo.OnLongestChain {
		ops = append(ops, aerospike.PutOp(aerospike.NewBin(fields.UnminedSince.String(), nil)))
	}

	// Add deleteAtHeight expression if retention is enabled. The bin is deliberately NOT
	// nulled first: the expression reads the existing DAH so it can raise but never lower
	// it, clear it when no longer eligible, and leave it untouched for preserveUntil and
	// retention 0 (matching the Lua setDeleteAtHeight).
	// Stamp the DAH relative to the height of the block the tx is mined into
	// (minedBlockInfo.BlockHeight), NOT the store's cached chain tip (thisBlockHeight).
	// The cached tip lags behind the block being validated during catchup/sync; using
	// it stamped the DAH too low and let the pruner delete the record before the
	// retention window elapsed.
	blockHeightRetention := s.settings.GetUtxoStoreBlockHeightRetention()
	if blockHeightRetention > 0 {
		ops = append(ops, s.buildDeleteAtHeightOps(minedBlockInfo.BlockHeight, blockHeightRetention, minedBlockInfo.OnLongestChain)...)
	}

	// Read final values after modifications (blockIDs and DAH)
	ops = append(ops,
		aerospike.GetBinOp(fields.BlockIDs.String()),
		aerospike.GetBinOp(fields.DeleteAtHeight.String()), // Read new DAH value after modifications
	)

	// Create batch records
	batchRecords := make([]aerospike.BatchRecordIfc, len(hashes))
	for i, hash := range hashes {
		key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
		if err != nil {
			return nil, errors.NewProcessingError("aerospike NewKey error", err)
		}
		batchRecords[i] = aerospike.NewBatchWrite(batchWritePolicy, key, ops...)
	}

	// Execute batch operation
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		return nil, errors.NewStorageError("aerospike BatchOperate error", err)
	}

	prometheusTxMetaAerospikeMapSetMinedBatch.Inc()

	// Process results
	blockIDs, work, err := s.processBatchResultsForSetMinedExpressions(ctx, batchRecords, hashes, thisBlockHeight, minedBlockInfo)

	// #1037: clear the lock on pagination records, and fully unlock (master + all
	// pagination records) any tx whose write was FILTERED_OUT — the blockID filter
	// otherwise skips the Locked=false op. Done here, outside the result processor,
	// so the processor performs no follow-up I/O and stays unit-testable.
	if clearErr := s.applyLockClearWork(ctx, work); clearErr != nil {
		err = errors.Join(err, clearErr)
	}

	return blockIDs, err
}

// processBatchResultsForSetMinedExpressions processes the batch results and
// returns the per-tx blockID map plus the lock-clearing follow-up work (#1037);
// the lock-clearing I/O is performed by the caller.
func (s *Store) processBatchResultsForSetMinedExpressions(
	ctx context.Context,
	batchRecords []aerospike.BatchRecordIfc,
	hashes []*chainhash.Hash,
	thisBlockHeight uint32,
	minedBlockInfo utxo.MinedBlockInfo,
) (map[chainhash.Hash][]uint32, lockClearWork, error) {
	blockIDs := make(map[chainhash.Hash][]uint32, len(hashes))
	var errs error
	okUpdates := 0
	nrErrors := 0

	// Collect follow-up actions for external transactions (child record DAH updates)
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
	externalDAH := make([]struct {
		TxID *chainhash.Hash
		DAH  uint32
	}, 0)
	// #1037: lock-clearing follow-up. Non-filtered records: pagination records via
	// work.items (the master is unlocked by the main batch write's Locked=false op).
	// FILTERED_OUT records: work.fullUnlock, since the filtered write returns no
	// child count and may have skipped the master's Locked=false too.
	var work lockClearWork

	// Process each batch record result
	for i, batchRecord := range batchRecords {
		batchRec := batchRecord.BatchRec()
		hash := hashes[i]

		// Handle errors
		if batchRec.Err != nil {
			var aErr *aerospike.AerospikeError
			if errors.As(batchRec.Err, &aErr) {
				if aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
					errs = errors.Join(errs, errors.NewTxNotFoundError("aerospike batch record not found", hash.String()))
					nrErrors++
					continue
				}

				if aErr.ResultCode == types.FILTERED_OUT {
					// Filter condition: count(blockID in blockIDs) == 0 -> operation runs.
					// FILTERED_OUT therefore proves the current blockID is already present
					// in the durable list. Synthesize the map entry so the postcondition
					// check at the end of this function sees this hash as covered.
					blockIDs[*hash] = []uint32{minedBlockInfo.BlockID}
					okUpdates++

					// #1037 (filter-gating): when the write is FILTERED_OUT the whole
					// batch write — including the Locked=false op — is skipped, and the
					// reads (totalExtraRecs etc.) return nothing, so we know neither the
					// master's lock state nor the child count. The tx is mined (its
					// blockID is present) and therefore must be spendable: fully unlock
					// it (master + all pagination records) via SetLocked, which reads the
					// child count from the master itself.
					work.fullUnlock = append(work.fullUnlock, *hash)
					continue
				}
			}
			errs = errors.Join(errs, errors.NewStorageError("aerospike batch record error", hash.String(), batchRec.Err))
			nrErrors++
			continue
		}

		// Process successful result
		if batchRec.Record == nil || batchRec.Record.Bins == nil {
			continue
		}

		state, err := s.parseSetMinedState(batchRec.Record.Bins)
		if err != nil {
			errs = errors.Join(errs, errors.NewProcessingError("aerospike parseSetMinedState error", hash.String(), err))
			nrErrors++
			continue
		}

		// Collect blockIDs for return
		if len(state.BlockIDs) > 0 {
			blockIDsUint32 := make([]uint32, len(state.BlockIDs))
			for j, bID := range state.BlockIDs {
				blockIDsUint32[j] = uint32(bID)
			}
			blockIDs[*hash] = blockIDsUint32
		}

		// For external transactions, check if DAH changed and collect follow-up actions
		if state.External && state.TotalExtraRecs != nil {
			// Check if deleteAtHeight actually changed
			dahChanged := false
			if state.OldDeleteAtHeight == nil && state.NewDeleteAtHeight != nil {
				dahChanged = true // DAH was set
			} else if state.OldDeleteAtHeight != nil && state.NewDeleteAtHeight == nil {
				dahChanged = true // DAH was cleared
			} else if state.OldDeleteAtHeight != nil && state.NewDeleteAtHeight != nil {
				dahChanged = *state.OldDeleteAtHeight != *state.NewDeleteAtHeight // DAH value changed
			}

			if dahChanged {
				// Update child records DAH if deleteAtHeight is set (not cleared)
				if state.NewDeleteAtHeight != nil {
					dahSetItems = append(dahSetItems, struct {
						TxID           *chainhash.Hash
						ChildCount     int
						DeleteAtHeight uint32
					}{TxID: hash, ChildCount: *state.TotalExtraRecs, DeleteAtHeight: *state.NewDeleteAtHeight})
				} else {
					// DAH was cleared on the master: clear it on the child records too
					// (the Lua path does this on the DAHUNSET signal).
					dahUnsetItems = append(dahUnsetItems, struct {
						TxID           *chainhash.Hash
						ChildCount     int
						DeleteAtHeight uint32
					}{TxID: hash, ChildCount: *state.TotalExtraRecs, DeleteAtHeight: 0})
				}

				// Update blob storage DAH (handles both set and clear cases)
				externalDAH = append(externalDAH, struct {
					TxID *chainhash.Hash
					DAH  uint32
				}{TxID: hash, DAH: func() uint32 {
					if state.NewDeleteAtHeight != nil {
						return *state.NewDeleteAtHeight
					}
					return 0 // 0 signals to clear DAH in blob storage
				}()})
			}
		}

		// #1037: this batch write cleared `locked` on the master only. Clear it on
		// the pagination/extra records too, so a child spending a high-index output
		// (vout >= utxoBatchSize) of a freshly-mined paginated tx is not rejected
		// with TX_LOCKED. Keyed on the presence of extra records (not the external
		// flag) so it holds for any paginated tx. UnsetMined never uses this path
		// (SetMinedMulti routes it to the UDF), so collecting here is always safe.
		if state.TotalExtraRecs != nil && *state.TotalExtraRecs > 0 {
			work.items = append(work.items, lockClearItem{txID: hash, childCount: *state.TotalExtraRecs})
		}

		okUpdates++
	}

	// Postcondition (see stores/utxo/Interface.go SetMinedMulti docstring): when
	// !UnsetMined every submitted hash MUST appear in blockIDs and the returned
	// slice MUST contain minedBlockInfo.BlockID. Paths that left a hash unmapped
	// (nil record/bins, empty BlockIDs bin) are promoted to errors here so all
	// backends fail closed identically — mirrors the SQL store's tx-not-found
	// enforcement in stores/utxo/sql/sql.go.
	if !minedBlockInfo.UnsetMined {
		for _, h := range hashes {
			bIDs, ok := blockIDs[*h]
			if !ok {
				errs = errors.Join(errs, errors.NewTxNotFoundError("setMinedMulti coverage gap: tx absent from store result", h.String()))
				nrErrors++

				continue
			}

			found := false
			for _, bID := range bIDs {
				if bID == minedBlockInfo.BlockID {
					found = true
					break
				}
			}

			if !found {
				errs = errors.Join(errs, errors.NewProcessingError("setMinedMulti coverage gap: tx present but missing current blockID", h.String()))
				nrErrors++
			}
		}
	}

	prometheusTxMetaAerospikeMapSetMinedBatchN.Add(float64(okUpdates))

	// Execute follow-up actions for external transactions (child record and blob storage DAH).
	// These run BEFORE the sibling-error return below: the records that did succeed are already
	// mined, and a retry would hit FILTERED_OUT and never reconcile their child-record DAH.
	var postErr error

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

	// Mirror the Lua path's single guard (set_mined.go) instead of the nested
	// `if nrErrors > 0 { if errs != nil }`: every nrErrors++ is paired with an
	// errs join, so the two conditions move together — keeping them as one
	// expression removes the implicit coupling. work is still returned so the
	// caller unlocks successfully-mined (and FILTERED_OUT) records despite a
	// sibling failure in this batch.
	if errs != nil || nrErrors > 0 {
		if postErr != nil {
			errs = errors.Join(errs, postErr)
		}

		prometheusTxMetaAerospikeMapSetMinedBatchErrN.Add(float64(nrErrors))
		return blockIDs, work, errors.NewError("aerospike batch record errors", errs)
	}

	if postErr != nil {
		return blockIDs, work, errors.NewError("aerospike setMined follow-up batch errors", postErr)
	}

	return blockIDs, work, nil
}

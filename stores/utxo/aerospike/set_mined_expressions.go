// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
package aerospike

import (
	"context"
	"time"

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

// childDAHFollowUps accumulates, per txid, the pagination-record DAH follow-up a batch needs.
// The LAST state recorded for a txid wins, so a set and an unset queued for the same txid in one
// batch can never be applied in the wrong order (an unset applied after a set would leave a live
// master's children without a DAH; a set after an unset would leave a prunable master's children
// pinned). The zero value is ready to use.
//
// Assumption: for one key within one batch, the order of the results equals the order the server
// executed the writes in, and a later result's "old" DAH is the earlier result's "new" DAH. That
// holds for a single BatchOperate on one node; it is not verified for a key repeated across
// nodes or batches, which is why callers collapse per batch rather than across batches.
type childDAHFollowUps struct {
	order []chainhash.Hash
	items map[chainhash.Hash]childDAHItem
}

func (f *childDAHFollowUps) put(item childDAHItem) {
	if f.items == nil {
		f.items = make(map[chainhash.Hash]childDAHItem)
	}

	if _, seen := f.items[*item.TxID]; !seen {
		f.order = append(f.order, *item.TxID)
	}

	f.items[*item.TxID] = item
}

// addSet queues "children get this DAH" (the Lua DAHSET signal).
func (f *childDAHFollowUps) addSet(hash *chainhash.Hash, childCount int, dah uint32) {
	f.put(childDAHItem{TxID: hash, ChildCount: childCount, DeleteAtHeight: dah})
}

// addUnset queues "children lose their DAH" (the Lua DAHUNSET signal).
func (f *childDAHFollowUps) addUnset(hash *chainhash.Hash, childCount int) {
	f.put(childDAHItem{TxID: hash, ChildCount: childCount})
}

// collect compares the master's DAH before and after the write and, for an external master with
// pagination records, queues the matching follow-up: a set or raised DAH is copied to the
// children, a cleared DAH clears them. It queues nothing for non-external records and when the
// DAH did not change.
func (f *childDAHFollowUps) collect(hash *chainhash.Hash, state SetMinedState) {
	if !state.External || state.TotalExtraRecs == nil {
		return
	}

	switch {
	case state.NewDeleteAtHeight != nil && (state.OldDeleteAtHeight == nil || *state.OldDeleteAtHeight != *state.NewDeleteAtHeight):
		f.addSet(hash, *state.TotalExtraRecs, *state.NewDeleteAtHeight)
	case state.NewDeleteAtHeight == nil && state.OldDeleteAtHeight != nil:
		f.addUnset(hash, *state.TotalExtraRecs)
	}
}

// sets returns the queued set items in first-seen order (nil if none).
func (f *childDAHFollowUps) sets() []childDAHItem { return f.filter(true) }

// unsets returns the queued unset items in first-seen order (nil if none).
func (f *childDAHFollowUps) unsets() []childDAHItem { return f.filter(false) }

func (f *childDAHFollowUps) filter(set bool) []childDAHItem {
	var out []childDAHItem

	for _, h := range f.order {
		if item := f.items[h]; (item.DeleteAtHeight > 0) == set {
			out = append(out, item)
		}
	}

	return out
}

// maxChildDAHPagesPerBatch bounds how many pagination records one child-DAH BatchOperate
// touches, so a batch of large external txs cannot build an unbounded request.
const maxChildDAHPagesPerBatch = 4096

// childDAHRetryBackoff is the pause before a retry of a failed child-DAH (or restore) write.
// A var so tests can shorten it.
var childDAHRetryBackoff = 50 * time.Millisecond

// chunkChildDAHItems groups items so a chunk holds about maxPages pagination records. An item is
// never split (SetDAHForChildRecordsMulti expands all of its pages), so an item larger than the
// cap travels alone. Order is preserved.
func chunkChildDAHItems(items []childDAHItem, maxPages int) [][]childDAHItem {
	var (
		chunks [][]childDAHItem
		pages  int
	)

	var cur []childDAHItem //nolint:prealloc // sized by the page cap, not len(items)

	for _, item := range items {
		if len(cur) > 0 && pages+item.ChildCount > maxPages {
			chunks = append(chunks, cur)
			cur, pages = nil, 0
		}

		cur = append(cur, item)
		pages += item.ChildCount
	}

	if len(cur) > 0 {
		chunks = append(chunks, cur)
	}

	return chunks
}

// childDAHApplyResult reports what applyChildDAHFollowUps could not write.
type childDAHApplyResult struct {
	unsetErr, setErr         error
	failedUnsets, failedSets []childDAHItem
}

// sleepCtx waits d, or returns ctx.Err() as soon as ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// writeChildDAHChunk writes one chunk, retrying once after a short backoff that honours ctx.
func (s *Store) writeChildDAHChunk(ctx context.Context, chunk []childDAHItem) error {
	err := s.SetDAHForChildRecordsMulti(chunk)
	if err != nil {
		if sleepErr := sleepCtx(ctx, childDAHRetryBackoff); sleepErr != nil {
			return errors.JoinCapped(10, err, sleepErr)
		}

		err = s.SetDAHForChildRecordsMulti(chunk)
	}

	return err
}

// applyChildDAHFollowUps writes the queued follow-ups in bounded chunks: unsets first, then sets,
// each chunk retried once after a backoff. Clearing is the data-deleting direction (a child left
// with a stale DAH can be pruned under a live master), so it goes first. A failed SET is not
// harmless either: for a raise (X -> Y) the children stay at the lower X and can be pruned before
// the master. Failures are logged at Error and returned (with the items that failed) so callers
// can count them and, where the master write is not retry-safe, restore the master.
func (s *Store) applyChildDAHFollowUps(ctx context.Context, f *childDAHFollowUps) childDAHApplyResult {
	var res childDAHApplyResult

	run := func(items []childDAHItem, kind string, failed *[]childDAHItem, errOut *error) {
		for _, chunk := range chunkChildDAHItems(items, maxChildDAHPagesPerBatch) {
			if err := s.writeChildDAHChunk(ctx, chunk); err != nil {
				s.logger.Errorf("[childDAHFollowUps] failed to %s child DAH for %d txs: %v", kind, len(chunk), err)
				*failed = append(*failed, chunk...)
				*errOut = errors.JoinCapped(10, *errOut, err)
			}
		}
	}

	run(f.unsets(), "unset", &res.failedUnsets, &res.unsetErr)
	run(f.sets(), "set", &res.failedSets, &res.setErr)

	return res
}

// dahRestore is one master record to put back into a known earlier state after a failed
// child-record DAH follow-up.
type dahRestore struct {
	// ID identifies the record in logs (usually the txid).
	ID string
	// Key is the master record.
	Key *aerospike.Key
	// Bins are the bin values to write back.
	Bins []*aerospike.Bin
	// Filter gates the write on the known post-write state (see dahUnchangedExpression plus the
	// caller's own conditions); a filtered write means another writer owns the record.
	Filter *aerospike.Expression
}

// dahUnchangedExpression is true when the record's DAH is still exactly what the write being
// undone left: absent when newDAH is nil, else present and equal to *newDAH. Nested ExpCond, so
// it does not rely on ExpAnd short-circuiting.
func dahUnchangedExpression(newDAH *uint32) *aerospike.Expression {
	dahBin := fields.DeleteAtHeight.String()
	if newDAH == nil {
		return aerospike.ExpNot(aerospike.ExpBinExists(dahBin))
	}

	return aerospike.ExpCond(
		aerospike.ExpBinExists(dahBin), aerospike.ExpEq(aerospike.ExpIntBin(dahBin), aerospike.ExpIntVal(int64(*newDAH))),
		aerospike.ExpBoolVal(false),
	)
}

// restoreDAHBatch restores masters after a failed child DAH follow-up, as one BatchOperate per
// attempt (not one per record), retrying once for only the records that failed, after a backoff
// that honours ctx. Each record is independently filtered: FILTERED_OUT or KEY_NOT_FOUND means
// another writer owns it (or it is gone) and is logged and skipped. Records that still fail are
// counted in the restore-failed metric and returned in a distinct "restore failed" error: such a
// master is left in its post-write state and nothing revisits it.
//
// Shared by every caller whose master write is not retry-safe (expired preservation here; the
// longest-chain path on the pruner branch).
func (s *Store) restoreDAHBatch(ctx context.Context, items []dahRestore) error {
	pending := items

	for attempt := 0; attempt < 2 && len(pending) > 0; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, childDAHRetryBackoff); err != nil {
				break
			}
		}

		records := make([]aerospike.BatchRecordIfc, len(pending))

		for i, it := range pending {
			policy := util.GetAerospikeBatchWritePolicy(s.settings)
			policy.RecordExistsAction = aerospike.UPDATE_ONLY
			policy.FilterExpression = it.Filter

			ops := make([]*aerospike.Operation, 0, len(it.Bins))
			for _, b := range it.Bins {
				ops = append(ops, aerospike.PutOp(b))
			}

			records[i] = aerospike.NewBatchWrite(policy, it.Key, ops...)
		}

		batchErr := s.batchOperate(util.GetAerospikeBatchPolicy(s.settings), records)

		var failed []dahRestore

		for i, rec := range records {
			recErr := rec.BatchRec().Err
			if batchErr != nil {
				recErr = batchErr
			}

			if recErr == nil {
				continue
			}

			var aErr *aerospike.AerospikeError
			if batchErr == nil && errors.As(recErr, &aErr) && (aErr.ResultCode == types.FILTERED_OUT || aErr.ResultCode == types.KEY_NOT_FOUND_ERROR) {
				s.logger.Infof("[restoreDAH] not restoring %s: record changed concurrently or deleted (%v)", pending[i].ID, aErr.ResultCode)
				continue
			}

			failed = append(failed, pending[i])
		}

		pending = failed
	}

	if len(pending) == 0 {
		return nil
	}

	if prometheusTxMetaAerospikeExpiredPreservationRestoreFailed != nil {
		prometheusTxMetaAerospikeExpiredPreservationRestoreFailed.Add(float64(len(pending)))
	}

	ids := make([]string, len(pending))
	for i, it := range pending {
		ids[i] = it.ID
	}

	s.logger.Errorf("[restoreDAH] restore failed for %d masters, they keep their post-write state and nothing will revisit them: %v", len(pending), ids)

	return errors.NewStorageError("[restoreDAH] restore failed for %d masters: %v", len(pending), ids)
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
			s.debugBareDAH()
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
// Child-record (pagination) follow-up and what happens when it fails. After the master write the
// children are updated by applyChildDAHFollowUps (unsets first, chunked, each retried once). The
// master write is not retry-safe: a retry sees old == new (or FILTERED_OUT) and queues nothing.
// Callers differ in what they do when a child write still fails:
//   - expired preservation (ProcessExpiredPreservations): RESTORES the master through
//     restoreDAHBatch, so the next run redoes the transition. The longest-chain path on the
//     pruner branch does the same with the same helper.
//   - setMined (Lua and expressions) and spend (expressions): only LOG at Error, COUNT and RETURN
//     the error (setMined returns it, spend increments prometheusUtxoMapErrors as its spends are
//     already completed). They do NOT restore, so a failed unset or raise leaves the pages stale
//     (or lower) under a master that no longer has that DAH and nothing revisits them. Known
//     follow-up, deliberately unchanged here: "pre-state" for a spend or an unmined-block
//     transition is not a single bin snapshot and needs its own design.
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
	setExp, clearExp := s.buildDeleteAtHeightExpressions(currentBlockHeight, blockHeightRetention, onLongestChain)
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
	newDeleteHeight := int64(currentBlockHeight + blockHeightRetention)

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

	newDAH := aerospike.ExpIntVal(newDeleteHeight)

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
	// Child (pagination) record DAH follow-ups, last state per txid wins.
	var childDAH childDAHFollowUps
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

		// For external transactions, queue the child-record DAH follow-up if the master DAH changed
		childDAH.collect(hash, state)

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

	applied := s.applyChildDAHFollowUps(ctx, &childDAH)
	postErr = errors.JoinCapped(10, applied.unsetErr, applied.setErr)

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

// debugBareDAH logs when the DAH bin comes back as a bare value on a path that always issues two
// reads (before and after). A bare value means the "before" read produced no entry, so old == new
// is assumed and a DAH set would not trigger the child-record follow-up.
func (s *Store) debugBareDAH() {
	if s.logger != nil {
		s.logger.Debugf("[aerospike] deleteAtHeight returned as a bare value on a two-read path; treating old == new")
	}
}

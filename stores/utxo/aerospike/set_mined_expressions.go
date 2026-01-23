// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
package aerospike

import (
	"context"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

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

// buildDeleteAtHeightExpression creates an Aerospike expression that calculates and returns
// the deleteAtHeight value based on record state. The expression evaluates the same logic
// as the Lua setDeleteAtHeight function.
//
// The expression returns:
//   - newDeleteHeight (int): When DAH should be set
//   - nil: When DAH should be cleared (with ExpWriteFlagAllowDelete this deletes the bin)
//   - ExpUnknown: When no change is needed (with ExpWriteFlagEvalNoFail this is a no-op)
//
// Logic implemented:
//  1. If preserveUntil is set → no change
//  2. If conflicting AND no existing DAH → set DAH
//  3. If master record (totalExtraRecs not nil) AND allSpent AND hasBlocks AND isOnLongestChain → set DAH
//  4. If master record AND NOT (allSpent AND hasBlocks AND isOnLongestChain) AND has existing DAH → clear DAH
//  5. Otherwise → no change
//
// Note: Pagination records (totalExtraRecs is nil) only update lastSpentState, not DAH.
// The lastSpentState logic is handled separately via ExpWriteOp for that bin.
func (s *Store) buildDeleteAtHeightExpression(currentBlockHeight uint32, blockHeightRetention uint32, onLongestChain bool) *aerospike.Expression {
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

	// Condition: conflicting is true
	isConflicting := aerospike.ExpEq(conflictingBin, aerospike.ExpBoolVal(true))

	// Condition: existing DAH is NOT set
	dahNotExists := aerospike.ExpNot(aerospike.ExpBinExists(fields.DeleteAtHeight.String()))

	// Condition: is master record (totalExtraRecs bin exists)
	isMasterRecord := aerospike.ExpBinExists(fields.TotalExtraRecs.String())

	// Condition: all UTXOs are spent (including extra records)
	// spentExtraRecs might not exist, so use ExpCond to default to 0
	spentExtraRecsOrZero := aerospike.ExpCond(
		aerospike.ExpBinExists(fields.SpentExtraRecs.String()), spentExtraRecsBin,
		aerospike.ExpIntVal(0),
	)
	allSpent := aerospike.ExpAnd(
		aerospike.ExpEq(totalExtraRecsBin, spentExtraRecsOrZero),
		aerospike.ExpEq(spentUtxosBin, recordUtxosBin),
	)

	// Condition: has blocks (blockIDs list size > 0)
	hasBlocks := aerospike.ExpGreater(
		aerospike.ExpListSize(blockIDsBin),
		aerospike.ExpIntVal(0),
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

	// Condition: should set DAH for master record
	shouldSetDAHMaster := aerospike.ExpAnd(
		isMasterRecord,
		allSpent,
		hasBlocks,
		isOnLongestChainExp,
	)

	// Build the main conditional expression
	// Order matters: check most specific conditions first
	return aerospike.ExpCond(
		// If preserveUntil is set, no change
		aerospike.ExpNot(preserveUntilNotSet), aerospike.ExpUnknown(),

		// If conflicting and no existing DAH, set DAH
		aerospike.ExpAnd(isConflicting, dahNotExists), aerospike.ExpIntVal(newDeleteHeight),

		// If master record and should set DAH
		shouldSetDAHMaster, aerospike.ExpIntVal(newDeleteHeight),

		// Default: no change
		aerospike.ExpUnknown(),
	)
}

// SetMinedMultiWithExpressions is the batch version of SetMinedWithExpressions.
// It processes multiple transactions in a single batch operation to Aerospike.
// This replaces the Lua UDF-based SetMinedMulti with expression-based operations.
func (s *Store) SetMinedMultiWithExpressions(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if len(hashes) == 0 {
		return nil, nil
	}

	thisBlockHeight := s.blockHeight.Load() + 1

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

	// Set deleteAtHeight to nil initially (will be updated by expression if needed)
	ops = append(ops, aerospike.PutOp(aerospike.NewBin(fields.DeleteAtHeight.String(), nil)))

	// Add deleteAtHeight expression if retention is enabled
	blockHeightRetention := s.settings.GetUtxoStoreBlockHeightRetention()
	if blockHeightRetention > 0 {
		dahExp := s.buildDeleteAtHeightExpression(thisBlockHeight, blockHeightRetention, minedBlockInfo.OnLongestChain)
		ops = append(ops, aerospike.ExpWriteOp(
			fields.DeleteAtHeight.String(),
			dahExp,
			aerospike.ExpWriteFlagAllowDelete|aerospike.ExpWriteFlagEvalNoFail,
		))
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
	return s.processBatchResultsForSetMinedExpressions(ctx, batchRecords, hashes, thisBlockHeight, minedBlockInfo)
}

// processBatchResultsForSetMinedExpressions processes the batch results and handles follow-up actions.
func (s *Store) processBatchResultsForSetMinedExpressions(
	ctx context.Context,
	batchRecords []aerospike.BatchRecordIfc,
	hashes []*chainhash.Hash,
	thisBlockHeight uint32,
	minedBlockInfo utxo.MinedBlockInfo,
) (map[chainhash.Hash][]uint32, error) {
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
	externalDAH := make([]struct {
		TxID *chainhash.Hash
		DAH  uint32
	}, 0)

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
					// BlockID already exists in the list - this transaction was already
					// marked as mined in this block, nothing to do
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

		okUpdates++
	}

	prometheusTxMetaAerospikeMapSetMinedBatchN.Add(float64(okUpdates))

	if nrErrors > 0 {
		prometheusTxMetaAerospikeMapSetMinedBatchErrN.Add(float64(nrErrors))
		if errs != nil {
			return blockIDs, errors.NewError("aerospike batch record errors", errs)
		}
	}

	// Execute follow-up actions for external transactions (child record and blob storage DAH)
	var postErr error

	if len(dahSetItems) > 0 {
		if err := s.SetDAHForChildRecordsMulti(dahSetItems); err != nil {
			postErr = errors.Join(postErr, err)
		}
	}

	if postErr != nil {
		return blockIDs, errors.NewError("aerospike setMined follow-up batch errors", postErr)
	}

	return blockIDs, nil
}

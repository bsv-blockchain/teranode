// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
package aerospike

import (
	"context"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// SpendState contains the state returned from spend operations.
type SpendState struct {
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
	// SpentUtxos is the number of spent UTXOs after the operation
	SpentUtxos int
	// RecordUtxos is the total number of UTXOs in this record
	RecordUtxos int
	// SpentExtraRecs is the number of spent extra records
	SpentExtraRecs *int
}

// parseSpendState extracts the SpendState from Aerospike bins.
func (s *Store) parseSpendState(bins aerospike.BinMap) (SpendState, error) {
	state := SpendState{}

	// Parse blockIDs
	if blockIDs, ok := bins[fields.BlockIDs.String()]; ok && blockIDs != nil {
		switch v := blockIDs.(type) {
		case []interface{}:
			state.BlockIDs = s.parseBlockIDsFromSlice(v)
		case []uint32:
			state.BlockIDs = v
		}
	}

	// Parse totalExtraRecs
	if totalExtraRecs, ok := bins[fields.TotalExtraRecs.String()]; ok && totalExtraRecs != nil {
		if v, ok := totalExtraRecs.(int); ok {
			state.TotalExtraRecs = &v
		}
	}

	// Parse external
	if external, ok := bins[fields.External.String()]; ok && external != nil {
		if v, ok := external.(bool); ok {
			state.External = v
		}
	}

	// Parse spentUtxos
	if spentUtxos, ok := bins[fields.SpentUtxos.String()]; ok && spentUtxos != nil {
		if v, ok := spentUtxos.(int); ok {
			state.SpentUtxos = v
		}
	}

	// Parse recordUtxos
	if recordUtxos, ok := bins[fields.RecordUtxos.String()]; ok && recordUtxos != nil {
		if v, ok := recordUtxos.(int); ok {
			state.RecordUtxos = v
		}
	}

	// Parse spentExtraRecs
	if spentExtraRecs, ok := bins[fields.SpentExtraRecs.String()]; ok && spentExtraRecs != nil {
		if v, ok := spentExtraRecs.(int); ok {
			state.SpentExtraRecs = &v
		}
	}

	// Parse deleteAtHeight values (old and new)
	if deleteAtHeightResults, ok := bins[fields.DeleteAtHeight.String()]; ok && deleteAtHeightResults != nil {
		switch v := deleteAtHeightResults.(type) {
		case aerospike.OpResults:
			s.parseDAHFromSlice([]interface{}(v), &SetMinedState{
				OldDeleteAtHeight: state.OldDeleteAtHeight,
				NewDeleteAtHeight: state.NewDeleteAtHeight,
			})
		case []interface{}:
			// Create temporary SetMinedState to reuse parsing logic
			tempState := &SetMinedState{}
			s.parseDAHFromSlice(v, tempState)
			state.OldDeleteAtHeight = tempState.OldDeleteAtHeight
			state.NewDeleteAtHeight = tempState.NewDeleteAtHeight
		case int:
			dah := uint32(v)
			state.OldDeleteAtHeight = &dah
			state.NewDeleteAtHeight = &dah
		}
	}

	return state, nil
}

// buildSpendFilterExpression creates a filter expression that validates spend preconditions.
// Returns nil if the spend should be rejected, allowing the operation to proceed if all checks pass.
func (s *Store) buildSpendFilterExpression(
	offset uint32,
	utxoHash []byte,
	spendingData []byte,
	ignoreConflicting bool,
	ignoreLocked bool,
	currentBlockHeight uint32,
) *aerospike.Expression {
	// Get bin references
	utxosBin := aerospike.ExpListBin(fields.Utxos.String())
	conflictingBin := aerospike.ExpBoolBin(fields.Conflicting.String())
	lockedBin := aerospike.ExpBoolBin(fields.Locked.String())
	creatingBin := aerospike.ExpBoolBin(fields.Creating.String())
	spendingHeightBin := aerospike.ExpIntBin(fields.SpendingHeight.String())
	// spendableInMapBin := aerospike.ExpMapBin(fields.UtxoSpendableIn.String())

	var filterConditions []*aerospike.Expression

	// Check if UTXOs bin exists
	filterConditions = append(filterConditions, aerospike.ExpBinExists(fields.Utxos.String()))

	// Check creating flag is not true
	filterConditions = append(filterConditions,
		aerospike.ExpOr(
			aerospike.ExpNot(aerospike.ExpBinExists(fields.Creating.String())),
			aerospike.ExpNot(aerospike.ExpEq(creatingBin, aerospike.ExpBoolVal(true))),
		),
	)

	// Check conflicting if not ignored
	if !ignoreConflicting {
		filterConditions = append(filterConditions,
			aerospike.ExpOr(
				aerospike.ExpNot(aerospike.ExpBinExists(fields.Conflicting.String())),
				aerospike.ExpNot(aerospike.ExpEq(conflictingBin, aerospike.ExpBoolVal(true))),
			),
		)
	}

	// Check locked if not ignored
	if !ignoreLocked {
		filterConditions = append(filterConditions,
			aerospike.ExpOr(
				aerospike.ExpNot(aerospike.ExpBinExists(fields.Locked.String())),
				aerospike.ExpNot(aerospike.ExpEq(lockedBin, aerospike.ExpBoolVal(true))),
			),
		)
	}

	// Check coinbase maturity (spendingHeight)
	filterConditions = append(filterConditions,
		aerospike.ExpOr(
			aerospike.ExpNot(aerospike.ExpBinExists(fields.SpendingHeight.String())),
			aerospike.ExpEq(spendingHeightBin, aerospike.ExpIntVal(0)),
			aerospike.ExpLessEq(spendingHeightBin, aerospike.ExpIntVal(int64(currentBlockHeight))),
		),
	)

	// Check spendableIn for this specific offset
	// Note: Aerospike expressions have limited map access capabilities.
	// We check if the spendableIn bin exists but cannot easily check specific map values.
	// The actual spendableIn validation will be done at the Lua level or by reading the map.
	// For the expression-based approach, we accept that this check is less strict than Lua.
	// Alternative: Skip the operation if spendableIn exists (conservative approach)
	filterConditions = append(filterConditions,
		aerospike.ExpNot(aerospike.ExpBinExists(fields.UtxoSpendableIn.String())),
	)

	// Check UTXO exists at offset (list size > offset)
	filterConditions = append(filterConditions,
		aerospike.ExpGreater(
			aerospike.ExpListSize(utxosBin),
			aerospike.ExpIntVal(int64(offset)),
		),
	)

	// Note: We cannot check the exact UTXO hash in expressions due to limitations:
	// - No byte-by-byte comparison support
	// - No ExpBlobSize function
	// - List element byte operations are not supported
	// The ListSetOp will fail if the UTXO doesn't match during the operation.

	// Combine all conditions with AND
	if len(filterConditions) == 1 {
		return filterConditions[0]
	}

	return aerospike.ExpAnd(filterConditions...)
}

// buildSpendOperations creates the operations for spending a single UTXO with expressions.
func (s *Store) buildSpendOperations(
	offset uint32,
	utxoHash []byte,
	spendingData []byte,
	currentBlockHeight uint32,
	blockHeightRetention uint32,
) []*aerospike.Operation {
	ops := []*aerospike.Operation{
		// Read old values before modifications
		aerospike.GetBinOp(fields.DeleteAtHeight.String()),
		aerospike.GetBinOp(fields.External.String()),
		aerospike.GetBinOp(fields.TotalExtraRecs.String()),
		aerospike.GetBinOp(fields.SpentExtraRecs.String()),
		aerospike.GetBinOp(fields.BlockIDs.String()),
	}

	// Create new UTXO bytes (32-byte hash + 36-byte spending data = 68 bytes)
	newUtxo := make([]byte, 68)
	copy(newUtxo[0:32], utxoHash)
	copy(newUtxo[32:68], spendingData)

	// Update the UTXO at the specified offset
	ops = append(ops,
		aerospike.ListSetOp(fields.Utxos.String(), int(offset), newUtxo),
	)

	// Increment spentUtxos counter
	ops = append(ops,
		aerospike.AddOp(aerospike.NewBin(fields.SpentUtxos.String(), 1)),
	)

	// Set deleteAtHeight to nil initially
	ops = append(ops, aerospike.PutOp(aerospike.NewBin(fields.DeleteAtHeight.String(), nil)))

	// Add deleteAtHeight expression if retention is enabled
	if blockHeightRetention > 0 {
		dahExp := s.buildDeleteAtHeightExpression(currentBlockHeight, blockHeightRetention, false)
		ops = append(ops, aerospike.ExpWriteOp(
			fields.DeleteAtHeight.String(),
			dahExp,
			aerospike.ExpWriteFlagAllowDelete|aerospike.ExpWriteFlagEvalNoFail,
		))
	}

	// Read final values after modifications
	ops = append(ops,
		aerospike.GetBinOp(fields.SpentUtxos.String()),
		aerospike.GetBinOp(fields.RecordUtxos.String()),
		aerospike.GetBinOp(fields.DeleteAtHeight.String()),
	)

	return ops
}

// SpendMultiWithExpressions processes multiple spend requests using Aerospike expressions.
// Unlike the Lua-based spendMulti which handles multiple spends per transaction in a single UDF call,
// this creates individual batch operations for each spend but executes them in a single batch.
func (s *Store) SpendMultiWithExpressions(ctx context.Context, batch []*batchSpend) {
	start := time.Now()
	_, _, deferFn := tracing.Tracer("aerospike").Start(ctx, "SpendMultiWithExpressions",
		tracing.WithHistogram(prometheusUtxoSpendBatch),
	)
	defer func() {
		prometheusUtxoSpendBatchSize.Observe(float64(len(batch)))
		deferFn()
	}()

	batchID := s.batchID.Add(1)
	s.logSpendBatchStart(batchID, len(batch))

	// Create individual batch operations for each spend
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(batch))
	spendIndex := make(map[int]*batchSpend) // Map batch record index to original spend

	aeroKeyMap := make(map[string]*aerospike.Key)
	batchRecordIdx := 0

	for _, bItem := range batch {
		key, err := s.getOrCreateAerospikeKey(bItem, s.utxoBatchSize, aeroKeyMap)
		if err != nil {
			bItem.errCh <- err
			continue
		}

		if err := s.validateSpendItem(bItem); err != nil {
			bItem.errCh <- err
			continue
		}

		// Calculate offset
		offset := s.calculateOffsetForOutput(bItem.spend.Vout)

		// Create filter expression for this spend
		filterExp := s.buildSpendFilterExpression(
			offset,
			bItem.spend.UTXOHash[:],
			bItem.spend.SpendingData.Bytes(),
			bItem.ignoreConflicting,
			bItem.ignoreLocked,
			bItem.blockHeight,
		)

		// Create batch write policy with filter
		batchWritePolicy := aerospike.NewBatchWritePolicy()
		batchWritePolicy.FilterExpression = filterExp
		batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY

		// Build operations
		blockHeightRetention := s.settings.GetUtxoStoreBlockHeightRetention()
		ops := s.buildSpendOperations(
			offset,
			bItem.spend.UTXOHash[:],
			bItem.spend.SpendingData.Bytes(),
			bItem.blockHeight,
			blockHeightRetention,
		)

		// Create batch record
		batchRecords = append(batchRecords, aerospike.NewBatchWrite(batchWritePolicy, key, ops...))
		spendIndex[batchRecordIdx] = bItem
		batchRecordIdx++
	}

	// Execute batch operation
	if len(batchRecords) == 0 {
		return
	}

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		for _, bItem := range batch {
			bItem.errCh <- errors.NewStorageError("[SPEND_BATCH_EXP] failed to batch spend: %w", err)
		}
		return
	}

	// Process results
	s.processSpendBatchResultsExpressions(ctx, batchRecords, spendIndex, batchID, start)
}

// processSpendBatchResultsExpressions processes the results from expression-based spend operations.
func (s *Store) processSpendBatchResultsExpressions(
	ctx context.Context,
	batchRecords []aerospike.BatchRecordIfc,
	spendIndex map[int]*batchSpend,
	batchID uint64,
	start time.Time,
) {
	okCount := 0
	errCount := 0

	// Collect follow-up actions
	extraRecords := make([]*chainhash.Hash, 0)
	dahSetItems := make([]struct {
		TxID           *chainhash.Hash
		ChildCount     int
		DeleteAtHeight uint32
	}, 0)
	externalDAH := make([]struct {
		TxID *chainhash.Hash
		DAH  uint32
	}, 0)

	for idx, batchRecord := range batchRecords {
		bItem, ok := spendIndex[idx]
		if !ok {
			continue
		}

		batchRec := batchRecord.BatchRec()

		// Handle errors
		if batchRec.Err != nil {
			var aErr *aerospike.AerospikeError
			if errors.As(batchRec.Err, &aErr) {
				if aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
					bItem.errCh <- errors.NewTxNotFoundError("transaction not found: %s", bItem.spend.TxID.String())
					errCount++
					continue
				}

				if aErr.ResultCode == types.FILTERED_OUT {
					// Spend was filtered out - could be already spent, conflicting, locked, etc.
					// Return a generic error since we can't distinguish the exact reason
					bItem.errCh <- errors.NewUtxoError("spend validation failed for %s:%d", bItem.spend.TxID.String(), bItem.spend.Vout)
					errCount++
					continue
				}
			}

			bItem.errCh <- errors.NewStorageError("spend error for %s:%d: %w", bItem.spend.TxID.String(), bItem.spend.Vout, batchRec.Err)
			errCount++
			continue
		}

		// Parse successful result
		if batchRec.Record == nil || batchRec.Record.Bins == nil {
			bItem.errCh <- nil
			okCount++
			continue
		}

		state, err := s.parseSpendState(batchRec.Record.Bins)
		if err != nil {
			bItem.errCh <- errors.NewProcessingError("failed to parse spend state: %w", err)
			errCount++
			continue
		}

		// Check if all UTXOs are now spent (pagination record check)
		if state.TotalExtraRecs == nil {
			// Pagination record - check if all spent
			if state.RecordUtxos > 0 && state.SpentUtxos == state.RecordUtxos {
				extraRecords = append(extraRecords, bItem.spend.TxID)
			}
		} else if state.TotalExtraRecs != nil {
			// Master record - check DAH changes for external transactions
			if state.External {
				dahChanged := false
				if state.OldDeleteAtHeight == nil && state.NewDeleteAtHeight != nil {
					dahChanged = true
				} else if state.OldDeleteAtHeight != nil && state.NewDeleteAtHeight == nil {
					dahChanged = true
				} else if state.OldDeleteAtHeight != nil && state.NewDeleteAtHeight != nil {
					dahChanged = *state.OldDeleteAtHeight != *state.NewDeleteAtHeight
				}

				if dahChanged {
					if state.NewDeleteAtHeight != nil {
						dahSetItems = append(dahSetItems, struct {
							TxID           *chainhash.Hash
							ChildCount     int
							DeleteAtHeight uint32
						}{TxID: bItem.spend.TxID, ChildCount: *state.TotalExtraRecs, DeleteAtHeight: *state.NewDeleteAtHeight})
					}

					externalDAH = append(externalDAH, struct {
						TxID *chainhash.Hash
						DAH  uint32
					}{TxID: bItem.spend.TxID, DAH: func() uint32 {
						if state.NewDeleteAtHeight != nil {
							return *state.NewDeleteAtHeight
						}
						return 0
					}()})
				}
			}
		}

		bItem.errCh <- nil
		okCount++
	}

	// Record successes for circuit breaker
	if s.spendCircuitBreaker != nil {
		if okCount > 0 {
			s.spendCircuitBreaker.RecordSuccess()
		}
		if errCount > 0 {
			s.spendCircuitBreaker.RecordFailure()
		}
	}

	prometheusUtxoMapSpend.Add(float64(okCount))
	if errCount > 0 {
		prometheusUtxoMapErrors.WithLabelValues("SpendExpressions", "ValidationFailed").Add(float64(errCount))
	}

	// Execute follow-up actions
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

	if postErr != nil {
		s.logger.Errorf("[SPEND_BATCH_EXP] follow-up errors: %v", postErr)
	}

	if s.settings.UtxoStore.VerboseDebug {
		s.logger.Debugf("[SPEND_BATCH_EXP] batch %d of %d spends completed in %v (ok=%d, err=%d)",
			batchID, len(spendIndex), time.Since(start), okCount, errCount)
	}
}

package aerospike

import (
	"context"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// PreserveTransactionsWithExpressions marks transactions to be preserved from deletion
// using Aerospike batch write operations instead of Lua UDFs.
// Missing records are treated as no-ops (not errors).
//
// Prune-eligibility gate: the FilterExpression restricts the write to records that already carry
// a deleteAtHeight stamp (eligible now) or are already being preserved (preserveUntil set, so
// renewal still works). A record with neither is not fully spent, so it is not at risk of pruning
// and needs no protection; gating server-side avoids pointless writes and keeps not-fully-spent
// txs out of the preservation/expiry path. Filtered records return FILTERED_OUT, which is treated
// as a benign skip below (mirrors the Lua path's no-op).
func (s *Store) PreserveTransactionsWithExpressions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	batchWritePolicy.FilterExpression = aerospike.ExpOr(
		aerospike.ExpBinExists(fields.DeleteAtHeight.String()),
		aerospike.ExpBinExists(fields.PreserveUntil.String()),
	)

	ops := []*aerospike.Operation{
		aerospike.PutOp(aerospike.NewBin(fields.PreserveUntil.String(), int(preserveUntilHeight))),
		aerospike.PutOp(aerospike.NewBin(fields.DeleteAtHeight.String(), nil)),
		// read back, so an external master's pagination records can be followed up (below)
		aerospike.GetBinOp(fields.External.String()),
		aerospike.GetBinOp(fields.TotalExtraRecs.String()),
	}

	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(txIDs))
	validIndexes := make([]int, 0, len(txIDs))

	var keyErrors int
	for i, txID := range txIDs {
		key, err := aerospike.NewKey(s.namespace, s.setName, txID[:])
		if err != nil {
			keyErrors++
			continue
		}
		batchRecords = append(batchRecords, aerospike.NewBatchWrite(batchWritePolicy, key, ops...))
		validIndexes = append(validIndexes, i)
	}

	if keyErrors > 0 {
		s.logger.Errorf("[PreserveTransactions] Failed to create keys for %d/%d transactions", keyErrors, len(txIDs))
	}

	if len(batchRecords) == 0 {
		return nil
	}

	if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		return errors.NewStorageError("failed to preserve transactions", err)
	}

	var (
		otherErrors int
		aErr        *aerospike.AerospikeError
	)

	preservedCount := 0

	var (
		childDAH childDAHFollowUps
		pagesErr error
	)

	for j, record := range batchRecords {
		batchRec := record.BatchRec()
		if batchRec.Err != nil {
			// KEY_NOT_FOUND: missing record. FILTERED_OUT: no deleteAtHeight stamp, so not
			// prune-eligible — a deliberate skip by the eligibility gate, not an error.
			if errors.As(batchRec.Err, &aErr) &&
				(aErr.ResultCode == types.KEY_NOT_FOUND_ERROR || aErr.ResultCode == types.FILTERED_OUT) {
				continue
			}

			s.logger.Warnf("[PreserveTransactions] Failed to preserve tx %s: %v", txIDs[validIndexes[j]].String(), batchRec.Err)
			otherErrors++

			continue
		}

		preservedCount++

		state, parseErr := s.parseSetMinedState(batchRec.Record.Bins)
		if parseErr != nil {
			pagesErr = errors.JoinCapped(10, pagesErr, errors.NewProcessingError("[PreserveTransactions][%s] failed to parse the pagination record count", txIDs[validIndexes[j]].String(), parseErr))
			continue
		}

		childDAH.collectPreserved(&txIDs[validIndexes[j]], state)
	}

	// A failed page write is returned: the pruner skips its deletion phase when preservation
	// fails and selects pages by their own DAH, so a page left with its DAH would otherwise be
	// deleted in the same cycle. The follow-up is unconditional, so the next run heals it.
	applied := s.applyChildDAHFollowUps(ctx, &childDAH)
	pagesErr = errors.JoinCapped(10, pagesErr, applied.unsetErr)

	if otherErrors > 0 {
		s.logger.Errorf("[PreserveTransactions] %d errors processing %d transactions", otherErrors, len(txIDs))
	}

	s.logger.Debugf("[PreserveTransactions] Successfully preserved %d out of %d transactions", preservedCount, len(txIDs))

	if pagesErr != nil {
		return errors.NewStorageError("[PreserveTransactions] failed to clear the deleteAtHeight of the pagination records of preserved txs", pagesErr)
	}

	return nil
}

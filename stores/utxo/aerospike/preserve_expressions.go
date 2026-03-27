package aerospike

import (
	"context"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// PreserveTransactionsWithExpressions marks transactions to be preserved from deletion
// using Aerospike batch write operations instead of Lua UDFs.
// Missing records are treated as no-ops (not errors).
func (s *Store) PreserveTransactionsWithExpressions(_ context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	// Use UPDATE (not UPDATE_ONLY) so Aerospike evaluates the filter expression on non-existing records.
	// With UPDATE_ONLY, missing records short-circuit to KEY_NOT_FOUND before the filter is checked.
	// The filter expression acts as the existence guard — records without the utxos bin are silently skipped.
	batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	batchWritePolicy.FilterExpression = aerospike.ExpBinExists(fields.Utxos.String())

	ops := []*aerospike.Operation{
		aerospike.PutOp(aerospike.NewBin(fields.PreserveUntil.String(), int(preserveUntilHeight))),
		aerospike.PutOp(aerospike.NewBin(fields.DeleteAtHeight.String(), nil)),
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

	for j, record := range batchRecords {
		batchRec := record.BatchRec()
		if batchRec.Err != nil {
			if errors.As(batchRec.Err, &aErr) && aErr.ResultCode == types.FILTERED_OUT {
				continue
			}

			s.logger.Warnf("[PreserveTransactions] Failed to preserve tx %s: %v", txIDs[validIndexes[j]].String(), batchRec.Err)
			otherErrors++

			continue
		}

		preservedCount++
	}

	if otherErrors > 0 {
		s.logger.Errorf("[PreserveTransactions] %d errors processing %d transactions", otherErrors, len(txIDs))
	}

	s.logger.Debugf("[PreserveTransactions] Successfully preserved %d out of %d transactions", preservedCount, len(txIDs))

	return nil
}

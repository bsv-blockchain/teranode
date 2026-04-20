package aerospike

import (
	"context"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// RemoveBlockIDs strips each removal's block IDs from the transaction's
// blockIDs list via BatchOperate. Idempotent.
func (s *Store) RemoveBlockIDs(ctx context.Context, removals []utxo.BlockIDsRemoval) error {
	_, _, deferFn := tracing.Tracer("aerospike").Start(ctx, "RemoveBlockIDs")
	defer deferFn()

	if len(removals) == 0 {
		return nil
	}

	batchWritePolicy := aerospike.NewBatchWritePolicy()
	batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(removals))

	for _, r := range removals {
		if r.TxHash == nil {
			return errors.NewInvalidArgumentError("txHash must be non-nil")
		}
		if len(r.BlockIDs) == 0 {
			continue
		}

		key, err := aerospike.NewKey(s.namespace, s.setName, r.TxHash.CloneBytes())
		if err != nil {
			return errors.NewProcessingError("could not create aerospike key", err)
		}

		// BlockIDs are stored as ints (see SetMinedMulti's ListAppendWithPolicyOp).
		vals := make([]interface{}, len(r.BlockIDs))
		for i, id := range r.BlockIDs {
			vals[i] = int(id)
		}

		op := aerospike.ListRemoveByValueListOp(fields.BlockIDs.String(), vals, aerospike.ListReturnTypeNone)

		batchRecords = append(batchRecords, aerospike.NewBatchWrite(batchWritePolicy, key, op))
	}

	if len(batchRecords) == 0 {
		return nil
	}

	if err := s.client.BatchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords); err != nil {
		return errors.NewStorageError("batch-remove blockIDs failed", err)
	}

	for _, br := range batchRecords {
		if recErr := br.BatchRec().Err; recErr != nil {
			if asErr, ok := recErr.(aerospike.Error); ok && asErr.Matches(types.KEY_NOT_FOUND_ERROR) {
				continue
			}
			return errors.NewStorageError("failed to remove blockIDs", recErr)
		}
	}

	return nil
}

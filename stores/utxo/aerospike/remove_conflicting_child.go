package aerospike

import (
	"context"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// RemoveFromConflictingChildren removes each child hash from the corresponding
// parent record's conflictingCs list using Aerospike BatchOperate.
//
// Each (parent, child) tuple becomes one BatchWrite with a
// ListRemoveByValue op. Missing parent records and absent list entries are
// tolerated individually — the call fails only if batch-level dispatch
// itself fails.
//
// Multiple removals targeting the same parent are packed into a single
// BatchWrite (ListRemoveByValueList) for efficiency.
func (s *Store) RemoveFromConflictingChildren(ctx context.Context, removals []utxo.ConflictingChildRemoval) error {
	_, _, deferFn := tracing.Tracer("aerospike").Start(ctx, "RemoveFromConflictingChildren")
	defer deferFn()

	if len(removals) == 0 {
		return nil
	}

	// Collapse by parent so we issue one ListRemoveByValueList per parent.
	grouped := make(map[string][][]byte, len(removals))
	parentKeys := make(map[string][]byte, len(removals))
	order := make([]string, 0, len(removals))

	for _, r := range removals {
		if r.ParentHash == nil || r.ChildHash == nil {
			return errors.NewInvalidArgumentError("parent and child hash must be non-nil")
		}

		k := string(r.ParentHash[:])
		if _, seen := grouped[k]; !seen {
			parentKeys[k] = r.ParentHash.CloneBytes()
			order = append(order, k)
		}
		grouped[k] = append(grouped[k], r.ChildHash.CloneBytes())
	}

	batchWritePolicy := aerospike.NewBatchWritePolicy()
	batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(order))

	for _, k := range order {
		key, err := aerospike.NewKey(s.namespace, s.setName, parentKeys[k])
		if err != nil {
			return errors.NewProcessingError("could not create aerospike key", err)
		}

		children := grouped[k]

		var op *aerospike.Operation
		if len(children) == 1 {
			op = aerospike.ListRemoveByValueOp(fields.ConflictingChildren.String(), children[0], aerospike.ListReturnTypeNone)
		} else {
			vals := make([]interface{}, len(children))
			for i, b := range children {
				vals[i] = b
			}
			op = aerospike.ListRemoveByValueListOp(fields.ConflictingChildren.String(), vals, aerospike.ListReturnTypeNone)
		}

		batchRecords = append(batchRecords, aerospike.NewBatchWrite(batchWritePolicy, key, op))
	}

	if err := s.client.BatchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords); err != nil {
		return errors.NewStorageError("batch-remove from conflictingCs failed", err)
	}

	// Per-record errors are recorded on each BatchRecordIfc. Missing parent
	// records are expected and tolerated; only unexpected errors should
	// surface.
	for _, br := range batchRecords {
		if recErr := br.BatchRec().Err; recErr != nil {
			if asErr, ok := recErr.(aerospike.Error); ok && asErr.Matches(types.KEY_NOT_FOUND_ERROR) {
				continue
			}
			return errors.NewStorageError("failed to remove from conflictingCs", recErr)
		}
	}

	return nil
}

package aerospike

import (
	"context"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// SetDAHItem describes a single set-DAH operation: the raw 32-byte transaction
// hash and the Delete-At-Height value to write.
type SetDAHItem struct {
	TxHash []byte
	DAH    uint32
}

// SetDAHResult reports the outcome of a single set-DAH operation within
// BatchSetDAH. Index i in the result slice corresponds to index i in the
// input items slice.
type SetDAHResult struct {
	// TxHash is the raw 32-byte transaction ID for this entry.
	TxHash []byte
	// Err is non-nil when the Aerospike write was rejected for this record
	// (e.g. KEY_NOT_FOUND because the record does not exist). A nil Err means
	// the update succeeded.
	Err error
}

// BatchSetDAH sets the DeleteAtHeight bin on N records via a single
// BatchOperate call using UPDATE_ONLY semantics, so records that do not exist
// surface as per-record SetDAHResult.Err rather than being silently created.
//
// If items is nil or empty, ([]SetDAHResult{}, nil) is returned immediately.
func (s *Store) BatchSetDAH(ctx context.Context, items []SetDAHItem) ([]SetDAHResult, error) {
	if len(items) == 0 {
		return []SetDAHResult{}, nil
	}

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, len(items))
	results := make([]SetDAHResult, len(items))

	for i, it := range items {
		results[i].TxHash = it.TxHash

		key, aErr := aerospike.NewKey(s.namespace, s.setName, it.TxHash)
		if aErr != nil {
			results[i].Err = errors.NewProcessingError("[BatchSetDAH] failed to build Aerospike key for tx %x", it.TxHash, aErr)
			// Use a placeholder BatchRead so the slice stays the same length.
			batchRecords[i] = aerospike.NewBatchRead(nil, placeholderKey, nil)
			continue
		}

		op := aerospike.PutOp(aerospike.NewBin(fields.DeleteAtHeight.String(), it.DAH))
		batchRecords[i] = aerospike.NewBatchWrite(batchWritePolicy, key, op)
	}

	if asErr := s.client.BatchOperate(batchPolicy, batchRecords); asErr != nil {
		return nil, errors.NewStorageError("[BatchSetDAH] BatchOperate failed", asErr)
	}

	for i := range items {
		// Skip entries already resolved during key-build.
		if results[i].Err != nil {
			continue
		}

		rec := batchRecords[i].BatchRec()
		if rec.Err == nil {
			continue
		}

		if aErr, ok := rec.Err.(*aerospike.AerospikeError); ok {
			switch aErr.ResultCode {
			case types.KEY_NOT_FOUND_ERROR:
				results[i].Err = errors.NewTxNotFoundError("[BatchSetDAH] tx %x not found", items[i].TxHash)
				continue
			}
		}

		results[i].Err = errors.NewStorageError("[BatchSetDAH] error setting DAH for tx %x: %v", items[i].TxHash, rec.Err)
	}

	return results, nil
}

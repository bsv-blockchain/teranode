package aerospike

import (
	"context"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// SetLockedResult reports the outcome of a single set-locked operation within
// BatchSetLocked. Index i in the result slice corresponds to index i in the
// input txHashes slice.
type SetLockedResult struct {
	// TxHash is the raw 32-byte transaction ID for this entry.
	TxHash []byte
	// Err is non-nil when the Aerospike write was rejected for this record
	// (e.g. KEY_NOT_FOUND because the record does not exist). A nil Err means
	// the update succeeded.
	Err error
}

// BatchSetLocked flips the locked bin on N records via a single BatchOperate
// call using UPDATE_ONLY semantics, so records that do not exist surface as
// per-record SetLockedResult.Err rather than being silently created.
//
// If txHashes is nil or empty, ([]SetLockedResult{}, nil) is returned immediately.
func (s *Store) BatchSetLocked(ctx context.Context, txHashes [][]byte, locked bool) ([]SetLockedResult, error) {
	if len(txHashes) == 0 {
		return []SetLockedResult{}, nil
	}

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, len(txHashes))
	results := make([]SetLockedResult, len(txHashes))

	for i, h := range txHashes {
		results[i].TxHash = h

		key, aErr := aerospike.NewKey(s.namespace, s.setName, h)
		if aErr != nil {
			results[i].Err = errors.NewProcessingError("[BatchSetLocked] failed to build Aerospike key for tx %x", h, aErr)
			// Use a placeholder BatchRead so the slice stays the same length.
			batchRecords[i] = aerospike.NewBatchRead(nil, placeholderKey, nil)
			continue
		}

		op := aerospike.PutOp(aerospike.NewBin(fields.Locked.String(), locked))
		batchRecords[i] = aerospike.NewBatchWrite(batchWritePolicy, key, op)
	}

	if asErr := s.client.BatchOperate(batchPolicy, batchRecords); asErr != nil {
		return nil, errors.NewStorageError("[BatchSetLocked] BatchOperate failed", asErr)
	}

	for i := range txHashes {
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
				results[i].Err = errors.NewTxNotFoundError("[BatchSetLocked] tx %x not found", txHashes[i])
				continue
			}
		}

		results[i].Err = errors.NewStorageError("[BatchSetLocked] error setting locked for tx %x: %v", txHashes[i], rec.Err)
	}

	return results, nil
}

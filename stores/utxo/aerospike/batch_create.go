package aerospike

import (
	"context"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
)

// CreateResult reports the outcome of a single create operation within BatchCreate.
// Index i in the result slice corresponds to index i in the input txs slice.
type CreateResult struct {
	// TxHash is the raw 32-byte transaction ID for this entry.
	TxHash []byte
	// Err is non-nil when the Aerospike write was rejected for this record
	// (e.g. CREATE_ONLY collision). A nil Err means the create succeeded.
	Err error
}

// BatchCreate writes N new tx records in a single BatchOperate call using
// CREATE_ONLY semantics so duplicate-key collisions surface as per-record
// CreateResult.Err rather than as a whole-call error.
//
// The method covers the single-record (non-paginated, non-external) path only.
// Transactions that require pagination (>20 000 outputs) or that exceed
// MaxTxSizeInStoreInBytes will surface a per-record error; use Store.Create for
// those. The lockedTrue flag is written to the fields.Locked bin, matching the
// same semantics as Store.Create's utxo.WithLocked option.
//
// blockHeight is passed through to GetBinsToStore (and onward to fee/UTXO-hash
// calculation). Pass 0 for unmined transactions, exactly as Store.Create does.
//
// If txs is nil or empty, ([]CreateResult{}, nil) is returned immediately so
// callers can range over the result without a nil-check, matching the empty-
// batch convention used by BatchSpend / BatchSetDAH / BatchSetLocked.
func (s *Store) BatchCreate(ctx context.Context, txs []*bt.Tx, blockHeight uint32, lockedTrue bool) ([]CreateResult, error) {
	if len(txs) == 0 {
		return []CreateResult{}, nil
	}

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	batchWritePolicy.RecordExistsAction = aerospike.CREATE_ONLY

	results := make([]CreateResult, len(txs))
	batchRecords := make([]aerospike.BatchRecordIfc, len(txs))
	// skipOperate[i] = true when we've already resolved results[i] during setup
	// (e.g. key-creation failure, multi-record tx) so the post-operate loop must
	// not overwrite it.
	skipOperate := make([]bool, len(txs))

	for i, tx := range txs {
		txHash := tx.TxIDChainHash()
		results[i].TxHash = txHash.CloneBytes()

		key, aErr := aerospike.NewKey(s.namespace, s.setName, txHash[:])
		if aErr != nil {
			results[i].Err = errors.NewProcessingError("[BatchCreate] failed to build Aerospike key for tx %s", txHash, aErr)
			skipOperate[i] = true
			// NOOP placeholder so the slice stays the same length as txs.
			batchRecords[i] = aerospike.NewBatchRead(nil, placeholderKey, nil)
			continue
		}

		// We only handle the single-record, in-store path. external=false, no
		// blockIDs/blockHeights/subtreeIdxs (unmined equivalent of Store.Create
		// called without MinedBlockInfos). The caller provides blockHeight for
		// fee/UTXO-hash computation.
		binsToStore, binsErr := s.GetBinsToStore(
			tx,
			blockHeight,
			nil,   // blockIDs
			nil,   // blockHeights
			nil,   // subtreeIdxs
			false, // external
			txHash,
			tx.IsCoinbase(),
			false, // conflicting — batch-create always non-conflicting
			lockedTrue,
			nil, // arena — batch path uses heap-backed bins (no per-batch arena reuse)
		)
		if binsErr != nil {
			results[i].Err = errors.NewProcessingError("[BatchCreate] failed to build bins for tx %s", txHash, binsErr)
			skipOperate[i] = true
			batchRecords[i] = aerospike.NewBatchRead(nil, placeholderKey, nil)
			continue
		}

		// Multi-record (paginated) txs are not supported in the batch path.
		if len(binsToStore) > 1 {
			results[i].Err = errors.NewProcessingError("[BatchCreate] tx %s has %d records (paginated) — use Store.Create for large txs", txHash, len(binsToStore))
			skipOperate[i] = true
			batchRecords[i] = aerospike.NewBatchRead(nil, placeholderKey, nil)
			continue
		}

		putOps := make([]*aerospike.Operation, len(binsToStore[0]))
		for j, bin := range binsToStore[0] {
			putOps[j] = aerospike.PutOp(bin)
		}

		// Conflicting is always false for BatchCreate (see above), so no
		// DeleteAtHeight bin is needed here.

		batchRecords[i] = aerospike.NewBatchWrite(batchWritePolicy, key, putOps...)
	}

	batchOperate := s.batchOperateFn
	if batchOperate == nil {
		batchOperate = s.client.BatchOperate
	}

	if err := batchOperate(batchPolicy, batchRecords); err != nil {
		var aErr *aerospike.AerospikeError
		if errors.As(err, &aErr) && aErr.ResultCode == types.KEY_EXISTS_ERROR {
			// Whole-call KEY_EXISTS_ERROR: map to per-record TxExistsError for
			// every entry that hasn't already been resolved.
			for i, tx := range txs {
				if skipOperate[i] {
					continue
				}
				results[i].Err = errors.NewTxExistsError("[BatchCreate] tx %s already exists in store", tx.TxIDChainHash())
			}
			return results, nil
		}

		return nil, errors.NewStorageError("[BatchCreate] BatchOperate failed", err)
	}

	// Map per-record results back to the input indexes.
	for i := range txs {
		if skipOperate[i] {
			continue
		}

		rec := batchRecords[i].BatchRec()
		if rec.Err == nil {
			// Success — results[i].Err stays nil.
			continue
		}

		if aErr, ok := rec.Err.(*aerospike.AerospikeError); ok {
			switch aErr.ResultCode {
			case types.KEY_EXISTS_ERROR:
				results[i].Err = errors.NewTxExistsError("[BatchCreate] tx %s already exists in store", txs[i].TxIDChainHash())
				continue
			case types.KEY_NOT_FOUND_ERROR:
				// KEY_NOT_FOUND on the NOOP placeholder BatchRead — should not
				// happen because we only reach this branch when skipOperate[i]
				// is false (i.e. a real BatchWrite was enqueued). Guard anyway.
				results[i].Err = errors.NewStorageError("[BatchCreate] unexpected KEY_NOT_FOUND for tx %s", txs[i].TxIDChainHash())
				continue
			}
		}

		results[i].Err = errors.NewStorageError("[BatchCreate] error creating tx %s: %v", txs[i].TxIDChainHash(), rec.Err)
	}

	// Emit the metric for each successfully created record.
	for i := range results {
		if results[i].Err == nil && !skipOperate[i] {
			prometheusUtxostoreCreate.Inc()
		}
	}

	return results, nil
}

package aerospike

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
)

// batchFinalize represents a request to clear the tentative "creating" state of a
// transaction (create-first phase 2). Batched across transactions so the common
// single-record case coalesces into one BatchOperate.
type batchFinalize struct {
	ctx       context.Context
	tx        *bt.Tx
	group     *completion.Group
	completed atomic.Bool
	result    error // written by the CAS winner, after the CAS and before group.Done(); see complete
	// onError, if set, is invoked the first time this item completes with a
	// non-nil error, so FinalizeTransaction can cancel its wait immediately.
	onError func(error)
}

// complete writes err into the item's result slot and marks the shared group's
// completion counter. Idempotent (CAS-guarded), mirroring batchLocked.complete.
func (b *batchFinalize) complete(err error) {
	if b.completed.CompareAndSwap(false, true) {
		b.result = err
		if err != nil && b.onError != nil {
			b.onError(err)
		}
		b.group.Done()
	}
}

// FinalizeTransaction clears the tentative creating state set by
// Create(..., WithCreating(true)), making the transaction's outputs spendable.
// Idempotent: finalizing an already-final tx (creating bin already absent) is a
// no-op success. Returns ErrTxNotFound if the record has vanished.
func (s *Store) FinalizeTransaction(ctx context.Context, tx *bt.Tx) error {
	if tx == nil {
		return errors.NewProcessingError("[FinalizeTransaction] nil tx")
	}

	group := completion.NewGroup(1)

	// Cancel the wait the moment this item fails, matching SetLocked's fail-fast.
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mu       sync.Mutex
		firstErr error
	)

	item := &batchFinalize{
		ctx:   ctx,
		tx:    tx,
		group: group,
		onError: func(err error) {
			mu.Lock()
			if firstErr == nil {
				firstErr = err
				cancel()
			}
			mu.Unlock()
		},
	}

	s.finalizeBatcher.PutCtx(ctx, item)

	waitErr := group.Wait(waitCtx, s.batcherWait)

	// Parent-context cancellation takes precedence: surface the raw context error.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	mu.Lock()
	fe := firstErr
	mu.Unlock()

	if fe != nil {
		return fe
	}

	if waitErr != nil {
		return errors.NewServiceUnavailableError("finalize transaction did not complete within %s", s.batcherWait)
	}

	return nil
}

// finalizeRecordCount returns the number of Aerospike records a tx spans, mirroring
// splitIntoBatches: ceil(len(outputs)/utxoBatchSize), at least one record.
func (s *Store) finalizeRecordCount(tx *bt.Tx) int {
	n := len(tx.Outputs)
	if n == 0 || s.utxoBatchSize <= 0 {
		return 1
	}

	return (n + s.utxoBatchSize - 1) / s.utxoBatchSize
}

// sendFinalizeBatch clears the creating bin for a batch of transactions. Single-record
// txs (the overwhelmingly common case) are cleared in one BatchOperate over the whole
// batch; multi-record txs are handled inline via clearCreatingFlag, which already
// implements the children-first-then-master ordering and FILTERED_OUT idempotency.
func (s *Store) sendFinalizeBatch(batch []*batchFinalize) {
	// go-batcher recovers panics in this fn; re-complete every item on panic so a
	// crash cannot orphan the waiting submitters (complete is CAS-guarded).
	defer func() {
		signalBatchPanic(recover(), batch, "sendFinalizeBatch", s.logger, func(it *batchFinalize, err error) {
			it.complete(err)
		})
	}()

	filterExp := aerospike.ExpBinExists(fields.Creating.String())

	var (
		singleRecords []aerospike.BatchRecordIfc
		singleOwner   []int // singleRecords[k] belongs to batch[singleOwner[k]]
	)

	for idx, item := range batch {
		numRecords := s.finalizeRecordCount(item.tx)

		if numRecords > 1 {
			// Multi-record: reuse the create-path phase-2 clearer (children first,
			// master last) so the master's creating flag stays the atomic completion
			// indicator, exactly as at create time.
			item.complete(s.clearCreatingFlag(item.tx.TxIDChainHash(), numRecords))
			continue
		}

		keySource := uaerospike.CalculateKeySourceInternal(item.tx.TxIDChainHash(), 0)

		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			item.complete(errors.NewProcessingError("[sendFinalizeBatch] failed to create key for %s", item.tx.TxIDChainHash().String(), err))
			continue
		}

		writePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
		writePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
		writePolicy.FilterExpression = filterExp // only touch records that still carry the creating bin

		// Clear by writing nil (bin absence == not creating).
		op := aerospike.PutOp(aerospike.NewBin(fields.Creating.String(), nil))

		singleRecords = append(singleRecords, aerospike.NewBatchWrite(writePolicy, key, op))
		singleOwner = append(singleOwner, idx)
	}

	if len(singleRecords) == 0 {
		return
	}

	if err := s.batchOperate(util.GetAerospikeBatchPolicy(s.settings), singleRecords); err != nil {
		for _, idx := range singleOwner {
			batch[idx].complete(errors.NewProcessingError("[sendFinalizeBatch] BatchOperate failed", err))
		}

		return
	}

	for k, rec := range singleRecords {
		idx := singleOwner[k]

		recErr := rec.BatchRec().Err
		if recErr == nil {
			batch[idx].complete(nil)
			continue
		}

		aErr, ok := recErr.(*aerospike.AerospikeError)
		switch {
		case ok && aErr.ResultCode == types.FILTERED_OUT:
			// Creating bin already absent → already finalized. Idempotent success.
			batch[idx].complete(nil)
		case ok && aErr.ResultCode == types.KEY_NOT_FOUND_ERROR:
			// The record vanished between create and finalize — surface it so the
			// caller (validator roll-forward / sweeper) knows the state is broken.
			batch[idx].complete(errors.NewTxNotFoundError("[sendFinalizeBatch] record not found for tx %s", batch[idx].tx.TxIDChainHash().String()))
		default:
			batch[idx].complete(errors.NewProcessingError("[sendFinalizeBatch] failed to finalize tx %s", batch[idx].tx.TxIDChainHash().String(), recErr))
		}
	}
}

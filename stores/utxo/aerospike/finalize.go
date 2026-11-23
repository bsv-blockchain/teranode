package aerospike

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
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
	attempt   []byte
	lease     []byte
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
//
// It returns nil only to the caller whose write cleared the state, and that caller owns the
// hand-off of the transaction. The write records who made it: lease is the spend lease the caller
// holds on the attempt, the attempt token for the call that created the record and the lease of
// TakeSpendLease for a roll-forward, and the write puts it in the attempt bin of the master. A
// call that finds the record final reads that bin. Its own lease there means that an earlier send
// of this same finalize cleared the state, with its answer lost, and the call returns nil. Any
// other value means that another actor did, and the call returns an ErrTxExists for which
// utxo.IsAlreadyFinal is true. A repeat of a finalize is therefore safe, and exactly one caller
// gets nil. A caller with no lease passes nil and is never told that the write was its own.
//
// The write removes the caller's lease from the spenders bin and leaves the leases of other
// spenders, so a delete of the record is refused while one of them may still spend.
//
// attempt is the token of the attempt the caller created or read: the clear is
// filtered on it, on the master and on every pagination child, so a finalize left over from an
// earlier attempt of the same txid cannot clear a later attempt's creating state. Returns
// ErrTxNotFound if the record has vanished, is rolling back (BeginCreateFirstRollback) or
// belongs to another attempt: all three mean this attempt's record will not be completed, and
// the owner of the rollback reverses the caller's spends of its inputs once the caller has released
// its spend lease. A batch that completed cleanly returns nil even if
// ctx was cancelled afterwards, because the record is finalized. Otherwise a cancelled ctx returns
// its context error, then a recorded batch error, then a timeout error.
func (s *Store) FinalizeTransaction(ctx context.Context, tx *bt.Tx, attempt, lease []byte) error {
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
		ctx:     ctx,
		tx:      tx,
		attempt: attempt,
		lease:   lease,
		group:   group,
		onError: func(err error) {
			mu.Lock()
			if firstErr == nil {
				firstErr = err
				cancel()
			}
			mu.Unlock()
		},
	}

	if putErr := safeBatcherPutCtx(s.finalizeBatcher, ctx, item, "FinalizeTransaction"); putErr != nil {
		return putErr
	}

	waitErr := group.Wait(waitCtx, s.batcherWait)

	mu.Lock()
	fe := firstErr
	mu.Unlock()

	if waitErr == nil && fe == nil {
		return nil
	}

	// Parent-context cancellation takes precedence over a recorded error: surface the raw context error.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	if fe != nil {
		return fe
	}

	return errors.NewServiceUnavailableError("finalize transaction did not complete within %s", s.batcherWait)
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

// removeUnminedSinceIfMinedOp returns the master-record write that removes unminedSince
// when the record has block IDs. A creating record created with mined block info carries
// unminedSince so the sweep can find it; once it is no longer creating it is an ordinary
// mined record and must not be reported as unmined. A record with no block IDs, or no
// unminedSince, is left alone: the unknown branch with ExpWriteFlagEvalNoFail makes the
// write a no-op.
func removeUnminedSinceIfMinedOp() *aerospike.Operation {
	return aerospike.ExpWriteOp(
		fields.UnminedSince.String(),
		aerospike.ExpCond(
			aerospike.ExpGreater(
				aerospike.ExpListSize(aerospike.ExpListBin(fields.BlockIDs.String())),
				aerospike.ExpIntVal(0),
			), aerospike.ExpNilValue(),
			aerospike.ExpUnknown(),
		),
		aerospike.ExpWriteFlagAllowDelete|aerospike.ExpWriteFlagEvalNoFail,
	)
}

// sendFinalizeBatch clears the creating bin for a batch of transactions. Single-record
// txs (the overwhelmingly common case) are cleared in one BatchOperate per deadline group;
// multi-record txs are handled inline via clearCreatingFlag, which already
// implements the children-first-then-master ordering and FILTERED_OUT idempotency.
//
// An item whose caller has given up is completed with that error and is not sent. Without that, the
// write of a finalize that its caller abandoned would still land and clear the creating bin, and
// stamp the record with the lease of a caller that already returned an error: the record would be
// final, its inputs spent, and no caller would own the hand-off to block assembly. A write that is
// sent carries only the time its callers have left (see finalizeGroups), so none starts after the
// deadline of its caller.
func (s *Store) sendFinalizeBatch(batch []*batchFinalize) {
	// go-batcher recovers panics in this fn; re-complete every item on panic so a
	// crash cannot orphan the waiting submitters (complete is CAS-guarded).
	defer func() {
		signalBatchPanic(recover(), batch, "sendFinalizeBatch", s.logger, func(it *batchFinalize, err error) {
			it.complete(err)
		})
	}()

	var (
		singles       []*batchFinalize
		singleRecords []aerospike.BatchRecordIfc // singleRecords[k] is the write of singles[k]
	)

	for _, item := range batch {
		if err := item.ctx.Err(); err != nil {
			item.complete(err)

			continue
		}

		numRecords := s.finalizeRecordCount(item.tx)

		if numRecords > 1 {
			// Multi-record: reuse the create-path phase-2 clearer (children first,
			// master last) so the master's creating flag stays the atomic completion
			// indicator, exactly as at create time.
			item.complete(s.clearCreatingFlag(item.tx.TxIDChainHash(), numRecords, item.attempt, item.lease))
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
		writePolicy.FilterExpression = finalizeFilter(item.attempt) // only touch records that still carry the creating bin and the token and are not rolling back

		singles = append(singles, item)
		singleRecords = append(singleRecords, aerospike.NewBatchWrite(writePolicy, key, finalizeMasterOps(item.attempt, item.lease)...))
	}

	for _, group := range finalizeGroups(singles) {
		s.sendFinalizeGroup(singles, singleRecords, group)
	}
}

// finalizeGroups splits single-record finalizes into groups that are sent apart: the items with no
// deadline, and the items with one, grouped by the second their deadline falls in. A write is
// bounded by the earliest deadline of its group, so no item is limited by a deadline more than a
// second shorter than its own, and a caller that is nearly out of time cannot shorten the budget of
// an unrelated one. Each group holds indexes into the items.
func finalizeGroups(items []*batchFinalize) [][]int {
	byBucket := make(map[time.Time][]int)

	var buckets []time.Time

	for i, item := range items {
		bucket := deadlineBucket(item.ctx)

		if _, seen := byBucket[bucket]; !seen {
			buckets = append(buckets, bucket)
		}

		byBucket[bucket] = append(byBucket[bucket], i)
	}

	groups := make([][]int, 0, len(buckets))

	for _, bucket := range buckets {
		groups = append(groups, byBucket[bucket])
	}

	return groups
}

// deadlineBucket is the second the deadline of ctx falls in, and the zero time for a context with
// none.
func deadlineBucket(ctx context.Context) time.Time {
	deadline, ok := ctx.Deadline()
	if !ok {
		return time.Time{}
	}

	return deadline.Truncate(time.Second)
}

// sendFinalizeGroup writes the finalizes of one group and completes their items.
func (s *Store) sendFinalizeGroup(items []*batchFinalize, records []aerospike.BatchRecordIfc, group []int) {
	groupRecords := make([]aerospike.BatchRecordIfc, len(group))

	var earliest time.Time

	for k, idx := range group {
		groupRecords[k] = records[idx]

		if deadline, ok := items[idx].ctx.Deadline(); ok && (earliest.IsZero() || deadline.Before(earliest)) {
			earliest = deadline
		}
	}

	writeCtx, cancel := context.Background(), context.CancelFunc(func() {})
	if !earliest.IsZero() {
		writeCtx, cancel = context.WithDeadline(context.Background(), earliest)
	}

	err := s.batchOperateWithin(writeCtx, util.GetAerospikeBatchPolicy(s.settings), groupRecords)

	cancel()

	if err != nil {
		for _, idx := range group {
			items[idx].complete(errors.NewProcessingError("[sendFinalizeBatch] BatchOperate failed", err))
		}

		return
	}

	for k, idx := range group {
		item := items[idx]

		recErr := groupRecords[k].BatchRec().Err
		if recErr == nil {
			item.complete(nil)
			continue
		}

		aErr, ok := recErr.(*aerospike.AerospikeError)
		switch {
		case ok && aErr.ResultCode == types.FILTERED_OUT:
			// Creating bin already absent → already finalized, idempotent success. A rolling
			// back record, or one owned by another attempt, is refused.
			item.complete(s.finalizeFilteredOutResult(item.tx.TxIDChainHash(), item.attempt, item.lease))
		case ok && aErr.ResultCode == types.KEY_NOT_FOUND_ERROR:
			// The record vanished between create and finalize — surface it so the
			// caller (validator roll-forward / sweeper) knows the state is broken.
			item.complete(errors.NewTxNotFoundError("[sendFinalizeBatch] record not found for tx %s", item.tx.TxIDChainHash().String()))
		default:
			item.complete(errors.NewProcessingError("[sendFinalizeBatch] failed to finalize tx %s", item.tx.TxIDChainHash().String(), recErr))
		}
	}
}

// finalizeFilter selects a record a finalize of the attempt that carries the token may clear:
// still creating, not rolling back, and written by that attempt.
func finalizeFilter(attempt []byte) *aerospike.Expression {
	return aerospike.ExpAnd(
		aerospike.ExpBinExists(fields.Creating.String()),
		aerospike.ExpNot(aerospike.ExpBinExists(fields.RollingBack.String())),
		attemptFilter(attempt),
	)
}

// attemptFilter selects a record that carries the token. A record with no attempt bin, one that
// predates the token, matches a nil token only.
func attemptFilter(attempt []byte) *aerospike.Expression {
	if len(attempt) == 0 {
		return aerospike.ExpNot(aerospike.ExpBinExists(fields.Attempt.String()))
	}

	return aerospike.ExpAnd(
		aerospike.ExpBinExists(fields.Attempt.String()),
		aerospike.ExpEq(aerospike.ExpBlobBin(fields.Attempt.String()), aerospike.ExpBlobVal(attempt)),
	)
}

// finalizeMasterOps returns the writes of a finalize on the master record: the creating bin goes,
// unminedSince goes from a mined record, and the write records its caller. A caller that holds a
// lease puts it in the attempt bin, which no write filters on once the record is final, and takes
// only its own lease out of the spenders bin. A caller with no lease has none to take out, and
// the leases of the spenders of the record stay until they are released or stale.
func finalizeMasterOps(attempt, lease []byte) []*aerospike.Operation {
	ops := []*aerospike.Operation{
		// Clear by writing nil (bin absence == not creating).
		aerospike.PutOp(aerospike.NewBin(fields.Creating.String(), nil)),
	}

	if len(lease) > 0 {
		if !bytes.Equal(lease, attempt) {
			ops = append(ops, aerospike.PutOp(aerospike.NewBin(fields.Attempt.String(), lease)))
		}

		ops = append(ops, removeSpendLeaseOps(lease)...)
	}

	return append(ops, removeUnminedSinceIfMinedOp())
}

// finalizeFilteredOutResult reads the master record after a finalize write was filtered out
// and returns the finalize result. A record that is final carries the lease of the caller whose
// write cleared the creating bin: when that is the lease of this call, an earlier send of its own
// write did it and the result is nil. Otherwise another actor did, and the result is an error
// marked with utxo.MarkAlreadyFinal. It is ErrTxNotFound when the record is rolling back, has
// vanished or is creating under another attempt.
func (s *Store) finalizeFilteredOutResult(txHash *chainhash.Hash, attempt, lease []byte) error {
	key, err := aerospike.NewKey(s.namespace, s.setName, txHash[:])
	if err != nil {
		return errors.NewProcessingError("[finalizeFilteredOutResult] failed to create key for %s", txHash.String(), err)
	}

	state, getErr := s.readCreatingState(key)
	if getErr != nil {
		return errors.NewProcessingError("[sendFinalizeBatch] failed to read tx %s after the finalize was filtered out", txHash.String(), getErr)
	}

	switch {
	case state.absent:
		return errors.NewTxNotFoundError("[sendFinalizeBatch] record not found for tx %s", txHash.String())
	case state.rollingBack:
		return errors.NewTxNotFoundError("[sendFinalizeBatch] tx %s is rolling back", txHash.String())
	case !state.creating && len(lease) > 0 && bytes.Equal(state.attempt, lease):
		return nil
	case !state.creating:
		return utxo.MarkAlreadyFinal(errors.NewTxExistsError("[sendFinalizeBatch] tx %s is already final", txHash.String()))
	case !bytes.Equal(state.attempt, attempt):
		return errors.NewTxNotFoundError("[sendFinalizeBatch] tx %s is creating under another attempt", txHash.String())
	}

	return errors.NewProcessingError("[sendFinalizeBatch] finalize of tx %s was filtered out but the record is still creating", txHash.String())
}

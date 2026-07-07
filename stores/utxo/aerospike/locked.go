package aerospike

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
)

// batchLocked represents a batch operation to set the locked flag on a transaction
type batchLocked struct {
	ctx        context.Context
	txHash     chainhash.Hash
	childIndex uint32 // This will default to 0 which is the master record
	setValue   bool
	group      *completion.Group
	completed  atomic.Bool
	result     error // written by the CAS winner, after the CAS and before group.Done(); see complete
	// onError, if set, is invoked with the result the first time this item
	// completes with a non-nil error. SetLocked uses it to fail-fast (cancel the
	// shared wait on the first failing hash); nil for callers that don't need it.
	onError func(error)
}

// complete writes err into the item's result slot and marks the shared
// group's completion counter. Idempotent: only the first call has any
// effect, so a panic-recovery sweep over an already-completed item (or the
// deferred child-record signal below) never double-signals or races a
// second write into result.
func (b *batchLocked) complete(err error) {
	if b.completed.CompareAndSwap(false, true) {
		b.result = err
		if err != nil && b.onError != nil {
			b.onError(err)
		}
		b.group.Done()
	}
}

func (s *Store) SetLocked(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	items := make([]*batchLocked, len(txHashes))
	group := completion.NewGroup(int32(len(txHashes)))

	// Restore the errgroup.WithContext fail-fast: cancel the wait the moment the
	// first hash fails, so a partial failure returns immediately instead of
	// blocking on the remaining in-flight hashes. Only the caller-side wait is
	// cancelled — waitCtx is never handed to the dispatcher, so in-flight
	// Aerospike writes for the other hashes are left to finish; we just stop
	// waiting on them. The first captured error is returned (matching
	// errgroup.Wait's "first non-nil error" contract).
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mu       sync.Mutex
		firstErr error
	)

	failFast := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		mu.Unlock()
	}

	for idx, txHash := range txHashes {
		items[idx] = &batchLocked{
			ctx:      ctx,
			txHash:   txHash,
			setValue: setValue,
			group:    group,
			onError:  failFast,
		}
	}

	// Enqueue all hashes as one ordered group via a single PutBatchCtx, instead
	// of one PutCtx per hash — cutting the per-item channel-send and collector-
	// select overhead to a single operation for the whole set.
	s.lockedBatcher.PutBatchCtx(ctx, items)

	// s.batcherWait <= 0 means unbounded (ctx-only) — Group.Wait treats a
	// non-positive timeout the same way. waitCtx cancellation (from failFast on
	// the first failing hash, or from the parent ctx) also unblocks the wait.
	waitErr := group.Wait(waitCtx, s.batcherWait)

	// Parent-context cancellation takes precedence: surface the raw context
	// error, not a wrapped teranode error type (matches the previous behavior).
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	// Fail-fast: the first failing hash captured its error and cancelled the
	// wait. Every failing complete() runs failFast, so this also covers the
	// all-completed case where a hash failed.
	mu.Lock()
	fe := firstErr
	mu.Unlock()

	if fe != nil {
		return fe
	}

	// No hash failed and the parent ctx is fine, so a non-nil waitErr is the
	// batcherWait timeout.
	if waitErr != nil {
		return errors.NewServiceUnavailableError("set locked did not complete within %s", s.batcherWait)
	}

	return nil
}

// setLockedBatch sets the locked flag on the given transactions in a batch.
//
// Child/extra records of a multi-record (externalised) tx are written inline
// here rather than re-queued into the lockedBatcher. Re-enqueuing from inside
// the batcher's own callback panics ("send on closed channel") and deadlocks
// during a draining Close — the worker that would service the re-queued item is
// the very one shutting down. Handling children inline (one extra BatchOperate)
// mirrors how the create path writes a tx's extra/external records, and keeps
// the lockedBatcher free of self-referential edges so Close can drain it safely.
func (s *Store) setLockedBatch(batch []*batchLocked) {
	// go-batcher recovers panics in this fn; re-complete every item on panic so a
	// crash (e.g. in ParseLuaMapResponse) cannot orphan the waiting submitters.
	// complete is CAS-guarded, so this never double-signals an item some earlier
	// stage already completed.
	defer func() {
		signalBatchPanic(recover(), batch, "setLockedBatch", s.logger, func(it *batchLocked, err error) {
			it.complete(err)
		})
	}()

	var (
		batchUDFPolicy = aerospike.NewBatchUDFPolicy()
		batchRecords   = make([]aerospike.BatchRecordIfc, len(batch))
		handled        = make([]bool, len(batch))
	)

	// Go through each batch item and set the tx to be locked
	for idx, batchItem := range batch {
		// We will do the master record first...
		keySource := uaerospike.CalculateKeySourceInternal(&batchItem.txHash, batchItem.childIndex)

		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			// Previously this called os.Exit(1), turning a recoverable key error
			// into a process crash. Surface it to the caller and keep the batch
			// index aligned with a NOOP placeholder instead.
			var keyErr error = errors.NewProcessingError("[setLockedBatch] failed to create key", err)
			batchItem.complete(keyErr)

			handled[idx] = true
			batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)

			continue
		}

		batchRecords[idx] = aerospike.NewBatchUDF(
			batchUDFPolicy,
			key,
			LuaPackage,
			"setLocked",
			aerospike.NewValue(batchItem.setValue),
		)
	}

	if err := s.batchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords); err != nil {
		for idx, batchItem := range batch {
			if handled[idx] {
				continue
			}

			var sendErr error = errors.NewProcessingError("could not batch write locked flag", err)
			batchItem.complete(sendErr)
		}

		return
	}

	// Process master results. Items reporting child/extra records defer their
	// completion to the inline child pass below (tracked via childErr, one
	// terminal result per item so each item is completed exactly once).
	childErr := make(map[int]error)

	var (
		childRecords []aerospike.BatchRecordIfc
		childOwner   []int // childRecords[k] belongs to batch[childOwner[k]]
	)

	for idx, batchRecord := range batchRecords {
		if handled[idx] {
			continue
		}

		if recErr := batchRecord.BatchRec().Err; recErr != nil {
			var sendErr error = errors.NewProcessingError("could not batch write locked flag", recErr)
			batch[idx].complete(sendErr)

			continue
		}

		response := batchRecord.BatchRec().Record
		if response == nil || response.Bins == nil || response.Bins[LuaSuccess.String()] == nil {
			// Previously this fell through without signalling — orphaning the
			// submitter on any nil/missing-bin response. Signal an error instead.
			var sendErr error = errors.NewProcessingError("setLocked: missing response for %s", batch[idx].txHash.String())
			batch[idx].complete(sendErr)

			continue
		}

		res, err := s.ParseLuaMapResponse(response.Bins[LuaSuccess.String()])
		if err != nil {
			var sendErr error = errors.NewProcessingError("could not parse response", err)
			batch[idx].complete(sendErr)

			continue
		}

		if res.Status != LuaStatusOK {
			if res.ErrorCode == LuaErrorCodeTxNotFound {
				var sendErr error = errors.NewTxNotFoundError("transaction not found: %s", batch[idx].txHash.String())
				batch[idx].complete(sendErr)
			} else {
				var sendErr error = errors.NewProcessingError("error from setLocked: %s", res.Message)
				batch[idx].complete(sendErr)
			}

			continue
		}

		extraRecords := res.ChildCount
		if extraRecords == 0 {
			batch[idx].complete(nil)

			continue
		}

		// Collect this item's child records for the inline batch below.
		childErr[idx] = nil

		for i := 1; i <= extraRecords; i++ {
			keySource := uaerospike.CalculateKeySourceInternal(&batch[idx].txHash, uint32(i)) // nolint:gosec

			key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
			if err != nil {
				childErr[idx] = errors.NewProcessingError("could not create child key for locked flag", err)
				break
			}

			childRecords = append(childRecords, aerospike.NewBatchUDF(
				batchUDFPolicy,
				key,
				LuaPackage,
				"setLocked",
				aerospike.NewValue(batch[idx].setValue),
			))
			childOwner = append(childOwner, idx)
		}
	}

	// Write all collected child records inline (no batcher re-entry, so this is
	// safe to run while the batcher is draining on Close). batchOperate shares the
	// same retry/short-circuit handling as the master batch above.
	if len(childRecords) > 0 {
		if err := s.batchOperate(util.GetAerospikeBatchPolicy(s.settings), childRecords); err != nil {
			for idx := range childErr {
				if childErr[idx] == nil {
					childErr[idx] = errors.NewProcessingError("could not batch write locked child records", err)
				}
			}
		} else {
			for k, childRecord := range childRecords {
				idx := childOwner[k]
				if childErr[idx] != nil {
					continue // already errored for this item
				}

				if childRecord.BatchRec().Err != nil {
					childErr[idx] = errors.NewProcessingError("could not write locked child record", childRecord.BatchRec().Err)
					continue
				}

				resp := childRecord.BatchRec().Record
				if resp == nil || resp.Bins == nil || resp.Bins[LuaSuccess.String()] == nil {
					continue
				}

				cres, perr := s.ParseLuaMapResponse(resp.Bins[LuaSuccess.String()])
				if perr != nil {
					childErr[idx] = errors.NewProcessingError("could not parse child response", perr)
				} else if cres.Status != LuaStatusOK {
					childErr[idx] = errors.NewProcessingError("error from setLocked child: %s", cres.Message)
				}
			}
		}
	}

	// Complete each child-bearing item exactly once with its terminal result.
	for idx, e := range childErr {
		batch[idx].complete(e)
	}
}

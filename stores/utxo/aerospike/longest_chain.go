package aerospike

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"golang.org/x/sync/errgroup"
)

// MarkTransactionsOnLongestChain updates unmined_since for transactions based on their chain status.
//
// This function is critical for maintaining data integrity during blockchain reorganizations.
// Uses a worker-pool pattern matching SetTxMined's proven production approach: a fixed number
// of worker goroutines each process sequential batches with pooled memory, avoiding the
// goroutine churn and allocation overhead of per-chunk goroutines.
//
// Behavior:
//   - onLongestChain=true: Clears unmined_since (transaction is mined on main chain)
//   - onLongestChain=false: Leaves an existing unmined_since untouched. Only when the bin is
//     absent (the transaction was mined and a reorg/reset moved its block off the longest
//     chain) is it stamped, with utxo.BackdatedUnminedSince: current height minus
//     unminedTxRetention, floored at 1.
//
// Both rules protect the same invariant: a parent must not be pruned while an unmined child
// still references it. The pruner's parent-preservation scan keys off unmined_since, while
// the parent's deleteAtHeight was stamped when this transaction spent it and keeps counting.
// Overwriting an existing unmined_since re-armed the child's protection clock against a
// parent delete clock that did not restart, and a late enough re-stamp let the parent be
// pruned while the child still needed it; stamping the current height on a mined->unmined
// transition started the protection clock unminedTxRetention blocks late for the same
// reason (issue 1768).
//
// CRITICAL - Resilient Error Handling (Must Not Fail Fast):
// This function attempts to update ALL transactions even if some fail.
//
// Error handling strategy:
//   - Processes ALL transactions concurrently (does not stop on first error)
//   - Collects up to 10 errors for logging/debugging (prevents log spam)
//   - Logs summary: attempted, succeeded, failed counts
//   - Returns aggregated errors after attempting all transactions
//   - onLongestChain=true: transactions the store does not hold are logged and skipped
//   - onLongestChain=false: a transaction the store does not hold is FATAL
//
// Missing on the longest chain: the transaction is mined and has no unminedSince
// to clear, and its record can be gone legitimately. A side-chain transaction whose
// outputs were all spent is pruned once its deleteAtHeight passes, and a reorg can
// still bring its block forward on a chain at height <= 1000 (reorgBlocks takes
// reorgs deeper than CoinbaseMaturity there) or with a short
// utxostore_blockHeightRetention. reorgBlocks calls this after its commit point,
// and exiting the process repaired nothing.
//
// Missing off the longest chain stays fatal: the transaction goes back into block
// assembly, and without a record its parents lose the pruner's protection.
//
// A pagination-record DAH write that still fails after its retry is returned as an error, and
// reorgBlocks turns it into a failed reorg that handleReorg answers with a full block assembly reset.
// That is deliberate: the reset reloads the unmined transactions and re-marks the ones the master
// restore (applyLongestChainChildDAH) left in a mined state with unmined_since set, so nothing is lost,
// at the price of one transient page failure costing an incremental reorg. Do not swallow this error.
func (s *Store) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	if len(txHashes) == 0 {
		return nil
	}

	allErrors := make([]error, 0, 10)
	firstMissing := make([]chainhash.Hash, 0, 10)
	var errorCount, missingCount, childDAHFailed int64
	var droppedErrors int // child errors beyond the allErrors cap, guarded by mu
	var mu sync.Mutex

	currentBlockHeight := s.GetBlockHeight()
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	batchWritePolicy := aerospike.NewBatchWritePolicy()
	batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY

	batchSize := s.settings.UtxoStore.MaxMinedBatchSize
	numChunks := (len(txHashes) + batchSize - 1) / batchSize
	numWorkers := min(s.settings.UtxoStore.MaxMinedRoutines, numChunks)

	// Pre-create the shared operations: identical for every transaction (same bin names, same
	// values). The aerospike *Operation struct is immutable after creation and only read during
	// serialization, so sharing one slice across all batch writes is safe.
	retention := s.settings.GetUtxoStoreBlockHeightRetention()
	restampDAH := onLongestChain && retention > 0

	var ops []*aerospike.Operation

	if onLongestChain {
		unminedOp := aerospike.PutOp(aerospike.NewBin(fields.UnminedSince.String(), nil))

		if restampDAH {
			// The pruner clears the DAH of unmined records, and txs validated while their block was on
			// a fork get none from setMined, so a tx brought onto the longest chain here must get its
			// DAH again. This is the common case in reorgBlocks (the txs of the blocks moved forward),
			// not only the BlockAssembler repair path.
			//
			// The old and new DAH are read around the write so pagination records can follow the master.
			ops = append(ops,
				aerospike.GetBinOp(fields.DeleteAtHeight.String()),
				aerospike.GetBinOp(fields.External.String()),
				aerospike.GetBinOp(fields.TotalExtraRecs.String()),
				aerospike.GetBinOp(fields.UnminedSince.String()), // old value, for a restore
				unminedOp,
			)
			// +1: the store height trails the block being validated, the allowance the spend path makes
			ops = append(ops, s.buildDeleteAtHeightOps(currentBlockHeight+1, retention, true)...)
			ops = append(ops, aerospike.GetBinOp(fields.DeleteAtHeight.String()))
		} else {
			ops = []*aerospike.Operation{unminedOp}
		}
	} else {
		// Only stamp unminedSince when the bin is absent. ExpUnknown with EvalNoFail makes the
		// existing-value branch a no-op, so an earlier unminedSince is never overwritten.
		unminedSince := utxo.BackdatedUnminedSince(currentBlockHeight, s.settings.UtxoStore.UnminedTxRetention)

		ops = []*aerospike.Operation{aerospike.ExpWriteOp(
			fields.UnminedSince.String(),
			aerospike.ExpCond(
				aerospike.ExpBinExists(fields.UnminedSince.String()), aerospike.ExpUnknown(),
				aerospike.ExpIntVal(int64(unminedSince)),
			),
			aerospike.ExpWriteFlagEvalNoFail,
		)}
	}

	// Divide work evenly across workers (fewer goroutines, sequential batches within each)
	rangeSize := (len(txHashes) + numWorkers - 1) / numWorkers

	g, gCtx := errgroup.WithContext(ctx)

	for w := 0; w < numWorkers && w*rangeSize < len(txHashes); w++ {
		workerStart := w * rangeSize
		workerEnd := min(workerStart+rangeSize, len(txHashes))
		workerHashes := txHashes[workerStart:workerEnd]

		g.Go(func() error {
			// One set of batch slots per worker, reused by each of its sequential batches, so a
			// transaction costs no key, batch-write or hash allocation of its own (see longestChainSlots).
			slots := newLongestChainSlots(min(batchSize, len(workerHashes)), batchWritePolicy, ops)

			// states is only read for the records followUps queued, within the same batch.
			states := make(map[chainhash.Hash]masterState)

			for i := 0; i < len(workerHashes); i += batchSize {
				if gCtx.Err() != nil {
					return gCtx.Err()
				}

				batchEnd := min(i+batchSize, len(workerHashes))
				chunk := workerHashes[i:batchEnd]

				// batchHashes[j] is the hash of batchRecords[j] (a key that failed to build is left out)
				n := 0

				for idx := range chunk {
					if err := slots.set(n, &chunk[idx], s.namespace, s.setName); err != nil {
						atomic.AddInt64(&errorCount, 1)
						mu.Lock()
						if len(allErrors) < 10 {
							allErrors = append(allErrors, err)
						}
						mu.Unlock()
						continue
					}

					n++
				}

				if n == 0 {
					continue
				}

				batchRecords := slots.records[:n]
				batchHashes := slots.hashes[:n]

				if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
					count := int64(len(batchRecords))
					atomic.AddInt64(&errorCount, count)
					mu.Lock()
					if len(allErrors) < 10 {
						allErrors = append(allErrors, errors.NewProcessingError("could not batch operate longest chain flag", err))
					}
					mu.Unlock()
					continue // Don't fail fast — continue processing other batches
				}

				// Pagination records of external txs whose master DAH changed. Applied per batch, in
				// bounded chunks, so one worker never holds sum(totalExtraRecs) writes at once and the
				// follow-up still runs if another worker fails. A page missing its DAH is harmless: pages
				// are deleted by computed key together with their master when the master is pruned.
				//
				// followUps points into batchHashes, which the next batch overwrites, so it and states
				// must not outlive this iteration: applyLongestChainChildDAH below consumes both.
				var followUps childDAHFollowUps

				clear(states)

				// Check individual record results
				for j, batchRecord := range batchRecords {
					recErr := batchRecord.BatchRec().Err
					if recErr == nil {
						// collect queues nothing for a record that is not external, and states is read only
						// for queued records, so the parse is skipped for the rest (most transactions).
						if restampDAH && batchRecord.BatchRec().Record != nil && binsSayExternal(batchRecord.BatchRec().Record.Bins) {
							if state, perr := s.parseSetMinedState(batchRecord.BatchRec().Record.Bins); perr == nil {
								h := &batchHashes[j]
								followUps.collect(h, state)
								states[*h] = masterState{oldDAH: state.OldDeleteAtHeight, newDAH: state.NewDeleteAtHeight, oldUnminedSince: oldUnminedSinceFrom(batchRecord.BatchRec().Record.Bins)}
							}
						}

						continue
					}

					if errors.Is(recErr, aerospike.ErrKeyNotFound) {
						atomic.AddInt64(&missingCount, 1)
						mu.Lock()
						if len(firstMissing) < 10 {
							firstMissing = append(firstMissing, batchHashes[j])
						}
						mu.Unlock()

						continue
					}

					atomic.AddInt64(&errorCount, 1)
					mu.Lock()
					if len(allErrors) < 10 {
						s.logger.Errorf("[MarkTransactionsOnLongestChain] error %d: transaction %s: %v",
							atomic.LoadInt64(&errorCount), batchHashes[j], recErr)
						allErrors = append(allErrors, recErr)
					}
					mu.Unlock()
				}

				if err := s.applyLongestChainChildDAH(gCtx, &followUps, states, &childDAHFailed); err != nil {
					mu.Lock()
					if len(allErrors) < 10 {
						allErrors = append(allErrors, err)
					} else {
						droppedErrors++ // the cap must not hide a child failure
					}
					mu.Unlock()
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	attempted := len(txHashes)
	finalErrorCount := int(atomic.LoadInt64(&errorCount))
	finalMissingCount := int(atomic.LoadInt64(&missingCount))
	succeeded := attempted - finalErrorCount - finalMissingCount

	s.logger.Infof("[MarkTransactionsOnLongestChain] completed: attempted=%d, succeeded=%d, failed=%d, missing=%d, childDAHFailed=%d, onLongestChain=%t",
		attempted, succeeded, finalErrorCount, finalMissingCount, atomic.LoadInt64(&childDAHFailed), onLongestChain)

	if finalMissingCount > 0 {
		if !onLongestChain {
			s.logger.Fatalf("CRITICAL: %d missing transactions during MarkTransactionsOnLongestChain(false) - data integrity compromised, first: %v",
				finalMissingCount, firstMissing)
		} else {
			s.logger.Errorf("[MarkTransactionsOnLongestChain] skipped %d transactions the store does not hold, first: %v",
				finalMissingCount, firstMissing)
		}
	}

	if len(allErrors) > 0 {
		if finalErrorCount > 10 {
			s.logger.Errorf("[MarkTransactionsOnLongestChain] only returned first 10 of %d errors", finalErrorCount)
		}
		mu.Lock()
		dropped := droppedErrors
		mu.Unlock()

		if dropped > 0 {
			s.logger.Errorf("[MarkTransactionsOnLongestChain] %d further pagination-record DAH errors were not returned", dropped)
		}

		return errors.Join(allErrors...)
	}

	return nil
}

// applyLongestChainChildDAH applies the pagination-record follow-ups of one batch through the shared
// applyChildDAHFollowUps (unsets first, chunked, each chunk retried once).
//
// The master write already happened (DAH stamped or cleared, unmined_since cleared), and a retry of
// MarkTransactionsOnLongestChain would see old == new and queue nothing, so a child write that still
// fails must not be left behind:
//   - failed UNSET: the pages keep a stale DAH under a master that has none, so the DAH pruner could
//     delete them while the master is live;
//   - failed RAISE (X -> Y): the pages stay at the lower X while the master is at Y, so they could be
//     pruned before the master.
//
// Both restore the master with the shared restoreDAHBatch, in one write per master that puts back the
// previous DAH AND the previous unmined_since (the same write cleared it; a record with a stale DAH and
// no unmined_since would be invisible to the repair scan). The restore only applies while the record is
// still in the state this call left it in: DAH unchanged, unmined_since absent, not preserved. A failed
// first-time SET (nil -> X) only leaks: the master carries the DAH and the pages cannot be pruned
// early; it is reported, not rolled back. A page merely missing its DAH is harmless: pages are deleted
// with their master by computed key.
//
// Child failures are counted in childDAHFailed, not in the per-transaction error count. The returned
// error is never dropped by the caller's error cap (see its use).
func (s *Store) applyLongestChainChildDAH(ctx context.Context, f *childDAHFollowUps, st map[chainhash.Hash]masterState, childDAHFailed *int64) error {
	applied := s.applyChildDAHFollowUps(ctx, f)

	atomic.AddInt64(childDAHFailed, int64(len(applied.failedUnsets)+len(applied.failedSets)))

	failed := applied.failedUnsets
	for _, item := range applied.failedSets {
		if st[*item.TxID].oldDAH != nil {
			failed = append(failed, item)
		}
	}

	restores := make([]dahRestore, 0, len(failed))

	for _, item := range failed {
		id := *item.TxID

		key, err := aerospike.NewKey(s.namespace, s.setName, id[:])
		if err != nil {
			continue
		}

		m := st[id]

		dahBin := aerospike.NewBin(fields.DeleteAtHeight.String(), nil)
		if m.oldDAH != nil {
			dahBin = aerospike.NewBin(fields.DeleteAtHeight.String(), int(*m.oldDAH))
		}

		unminedBin := aerospike.NewBin(fields.UnminedSince.String(), nil)
		if m.oldUnminedSince != nil {
			unminedBin = aerospike.NewBin(fields.UnminedSince.String(), *m.oldUnminedSince)
		}

		restores = append(restores, dahRestore{
			ID:   id.String(),
			Key:  key,
			Bins: []*aerospike.Bin{dahBin, unminedBin},
			Filter: aerospike.ExpAnd(
				dahUnchangedExpression(m.newDAH),
				aerospike.ExpNot(aerospike.ExpBinExists(fields.UnminedSince.String())),
				aerospike.ExpNot(aerospike.ExpBinExists(fields.PreserveUntil.String())),
			),
		})
	}

	var restoreErr error
	if len(restores) > 0 {
		restoreErr = s.restoreDAHBatch(ctx, restores, prometheusTxMetaAerospikeLongestChainRestoreFailed)
	}

	if err := errors.JoinCapped(10, applied.unsetErr, applied.setErr, restoreErr); err != nil {
		return errors.NewProcessingError("could not update DAH of pagination records", err)
	}

	return nil
}

// masterState is a master's state before and after the longest-chain write, as read by that write.
type masterState struct {
	oldDAH, newDAH  *uint32
	oldUnminedSince *int
}

// oldUnminedSinceFrom extracts the unmined_since value read before the write (bare or as the first
// operation result); nil if the bin was absent.
func oldUnminedSinceFrom(bins aerospike.BinMap) *int {
	switch v := bins[fields.UnminedSince.String()].(type) {
	case int:
		return &v
	case aerospike.OpResults:
		if len(v) > 0 {
			if n, ok := v[0].(int); ok {
				return &n
			}
		}
	case []interface{}:
		if len(v) > 0 {
			if n, ok := v[0].(int); ok {
				return &n
			}
		}
	}

	return nil
}

// longestChainSlots is one worker's reusable batch for MarkTransactionsOnLongestChain. Slot k holds
// the hash of records[k] in hashes[k], and keys[k] reads that hash through values[k], so a later
// batch only copies a hash in and recomputes the key's digest in place (Key.SetValue).
//
// The mark writes one record per transaction of the blocks a reorg moves, hundreds of millions at
// scale. Allocating a key, its boxed value, a hash copy and a BatchWrite for each one fed the
// garbage collector of block assembly's very large heap, and the mark's goroutines spent their time
// in GC assist. Each slot now allocates once per call, on its first use.
//
// Reuse is safe because BatchOperate returns only after every node command has finished, and it
// resets each record's result fields (Record, ResultCode, Err, InDoubt) before sending, so a
// result never carries over from the previous batch. The slots are scoped to one call, so no key
// keeps a reference to memory after the call returns.
type longestChainSlots struct {
	hashes  []chainhash.Hash
	values  []aerospike.Value
	keys    []*aerospike.Key
	records []aerospike.BatchRecordIfc

	policy *aerospike.BatchWritePolicy
	ops    []*aerospike.Operation
}

func newLongestChainSlots(size int, policy *aerospike.BatchWritePolicy, ops []*aerospike.Operation) *longestChainSlots {
	return &longestChainSlots{
		hashes:  make([]chainhash.Hash, size),
		values:  make([]aerospike.Value, size),
		keys:    make([]*aerospike.Key, size),
		records: make([]aerospike.BatchRecordIfc, size),
		policy:  policy,
		ops:     ops,
	}
}

// set points slot k at hash. On an error the caller leaves the transaction out of the batch and
// gives slot k to the next transaction, whose set resets the slot's key again.
func (ls *longestChainSlots) set(k int, hash *chainhash.Hash, namespace, setName string) error {
	ls.hashes[k] = *hash

	if ls.keys[k] != nil {
		// values[k] already reads hashes[k]; only the digest changes.
		return ls.keys[k].SetValue(ls.values[k])
	}

	if ls.values[k] == nil {
		// Boxed once per slot: the slice header never changes, only the bytes it points at.
		ls.values[k] = aerospike.BytesValue(ls.hashes[k][:])
	}

	key, err := aerospike.NewKey(namespace, setName, ls.values[k])
	if err != nil {
		return err
	}

	ls.keys[k] = key
	// NewBatchWrite, not a struct literal: it sets the unexported flag that routes the record as a write.
	ls.records[k] = aerospike.NewBatchWrite(ls.policy, key, ls.ops...)

	return nil
}

// binsSayExternal reports whether a longest-chain write read back external=true, the only case in
// which childDAHFollowUps.collect queues anything (it also needs totalExtraRecs, checked there).
func binsSayExternal(bins aerospike.BinMap) bool {
	external, ok := bins[fields.External.String()].(bool)

	return ok && external
}

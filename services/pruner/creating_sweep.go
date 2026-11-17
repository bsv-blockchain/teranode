package pruner

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// defaultCreatingSweepMaxPerPass is the per-pass cap used when
// Pruner.CreatingTxSweepMaxPerPass is zero. The cap bounds both the store-side scan and
// the serial roll-forward loop so neither grows with the unmined backlog after a crash
// burst; any remainder is swept on the next block.
const defaultCreatingSweepMaxPerPass = 1000

// sweepCreatingTxs heals transactions abandoned in the create-first "creating" state. A
// record still creating is rolled forward: the sweep re-runs the input spends from the
// stored tx bytes and finalizes it. Spends are idempotent for the same spender, so
// re-spending inputs an earlier attempt already spent is safe. The roll-forward does not
// ignore locks: a locked parent is the hand-off barrier, so ErrTxLocked leaves the record
// creating and it is retried on a later pass. A tx that lost a double-spend (ErrSpent,
// ErrTxConflicting, ErrFrozen, ErrUtxoHashMismatch), or that spends a missing parent or a vout
// that does not exist (ErrTxNotFound), is rolled back by
// utxo.RollBackCreating: the record is marked rolling back, every input of the tx is
// unspent and the record becomes a hold that keeps its txid taken for a while. A record already
// marked rolling back was abandoned by a crashed rollback owner; the sweep finishes it (utxo.FinishRollBackCreating) by taking
// the hold of its attempt, unspending every input once no spender holds a live lease, and ending
// the rollback. A hold expires on its own; one that a namespace without record expiry keeps past
// its time is deleted here. A record marked rolling back that also carries the reversed marker has all its
// spends undone: the sweep ends its rollback without taking the hold, unspending or reading the
// transaction, which may be gone from the blob store. The sweep reads the record in two steps, the
// markers first and the transaction only when the record needs a roll-forward or a reversal. Records it cannot roll forward for another reason (e.g. a
// locked parent, or a finalize that fails) are left creating, gated (unspendable), and retried on a later pass. After finalizing, the
// sweep hands the tx to block assembly and only then unlocks it, the order
// loadUnminedTransactions uses. When the announce fails, or no block assembly client is
// configured, the record stays finalized and locked so the next unmined reload restores it.
// Runs whatever the value of utxostore_useCreateFirstOrder, so records stranded before the
// flag was turned off still heal.
func (s *Server) sweepCreatingTxs(ctx context.Context, blockHeight uint32) {
	minAge := s.settings.Pruner.CreatingTxSweepMinAgeBlocks
	if minAge == 0 || blockHeight <= minAge {
		return
	}

	maxPerPass := int(s.settings.Pruner.CreatingTxSweepMaxPerPass)
	if maxPerPass == 0 {
		maxPerPass = defaultCreatingSweepMaxPerPass
	}

	hashes, err := s.utxoStore.QueryStaleCreatingTxs(ctx, blockHeight-minAge, maxPerPass)
	if err != nil {
		s.logger.Errorf("[pruner][creating-sweep][%d] query failed: %v", blockHeight, err)
		return
	}

	prunerCreatingBacklog.Set(float64(len(hashes)))

	if len(hashes) == 0 {
		return
	}

	if len(hashes) == maxPerPass {
		s.logger.Infof("[pruner][creating-sweep][%d] hit per-pass cap of %d; remaining stale creating txs will be swept next block", blockHeight, maxPerPass)
	}

	s.logger.Infof("[pruner][creating-sweep][%d] healing %d stale creating txs", blockHeight, len(hashes))

	// Track progress so a page that is stuck for a persistent reason (e.g. ≥cap records
	// with a permanently locked parent) is distinguishable from "more remain,
	// caught up next block" — the reviewers' non-convergence concern.
	progressed := 0
	warnedNoBlockAssembly := false

	for _, hash := range hashes {
		hash := hash

		if ctx.Err() != nil {
			s.logger.Infof("[pruner][creating-sweep][%d] context cancelled, aborting sweep: %v", blockHeight, ctx.Err())
			return
		}

		md, err := s.utxoStore.Get(ctx, &hash, fields.Creating, fields.RollingBack, fields.Attempt, fields.Reversed)
		if err != nil {
			s.logger.Warnf("[pruner][creating-sweep][%d] get %s failed: %v", blockHeight, hash, err)
			continue
		}

		if md.Hold {
			cleared, clearErr := s.utxoStore.ClearCreateFirstHold(ctx, &hash, false)
			if clearErr != nil {
				s.logger.Errorf("[pruner][creating-sweep][%d] clearing the hold of %s failed, will retry next block: %v", blockHeight, hash, clearErr)
				prunerCreatingSweepFailures.Inc()

				continue
			}

			if cleared {
				progressed++

				s.logger.Infof("[pruner][creating-sweep][%d] deleted the expired hold of %s", blockHeight, hash)
			}

			continue
		}

		if !md.Creating {
			// Finalized between the query and this get.
			continue
		}

		if md.RollingBack && md.Reversed {
			if err = s.utxoStore.EndCreateFirstRollback(ctx, &hash, md.Attempt); err != nil {
				s.logger.Errorf("[pruner][creating-sweep][%d] ending the reversed rollback of %s failed, will retry next block: %v", blockHeight, hash, err)
				prunerCreatingSweepFailures.Inc()

				continue
			}

			progressed++

			s.logger.Infof("[pruner][creating-sweep][%d] ended the reversed rollback of %s", blockHeight, hash)

			continue
		}

		md, err = s.utxoStore.Get(ctx, &hash, fields.Tx, fields.Creating, fields.RollingBack, fields.Attempt, fields.Reversed, fields.Fee, fields.SizeInBytes, fields.TxInpoints)
		if err != nil {
			s.logger.Warnf("[pruner][creating-sweep][%d] get of the transaction of %s failed: %v", blockHeight, hash, err)
			continue
		}

		if !md.Creating || md.Tx == nil || md.Hold {
			// Finalized, replaced by a hold, or unreadable between the two gets.
			continue
		}

		if md.RollingBack && md.Reversed {
			continue
		}

		if md.RollingBack {
			finished, finishErr := utxo.FinishRollBackCreating(ctx, s.logger, s.utxoStore, md.Tx, md.Attempt, false)
			if finishErr != nil {
				s.logger.Errorf("[pruner][creating-sweep][%d] finishing rollback of %s failed, will retry next block: %v", blockHeight, hash, finishErr)
				prunerCreatingSweepFailures.Inc()

				continue
			}

			if finished {
				progressed++

				s.logger.Infof("[pruner][creating-sweep][%d] finished abandoned rollback of %s", blockHeight, hash)
			}

			continue
		}

		// Re-spend the inputs only (the record already exists in the creating state) and
		// finalize, through the shared roll-forward rule. Spends are idempotent for the same
		// spender, which makes roll-forward safe. Locks are not ignored: a locked parent is
		// the hand-off barrier, so ErrTxLocked is retried on a later pass rather than
		// bypassed. The rule takes a spend lease first, which the store refuses for a record
		// that is rolling back, and a rollback that begins later waits for the lease and then
		// reverses the spends this pass made.
		_, rfErr := utxo.RollForwardCreating(ctx, s.logger, s.utxoStore, md.Tx, md.Attempt, blockHeight)
		if rfErr != nil {
			if isUnwinnableSpendError(rfErr) {
				// A lease that is still live belongs to a spender that returned with an unknown
				// outcome or crashed. The rollback would wait for it, so the record is left for a
				// later pass, when the lease is stale.
				if live, liveErr := s.utxoStore.SpendLeasesLive(ctx, &hash, md.Attempt); liveErr != nil || live {
					s.logger.Warnf("[pruner][creating-sweep][%d] %s cannot be completed but a spender may still hold a lease on it, will retry next block: %v", blockHeight, hash, liveErr)
					prunerCreatingSweepFailures.Inc()

					continue
				}

				if rbErr := utxo.RollBackCreating(ctx, s.logger, s.utxoStore, md.Tx, md.Attempt); rbErr != nil {
					s.logger.Errorf("[pruner][creating-sweep][%d] rollback of %s failed, will retry next block: %v", blockHeight, hash, rbErr)
					prunerCreatingSweepFailures.Inc()

					continue
				}

				progressed++ // loser unspent and held

				continue
			}

			s.logger.Warnf("[pruner][creating-sweep][%d] roll-forward of %s failed, will retry next block: %v", blockHeight, hash, rfErr)
			prunerCreatingSweepFailures.Inc()

			continue
		}

		if s.blockAssemblyClient == nil {
			if !warnedNoBlockAssembly {
				s.logger.Warnf("[pruner][creating-sweep][%d] no block assembly client; rolled-forward txs are not announced and stay locked", blockHeight)

				warnedNoBlockAssembly = true
			}

			prunerCreatingSweepFailures.Inc()

			continue
		}

		if _, storeErr := s.blockAssemblyClient.Store(ctx, &hash, md.Fee, md.SizeInBytes, md.TxInpoints); storeErr != nil {
			s.logger.Errorf("[pruner][creating-sweep][%d] announce %s to block assembly failed, leaving it locked: %v", blockHeight, hash, storeErr)
			prunerCreatingSweepFailures.Inc()

			continue
		}

		// FinalizeTransaction clears creating but not the lock; the tentative create carried
		// WithLocked(true) on the block-assembly path, so without this the "rolled forward"
		// record stays locked=true and its outputs remain unspendable. Clear it (idempotent).
		if unlockErr := s.utxoStore.SetLocked(ctx, []chainhash.Hash{hash}, false); unlockErr != nil {
			s.logger.Errorf("[pruner][creating-sweep][%d] unlock %s failed: %v", blockHeight, hash, unlockErr)
			prunerCreatingSweepFailures.Inc()

			continue
		}

		progressed++

		s.logger.Infof("[pruner][creating-sweep][%d] rolled forward %s", blockHeight, hash)
	}

	// Zero progress on a non-empty page (especially a full one) means the page is stuck on
	// records that cannot converge — surface it as a warning rather than the misleading
	// "will catch up next block" info line above.
	if progressed == 0 {
		s.logger.Warnf("[pruner][creating-sweep][%d] rolled forward 0 of %d stale creating txs; the page may be stuck (e.g. a permanently locked parent)", blockHeight, len(hashes))
	}
}

// isUnwinnableSpendError reports whether a spend error means the tx can never be
// completed: an input is spent by another tx, conflicting, frozen, does not match or names
// an output that does not exist.
func isUnwinnableSpendError(err error) bool {
	return errors.Is(err, errors.ErrSpent) ||
		errors.Is(err, errors.ErrTxNotFound) ||
		errors.Is(err, errors.ErrTxConflicting) ||
		errors.Is(err, errors.ErrFrozen) ||
		errors.Is(err, errors.ErrUtxoHashMismatch)
}

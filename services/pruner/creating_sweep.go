package pruner

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
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
// ErrTxConflicting, ErrFrozen, ErrUtxoHashMismatch) is rolled back by
// utxo.RollBackCreating: the record is marked rolling back, every input of the tx is
// unspent and the record becomes a hold that keeps its txid taken for a while. A record already
// marked rolling back was abandoned by a crashed rollback owner; the sweep finishes it by taking
// the hold of its attempt, unspending every input and ending the rollback. A hold whose time has
// passed is deleted. A record marked rolling back that also carries the reversed marker has all its
// spends undone: the sweep ends its rollback without taking the hold, unspending or reading the
// transaction, which may be gone from the blob store. The sweep reads the record in two steps, the
// markers first and the transaction only when the record needs a roll-forward or a reversal. Records it cannot roll forward for another reason (e.g. a
// missing parent) are left creating, gated (unspendable), and retried on a later pass. After finalizing, the
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
	// with a permanently locked/missing parent) is distinguishable from "more remain,
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
			if err = s.finishRollback(ctx, md.Tx, md.Attempt); err != nil {
				s.logger.Errorf("[pruner][creating-sweep][%d] finishing rollback of %s failed, will retry next block: %v", blockHeight, hash, err)
				prunerCreatingSweepFailures.Inc()

				continue
			}

			progressed++

			s.logger.Infof("[pruner][creating-sweep][%d] finished abandoned rollback of %s", blockHeight, hash)

			continue
		}

		// Re-spend the inputs only (the record already exists in the creating state) and
		// finalize, through the shared roll-forward rule. Spends are idempotent for the same
		// spender, which makes roll-forward safe. Locks are not ignored: a locked parent is
		// the hand-off barrier, so ErrTxLocked is retried on a later pass rather than
		// bypassed. The rule refuses a record that is rolling back, and reverses its own
		// spends when the record is rolled back or deleted before the finalize lands.
		_, rfErr := utxo.RollForwardCreating(ctx, s.logger, s.utxoStore, md.Tx, md.Attempt, blockHeight)
		if rfErr != nil {
			if isUnwinnableSpendError(rfErr) {
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
		s.logger.Warnf("[pruner][creating-sweep][%d] rolled forward 0 of %d stale creating txs; the page may be stuck (e.g. a permanently locked or missing parent)", blockHeight, len(hashes))
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

// finishRollback completes a rollback a crashed owner left marked. It first takes the hold of
// the attempt that was rolled back. The take is exclusive and proves that the record still
// belongs to that attempt, so a second pruner, or one that read the record before a new attempt
// replaced it, takes nothing and reverses nothing. A failed take is logged and is not an error.
// With the hold taken, it unspends every input of tx under a context bounded by
// utxostore_spendRollbackTimeout, skipping inputs the tx can never have spent (a parent that is
// already gone, a vout that does not exist or a UTXO hash that does not match), releases the
// hold, and ends the rollback, which turns the record into a hold. Every step is idempotent. The
// record is rolling back, so nothing can finalize it meanwhile.
func (s *Server) finishRollback(ctx context.Context, tx *bt.Tx, attempt []byte) error {
	hash := tx.TxIDChainHash()

	took, err := s.utxoStore.TakeRollbackHold(ctx, hash, attempt)
	if err != nil {
		return err
	}

	if !took {
		s.logger.Infof("[pruner][creating-sweep] hold of %s not taken: the record is gone, owned by another attempt or busy, nothing reversed", hash)

		return nil
	}

	if err = s.unspendHeld(ctx, tx, attempt); err != nil {
		return err
	}

	return s.utxoStore.EndCreateFirstRollback(ctx, hash, attempt)
}

// unspendHeld reverses the spends of tx while the caller holds the hold of its attempt, then
// releases the hold. A reversal that fails still releases the hold, best effort, and returns the
// reversal error. A release that fails after a good reversal is an error too: the caller must not
// end the rollback, because that would record the spends as reversed over a busy marker that the
// conversion refuses, and the record would stay rolling back until the marker ages out.
func (s *Server) unspendHeld(ctx context.Context, tx *bt.Tx, attempt []byte) error {
	hash := tx.TxIDChainHash()

	reverseErr := s.reverseSpends(ctx, tx)

	relCtx, cancel := s.rollbackContext(context.WithoutCancel(ctx))
	defer cancel()

	relErr := s.utxoStore.ReleaseRollbackHold(relCtx, hash, attempt)
	if relErr != nil {
		s.logger.Errorf("[pruner][creating-sweep] releasing the hold of %s failed, it expires on its own: %v", hash, relErr)
	}

	if reverseErr != nil {
		return reverseErr
	}

	return relErr
}

// reverseSpends unspends every input of tx under a context bounded by the rollback bound.
func (s *Server) reverseSpends(ctx context.Context, tx *bt.Tx) error {
	spends, err := utxo.GetSpends(tx)
	if err != nil {
		return err
	}

	rbCtx, cancel := s.rollbackContext(ctx)
	defer cancel()

	return utxo.UnspendTolerantOfMissingParents(rbCtx, s.utxoStore, spends)
}

// rollbackContext bounds a reversal of spends by utxostore_spendRollbackTimeout, the life of the
// hold it runs under.
func (s *Server) rollbackContext(ctx context.Context) (context.Context, context.CancelFunc) {
	bound := s.settings.UtxoStore.SpendRollbackTimeout
	if bound <= 0 {
		return context.WithCancel(ctx)
	}

	return context.WithTimeout(ctx, bound)
}

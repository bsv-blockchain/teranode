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
// creating and it is retried on every pass. Only when pruner_creatingTxSweepBlockedAgeBlocks is set
// is such a record rolled back, and then only when it is older than that age and every parent that
// blocked it has been blocked that long too (blockedByParentsForLong). A tx that lost a double-spend (ErrSpent,
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

	// parentBlocked caches, for this pass, whether a parent has been locked or creating for at least
	// pruner_creatingTxSweepBlockedAgeBlocks. Many records can wait on one parent.
	parentBlocked := make(map[chainhash.Hash]bool)

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

		md, err = s.utxoStore.Get(ctx, &hash, fields.Tx, fields.Creating, fields.RollingBack, fields.Attempt, fields.Reversed, fields.Fee, fields.SizeInBytes, fields.TxInpoints, fields.UnminedSince)
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

		if len(md.Attempt) == 0 {
			// A creating record with no attempt token was not written by a create-first attempt.
			// The create that wrote it did not defer the spend of its inputs, and its clear of the
			// creating bin failed. There is no spend to repeat and nothing to roll back: the
			// sweep clears the bin, which is all that the record still needs. Its creator
			// reported it as stored, so it is not announced again.
			finErr := s.utxoStore.FinalizeTransaction(ctx, md.Tx, nil, nil)
			if finErr != nil && !utxo.IsAlreadyFinal(finErr) {
				s.logger.Warnf("[pruner][creating-sweep][%d] clearing the creating state of %s, which carries no attempt token, failed, will retry next block: %v", blockHeight, hash, finErr)
				prunerCreatingSweepFailures.Inc()

				continue
			}

			progressed++

			s.logger.Infof("[pruner][creating-sweep][%d] cleared the creating state of %s, which carries no attempt token", blockHeight, hash)

			continue
		}

		// Re-spend the inputs only (the record already exists in the creating state) and
		// finalize, through the shared roll-forward rule. Spends are idempotent for the same
		// spender, which makes roll-forward safe. Locks are not ignored: a locked parent is
		// the hand-off barrier, so ErrTxLocked is retried on a later pass rather than
		// bypassed. The rule takes a spend lease first, which the store refuses for a record
		// that is rolling back, and a rollback that begins later waits for the lease and then
		// reverses the spends this pass made.
		rfSpends, completed, rfErr := utxo.RollForwardCreating(ctx, s.logger, s.utxoStore, md.Tx, md.Attempt, blockHeight)
		if rfErr != nil {
			// A locked or creating parent is transient, and the record waits for it. Only when
			// pruner_creatingTxSweepBlockedAgeBlocks is set is such a record rolled back, and then only
			// when the record is that old and every parent that blocked it has been blocked for that
			// long too: one that is not committing keeps the inputs that did spend, and every output of
			// the record, unspendable for as long as it lasts.
			if isUnwinnableSpendError(rfErr) || (isTransientSpendError(rfErr) && s.blockedByParentsForLong(ctx, blockHeight, md.UnminedSince, rfSpends, parentBlocked)) {
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

		if !completed {
			// Another actor's finalize completed the record. That actor owns the hand-off to block
			// assembly and the unlock after it.
			progressed++

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

		validatedHeight := utxo.UnminedValidatedHeight(md.UnminedSince, blockHeight, s.settings.UtxoStore.UnminedTxRetention)

		if _, storeErr := s.blockAssemblyClient.Store(ctx, &hash, md.Fee, md.SizeInBytes, md.TxInpoints, validatedHeight); storeErr != nil {
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

// blockedByParentsForLong reports whether a creating record that failed its re-spend only because
// parents are in flight may be rolled back for its age. It is false when
// pruner_creatingTxSweepBlockedAgeBlocks is 0, which is the default. Otherwise the record must be
// older than that age, and every parent that gave a transient answer in this pass must be read as
// still locked or creating and as having been so for at least that age. A failure of any other kind
// on an input, a spend that does not name its parent, a parent that cannot be read and a parent whose
// age cannot be established all leave the record for the next pass.
func (s *Server) blockedByParentsForLong(ctx context.Context, blockHeight, unminedSince uint32, spends []*utxo.Spend, verified map[chainhash.Hash]bool) bool {
	age := s.settings.Pruner.CreatingTxSweepBlockedAgeBlocks
	if age == 0 {
		return false
	}

	if unminedSince == 0 || unminedSince >= blockHeight || blockHeight-unminedSince <= age {
		return false
	}

	var parents []chainhash.Hash

	for _, sp := range spends {
		if sp == nil || sp.Err == nil {
			continue
		}

		if !isTransientSpendError(sp.Err) || sp.TxID == nil {
			return false
		}

		parents = append(parents, *sp.TxID)
	}

	if len(parents) == 0 {
		return false
	}

	for _, parent := range parents {
		long, known := verified[parent]
		if !known {
			long = s.parentBlockedForAtLeast(ctx, blockHeight, parent, age)
			verified[parent] = long
		}

		if !long {
			return false
		}
	}

	return true
}

// parentBlockedForAtLeast reads the record of a parent and reports whether it is still locked or
// creating and was stamped with an unminedSince at least age blocks below blockHeight. An unminedSince
// of 0 means mined, and 1 is the floor of a backdated value (utxo.BackdatedUnminedSince), so neither
// says when the parent was stored.
func (s *Server) parentBlockedForAtLeast(ctx context.Context, blockHeight uint32, parent chainhash.Hash, age uint32) bool {
	md, err := s.utxoStore.Get(ctx, &parent, fields.UnminedSince, fields.Locked, fields.Creating)
	if err != nil {
		s.logger.Warnf("[pruner][creating-sweep][%d] reading the parent %s of a blocked record failed, the record is left for the next pass: %v", blockHeight, parent, err)

		return false
	}

	if !md.Locked && !md.Creating {
		return false
	}

	return md.UnminedSince > 1 && md.UnminedSince <= blockHeight && blockHeight-md.UnminedSince >= age
}

// isTransientSpendError reports whether a spend error says that a parent is committing: its record
// is locked, or still creating.
func isTransientSpendError(err error) bool {
	return errors.Is(err, errors.ErrTxLocked) || errors.Is(err, errors.ErrTxCreating)
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

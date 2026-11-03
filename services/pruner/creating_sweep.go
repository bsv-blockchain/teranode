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

// sweepCreatingTxs rolls forward transactions abandoned in the create-first
// "creating" state: it re-runs their input spends from the stored tx bytes and
// finalizes them. Spends are idempotent for the same spender, so re-spending
// inputs an earlier attempt already spent is safe. Double-spent txs land in the
// conflicting terminal state (marked recursively, then finalized: exists,
// conflicting, unspendable, DAH-pruned). Records it cannot roll forward (e.g. a
// permanently locked or missing parent) are left creating — gated (unspendable) and
// retried on a later pass. After finalizing, the sweep hands the tx to block assembly
// and only then unlocks it, the order loadUnminedTransactions uses. When the announce
// fails, or no block assembly client is configured, the record stays finalized and
// locked so the next unmined reload restores it. Runs whatever the value of
// utxostore_useCreateFirstOrder, so records stranded before the flag was turned off
// still heal.
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

	s.logger.Infof("[pruner][creating-sweep][%d] rolling forward %d stale creating txs", blockHeight, len(hashes))

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

		md, err := s.utxoStore.Get(ctx, &hash, fields.Tx, fields.Creating, fields.Fee, fields.SizeInBytes, fields.TxInpoints)
		if err != nil {
			s.logger.Warnf("[pruner][creating-sweep][%d] get %s failed: %v", blockHeight, hash, err)
			continue
		}

		if !md.Creating || md.Tx == nil {
			// Finalized (or unreadable) between the query and this get.
			continue
		}

		// Re-spend the inputs only (the record already exists in the creating state).
		// #1326 removed Store.Spend; the spend-only phase of SpendAndCreate is the
		// equivalent. Idempotent for the same spender, which makes roll-forward safe.
		// WithIgnoreLocked: a roll-forward completes a spend the node already committed
		// to, so a locked parent (e.g. the tentative create locked its own record, or a
		// ProcessConflicting run locked parents) must not turn recovery into a hard stop.
		// A genuine double-spend still surfaces as ErrSpent/ErrTxConflicting below —
		// IgnoreLocked does not suppress those.
		_, spends, spendErr := s.utxoStore.SpendAndCreate(ctx, md.Tx, blockHeight, utxo.WithSpendOnly(), utxo.WithIgnoreLocked(true))
		if spendErr != nil {
			if errors.Is(spendErr, errors.ErrSpent) || errors.Is(spendErr, errors.ErrTxConflicting) {
				if _, _, setErr := utxo.MarkConflictingRecursively(ctx, s.utxoStore, []chainhash.Hash{hash}); setErr != nil {
					s.logger.Errorf("[pruner][creating-sweep][%d] mark conflicting %s failed: %v", blockHeight, hash, setErr)
					prunerCreatingSweepFailures.Inc()

					continue
				}

				if finErr := s.utxoStore.FinalizeTransaction(ctx, md.Tx); finErr != nil {
					s.logger.Errorf("[pruner][creating-sweep][%d] finalize conflicting %s failed: %v", blockHeight, hash, finErr)
					prunerCreatingSweepFailures.Inc()

					continue
				}

				progressed++ // resolved to the conflicting terminal state

				continue
			}

			s.logger.Warnf("[pruner][creating-sweep][%d] spend %s failed, will retry next block: %v", blockHeight, hash, spendErr)
			prunerCreatingSweepFailures.Inc()

			continue
		}

		// Defence-in-depth: only finalize when every input was actually spent this pass.
		// A nil top-level error alone is not proof — guard against a swallowed per-input
		// error (e.g. an "already blessed" fallback) leaving an input unspent while we
		// make the outputs spendable.
		if !utxo.AllInputsSpent(md.Tx, spends) {
			s.logger.Warnf("[pruner][creating-sweep][%d] spend of %s did not cover all inputs (%d spends for %d inputs); not finalizing", blockHeight, hash, len(spends), len(md.Tx.Inputs))
			prunerCreatingSweepFailures.Inc()

			continue
		}

		if finErr := s.utxoStore.FinalizeTransaction(ctx, md.Tx); finErr != nil {
			s.logger.Errorf("[pruner][creating-sweep][%d] finalize %s failed: %v", blockHeight, hash, finErr)
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

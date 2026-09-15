package utxoset

import (
	"context"

	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
)

// journalPruner drops what has aged out, and reads nothing to decide it.
//
// There is no DAH sweep here and no pile of spent rows to walk. The DELETE that spends an
// output frees its space and its index entry in the same statement that authorises the
// spend, so the reclaim that dominated the previous store does not exist: its sweep,
// pruner and the vacuum they forced together measured 76.7% of all disk reads and 52% of
// statement write-ahead log volume, with the watermark thousands of blocks behind the tip.
//
// What is left is catalog operations on two horizons. Transaction bodies retire on
// DefaultTxBodyRetentionBlocks; membership windows, the spend journal and the
// conflict-bookkeeping windows created alongside its leaves retire on
// DefaultSpendJournalRetentionBlocks. Each is a DROP TABLE of partitions that have aged out,
// so there is no work list, no probe and no per-row cost that can fall behind.
//
// Identity reclaim used to be the expensive half of this: a retiring journal partition read
// as a work list, each parent judged on whether its spenders were settled, and its identity
// row deleted. That is gone. A mined transaction claims on tx_mined instead of tx_ident, and
// its coins carry the height and block that made them, so retiring its membership is dropping
// the window it lives in.
//
// It runs HERE, rather than on the spend path where it used to, for three reasons. The
// spend path had to swallow the error to avoid failing a spend over old history, and it
// swallowed a real one for the entire life of the branch. DETACH CONCURRENTLY waits for
// every open transaction on the parent, which from inside a spend stalls the pipeline.
// And services/pruner/worker.go already provides exactly the trigger this needs: once per
// block, off its own single-threaded goroutine, with the returned error logged at Errorf
// against a metric and the call timed. Nothing had to be built to get that.
//
// Inherited from that worker, and worth knowing rather than discovering: it gates on block
// assembly being caught up, skips heights at or below pruner_min_block_height, and
// deduplicates to the latest notification when it falls behind. So a session can be
// skipped or can jump several leaves at once. Every drop here absorbs that -- one call
// drops every partition below the cutoff, not one per call -- so a skipped block defers
// the drop rather than losing it, and the store simply carries more history until the
// next session lands.
type journalPruner struct {
	store *Store
}

// Start does nothing. The pruner service drives Prune once per block; there is no
// background loop here to fall behind.
func (journalPruner) Start(_ context.Context) {
	// Deliberately empty: the pruner service calls Prune once per block off its own
	// goroutine, so there is nothing for this store to start and nothing to stop.
}

// Prune drops the bodies, membership windows and journal leaves that have aged out at this
// height.
//
// The height is the tip, not a retention-adjusted one, so the retention is applied here.
//
// It reports ZERO records processed, and that is exact rather than evasive. The caller adds
// the return value to a counter of child transaction records deleted by a delete-at-height
// sweep, which this store does not have: nothing here deletes a row at all, it drops
// partitions. Reporting dropped partitions in that counter would put two different units in
// one metric, so they are logged instead.
func (p journalPruner) Prune(ctx context.Context, height uint32, _ string) (int64, error) {
	// The body horizon and the journal horizon are DIFFERENT numbers, 288 against 1440, so
	// the two reclaims must not be gated behind one another. Doing so left the bodies
	// unreclaimed for the whole of early sync, which is exactly when the disk is tightest.
	bodies, err := p.store.dropTxBodyWindowsBelow(ctx, height)
	if err != nil {
		return 0, err
	}

	if bodies > 0 {
		p.store.logger.Infof("[utxoset] pruner dropped %d transaction-body windows past the %d-block horizon",
			bodies, p.store.bodyRetention)
	}

	// The window and spend-journal drops are gated on journalRetention: below it nothing has
	// aged out yet, so there is nothing to drop. That gate does NOT extend to the coin-index
	// rebuild below -- a coin index can already be bloated on a chain three blocks deep, and
	// every dev/test net and every from-scratch sync spends most of its life below
	// DefaultSpendJournalRetentionBlocks (1440). Gating the rebuild on it, as an earlier
	// version of this did by putting the rebuild after this block's early return, meant the
	// rebuild never ran anywhere that mattered for testing it and would not have run during
	// the early, most write-heavy part of a real sync either.
	if height > p.store.journalRetention {
		cutoff := height - p.store.journalRetention

		// Identity reclaim is a partition drop. Nothing is read to decide it: a window whose
		// upper bound is journalRetention below the pruner's height holds transactions whose
		// blocks cannot be un-mined and whose coins carry their own block facts.
		windows, err := p.store.dropTxMinedWindowsBelow(ctx, cutoff)
		if err != nil {
			return 0, err
		}

		// The journal's leaves and the conflict-bookkeeping windows that retire with them, in
		// one pass: a note names a race whose losing spends are restored out of the journal,
		// so keeping it past its journal leaf would keep an answer nothing can act on.
		leaves, err := p.store.dropSpendJournalPartitionsBelow(ctx, cutoff)
		if err != nil {
			return 0, err
		}

		if windows > 0 || leaves > 0 {
			p.store.logger.Infof("[utxoset] pruner dropped %d membership windows and %d spend-journal and conflict partitions below height %d",
				windows, leaves, cutoff)
		}
	}

	// LAST, and unconditional: every session reaches this, regardless of height or
	// journalRetention. It is also deliberately once per session rather than looped: a
	// REINDEX CONCURRENTLY on a big partition can run for minutes, so this call returning is
	// not "the index is now clean", it is "at most one rebuild is in flight". The next
	// block's pruner call runs this again and finds whichever partition is now worst --
	// including the one just finished, back near the 31.5-byte floor -- so the schedule
	// catches up over a run of blocks rather than blocking this one.
	if _, err := p.store.rebuildOneBloatedCoinIndex(ctx, p.store.coinIndexDecider); err != nil {
		return 0, err
	}

	return 0, nil
}

// AddObserver accepts and discards. Observers are notified when a pruning cycle completes,
// and nothing registers one: the SQL pruner's AddObserver is also a no-op and
// services/pruner/server.go never calls it.
func (journalPruner) AddObserver(_ pruner.Observer) {
	// Deliberately empty: nothing registers an observer, and the SQL store's
	// AddObserver is a no-op for the same reason. Storing one here would be dead
	// state that reads as a working notification path.
}

// GetPrunerService satisfies pruner.PrunerServiceProvider.
//
// The pruner service is mandatory: services/pruner/server.go type-asserts the UTXO store
// to this interface and refuses to start without it, taking the whole daemon down. It is a
// real value rather than a nil, even though the provider interface documents nil as
// meaning "unsupported", because services/pruner/server.go stores the result and uses it
// without a nil check.
func (s *Store) GetPrunerService() (pruner.Service, error) {
	return journalPruner{store: s}, nil
}

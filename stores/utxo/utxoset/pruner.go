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
// DefaultTxBodyRetentionBlocks; containment windows, the spend journal and the
// conflict-bookkeeping windows created alongside its leaves retire on
// DefaultSpendJournalRetentionBlocks. Each is a DROP TABLE of partitions that have aged out,
// so there is no work list, no probe and no per-row cost that can fall behind.
//
// A containment window drops on the four-part rule of dropStampedTxMinedWindows, which needs
// the completion record the stamp writes. The stamp itself runs on the pruner service's own
// worker through the Stamper interface in stamp.go, never in front of a drop.
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

// Prune drops the bodies, undo partitions and containment windows that are due at this height,
// then rebuilds at most one bloated UTXO index.
//
// The height is the tip, not a retention-adjusted one, so the retention is applied here.
//
// The steps do not gate each other. Each runs even when an earlier one failed; the errors are
// logged, counted by step, and the first one is returned at the end. Deferring reclaim is the
// shape that filled the volume in September. The one ordering that matters is undo before
// containment: the window drop's third condition reads which undo partitions are still attached,
// so the undo drop in the same pass is what lets a containment drop succeed.
//
// The stamp is NOT here. It runs on the pruner service's own worker through the Stamper
// interface, beside this pass rather than in front of it, so a long drain never holds back a
// drop.
//
// It reports ZERO records processed, and that is exact rather than evasive. The caller adds
// the return value to a counter of child transaction records deleted by a delete-at-height
// sweep, which this store does not have: nothing here deletes a row at all, it drops
// partitions. Reporting dropped partitions in that counter would put two different units in
// one metric, so they are logged instead.
func (p journalPruner) Prune(ctx context.Context, height uint32, _ string) (int64, error) {
	var first error

	fail := func(step string, err error) {
		pruneStepErrors.WithLabelValues(step).Inc()
		p.store.logger.Errorf("[utxoset] pruner step %s at height %d: %v", step, height, err)

		if first == nil {
			first = err
		}
	}

	// The body horizon and the journal horizon are DIFFERENT numbers, 288 against 1440, so
	// the two reclaims must not be gated behind one another. Doing so left the bodies
	// unreclaimed for the whole of early sync, which is exactly when the disk is tightest.
	bodies, err := p.store.dropTxBodyWindowsBelow(ctx, height)
	if err != nil {
		fail("bodies", err)
	} else if bodies > 0 {
		p.store.logger.Infof("[utxoset] pruner dropped %d transaction-body windows past the %d-block horizon",
			bodies, p.store.bodyRetention)
	}

	// Undo partitions first, then containment windows, for the reason above. The undo drop is
	// gated on journalRetention: below it nothing has aged out. That gate does NOT extend to
	// the UTXO-index rebuild -- a UTXO index can already be bloated on a chain three blocks
	// deep, and every dev/test net and every from-scratch sync spends most of its life below
	// DefaultSpendJournalRetentionBlocks (1440).
	if !p.store.retainIndefinitely && height > p.store.journalRetention {
		leaves, err := p.store.dropSpendJournalPartitionsBelow(ctx, height-p.store.journalRetention)
		if err != nil {
			fail("undo", err)
		} else if leaves > 0 {
			p.store.logger.Infof("[utxoset] pruner dropped %d spend-journal and conflict partitions below height %d",
				leaves, height-p.store.journalRetention)
		}
	}

	windows, err := p.store.dropStampedTxMinedWindows(ctx, height)
	if err != nil {
		fail("containment", err)
	} else if windows > 0 {
		p.store.logger.Infof("[utxoset] pruner dropped %d containment windows at height %d", windows, height)
	}

	// LAST, and unconditional: every session reaches this, regardless of height or
	// journalRetention. It is also deliberately once per session rather than looped: a
	// REINDEX CONCURRENTLY on a big partition can run for minutes, so this call returning is
	// not "the index is now clean", it is "at most one rebuild is in flight". The next
	// block's pruner call runs this again and finds whichever partition is now worst --
	// including the one just finished, back near the 31.5-byte floor -- so the schedule
	// catches up over a run of blocks rather than blocking this one.
	if _, err := p.store.rebuildOneBloatedUTXOIndex(ctx, p.store.utxoIndexDecider); err != nil {
		fail("reindex", err)
	}

	return 0, first
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

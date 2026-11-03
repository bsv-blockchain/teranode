// //go:build aerospike

// Package utxo provides UTXO (Unspent Transaction Output) management for the BSV Blockchain Teranode implementation.
//
// This file implements conflicting transaction processing functionality for handling double-spend scenarios
// and transaction conflicts in the UTXO store. It requires the aerospike build tag.
package utxo

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"
)

// prometheusUtxoConflictingWalkNodes and prometheusUtxoConflictingWalkDepth record the
// shape of each completed GetConflictingChildren walk. The cone is unbounded by
// construction — it is every spender of every output of a transaction's descendants —
// and in the #1391 incident it was a linear self-spend chain growing by roughly a
// hundred transactions per block, so the walk degenerated to one store round trip per
// node with no fan-out at all. These free functions have no logger, so the histograms
// are the surface that makes a growing cone visible before it becomes a stall.
// Only completed walks are observed; an aborted walk returns before the observation.
var (
	prometheusUtxoConflictingWalkNodes = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "utxo",
			Name:      "conflicting_walk_nodes",
			Help:      "Number of transactions visited by a completed conflicting-descendant walk",
			Buckets:   prometheus.ExponentialBuckets(1, 4, 10),
		},
	)

	prometheusUtxoConflictingWalkDepth = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "utxo",
			Name:      "conflicting_walk_depth",
			Help:      "Number of BFS levels walked by a completed conflicting-descendant walk",
			Buckets:   prometheus.ExponentialBuckets(1, 4, 8),
		},
	)

	// prometheusUtxoConflictingWalkDuration replaces the store-method duration
	// histogram (e.g. aerospike txmeta_get_conflicting) for the walks that
	// GetCounterConflictingTxHashes now runs via the package-level function —
	// bypassing the Store wrapper so the maxNodes budget flows also bypassed its
	// tracing histogram. This one observes every walk, on every backend.
	prometheusUtxoConflictingWalkDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "utxo",
			Name:      "conflicting_walk_duration",
			Help:      "Duration of a conflicting-descendant walk",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
)

// conflictingWalkFanOut caps the per-level width of the conflicting-descendant
// walks (GetAndLockChildren and GetConflictingChildren), which previously opened
// one concurrent store read per level member with no ceiling. 128 matches the
// counter-conflicting GetMeta errgroup in subtreevalidation and is safe on the
// SQL backend, where Store.get falls back to getUnbatched whenever the requested
// bins include ConflictingChildren or Utxos (stores/utxo/sql/sql.go) — exactly
// the fields these walks request — so every level member is a concurrent
// unbatched query against postgres_maxOpenConns (50 by default).
//
// Known tradeoff, accepted deliberately: on Aerospike a wide level fills the
// getBatcher (conf utxostore_getBatcherSize 4096) in ~N/128 timer-triggered
// waves instead of ~N/4096 fill-triggered flushes, so very wide cones walk
// slower than a batcher-matched ceiling would allow. The incident cone was
// fully linear (one node per level), where the ceiling is moot.
const conflictingWalkFanOut = 128

// compensationTimeout bounds the deferred unwind of a failed ProcessConflicting.
// It runs on its own context rather than the caller's (see the rollback call site),
// so it needs a budget of its own: long enough to restore the recorded spends of a
// whole cascade, short enough not to pin a goroutine on a wedged store.
const compensationTimeout = 2 * time.Minute

// ProcessConflicting makes the winning transactions the spenders of their inputs, and demotes the
// transactions that conflict with them.
//
// Take txa and txb, which both spend txp[5]. If txb wins, txa and every transaction that spends
// the outputs of txa (its descendants) lose.
//
// The operation is driven by a durable recovery checklist (ConflictRecovery), saved before any
// state changes:
//
//  1. Collect the winners, the counter-conflicting transactions and their descendants. Record the
//     owner of every input spend, the parents, and each transaction's Conflicting flag.
//  2. Lock the parents that no one else has locked, and check that no spend changed since step 1.
//  3. Mark the whole set Conflicting. A descendant that appears after step 1 aborts the run.
//  4. Unspend the original spends, then write the winning spends.
//  5. Mark the winners not Conflicting, save phase applied, unlock the parents, save phase finished.
//
// A failure after step 2 restores the recorded owners and flags (RollbackConflictIntent). The
// intent is keyed by the block, so every caller and retry finds the same checklist. A retry of a run
// that reached applied or finished skips the mutation and returns the saved result. An additional
// winner that is already applied is dropped. One that is still Conflicting is refused until the
// caller acknowledges the existing intent after its subtree and queue updates.
// When a reverse of the block has since made a winner Conflicting again, the retry rolls the checklist
// back and plans again under the same record. A winner that is not Conflicting and owns every input
// is already applied, and the plan skips it.
//
// ProcessConflicting returns:
//   - losers: the transactions displaced by the winners, with their descendants. It never contains
//     a winner. Callers mark these in the subtrees.
//   - marked: the same set as a list, in discovery order. Block assembly uses it to reject children
//     of conflicting parents that arrive after the operation. On error it lists every checklist
//     transaction the run or its rollback may have flipped to Conflicting, for txmeta cache
//     eviction only. The winners are in it only once the run has tried to clear their flag.
//
// The third parameter is unused. A process-local map cannot prove what an earlier run did, so only
// the durable checklist authorises a retry. The parameter stays so callers need no change.
func ProcessConflicting(ctx context.Context, s Store, blockHeight uint32, blockHash chainhash.Hash, conflictingTxHashes []chainhash.Hash,
	_ map[chainhash.Hash]struct{}, guard AncestryGuard, opts ...ProcessConflictingOption) (losers txmap.TxMap, marked []chainhash.Hash, err error) {
	ctx, _, done := tracing.Tracer("utxo").Start(ctx, "ProcessConflicting")
	defer done()
	var options processConflictingOptions

	for _, opt := range opts {
		opt(&options)
	}

	// Registered before the rollback defer so it runs after it. A failed run or rollback can leave
	// checklist transactions conflicting, so on error the caller evicts them from the txmeta cache.
	// An eviction is not free: subtree validation keeps a tombstone for each evicted hash and drops
	// its later ADDs for subtreevalidation_txMetaTombstoneWindow. So add a hash only once the run
	// may have changed its flag.
	//
	// earlier holds what the rollback of an earlier checklist may have changed. On success, marked is
	// this run's loser set and can miss those transactions, so they go to the evictor instead.
	var earlier, mayBeMarked, carried []chainhash.Hash
	defer func() {
		if err != nil {
			losers = nil
			marked = appendNewHashes(append([]chainhash.Hash(nil), earlier...), mayBeMarked)
			return
		}
		if options.evict != nil && len(earlier) > 0 {
			options.evict(earlier)
		}
	}()

	intent := ConflictIntent{Recovery: &ConflictRecovery{Version: 1, Phase: ConflictRecoveryPlanning, Winners: append([]chainhash.Hash(nil), conflictingTxHashes...)}, Kind: ConflictIntentForward, BlockHeight: blockHeight, BlockHash: blockHash, TxHashes: conflictingTxHashes, StartedAt: time.Now().UnixNano()}
	recovery, err := s.LoadConflictRecovery(ctx, intent.IntentID())
	if err != nil {
		return nil, nil, err
	}
	if recovery == nil {
		if err = s.BeginConflictIntent(ctx, intent); err != nil {
			return nil, nil, errors.NewProcessingError("failed to record conflict WAL intent", err)
		}
		// Begin gives a new intent a planning checklist and leaves an existing record as it is, so a
		// record that still has no checklist is a legacy intent. This avoids a scan of the whole WAL.
		if recovery, err = s.LoadConflictRecovery(ctx, intent.IntentID()); err != nil {
			return nil, nil, err
		}
		if recovery == nil {
			return nil, nil, errors.NewProcessingError("legacy conflict intent %s has no recovery checklist; manual repair required", intent.IntentID().String())
		}
	}
	if recovery != nil {
		if err = recovery.Validate(); err != nil {
			return nil, nil, err
		}
		// Do not roll an unacknowledged operation back merely to add winners. Besides replacing
		// its pending result, rollback can demote children accepted since its snapshot. The caller
		// must finish the saved subtree and queue updates and acknowledge this intent first.
		// Older planning records have no Winners and no before-image or mutations to preserve.
		//
		// The callers do not all pass the same list for a block. reorgBlocks leaves out the winners
		// a moveBack already promoted, so the record can hold a subset; a reset or a startup
		// catch-up then passes the full list and never acknowledges first. An extra winner that is
		// already applied needs no rollback and nothing from this run, so drop it. Only an extra
		// winner that is still Conflicting asks for a new plan, and that is refused.
		if recovery.Phase != ConflictRecoveryPlanning || len(recovery.Winners) > 0 {
			if conflictingTxHashes, err = dropAppliedExtraWinners(ctx, s, blockHash, conflictingTxHashes, recovery.Winners, options.currentChainGuard); err != nil {
				return nil, nil, err
			}
		}
		// A replay must plan every recorded winner, whatever subset this caller passes.
		conflictingTxHashes = appendNewHashes(conflictingTxHashes, recovery.Winners)
		intent.TxHashes = conflictingTxHashes
		if recovery.Phase == ConflictRecoveryApplied || recovery.Phase == ConflictRecoveryFinished {
			settled, e := conflictWinnersSettled(ctx, s, recovery.Winners, options.currentChainGuard)
			if e != nil {
				return nil, nil, e
			}
			if settled {
				return finishConflictRecovery(ctx, s, intent, recovery, options.deferCompletion)
			}
		}
		// The record describes an operation this run cannot keep: it stopped before it applied, or a
		// reverse of the block undid some of its winners. Roll it back before planning again.
		// The checklist is the only before-image of the winners it applied, and a new plan skips a
		// winner that is applied, so a new checklist saved
		// over this one would hold no way to undo them. The rollback restores every recorded owner and
		// flag under this record, and only then does the new plan take its own before-image. A failure
		// or crash anywhere before the new checklist saves leaves this record, and the losers it
		// holds, for the next replay. A planning record holds no before-image and no lock.
		if recovery.Phase != ConflictRecoveryPlanning && recovery.Phase != ConflictRecoveryRolledBack {
			if earlier, err = RollbackConflictIntent(ctx, s, intent, options.currentChainGuard); err != nil {
				return nil, nil, err
			}
		}
		// The caller has not acknowledged the record's losers. The rollback restored the ones this
		// record demoted, and the new plan finds them again through their winners. A loser that is
		// still Conflicting after the rollback was Conflicting before the record, or lost an input to
		// a new transaction, so the new plan does not find it: carry it into the result and the new
		// checklist, so the caller gets it again.
		if carried, err = conflictingSubset(ctx, s, recovery.Losers); err != nil {
			return nil, nil, err
		}
	}
	winnerSet := make(map[chainhash.Hash]struct{}, len(conflictingTxHashes))
	var winningSpends []*Spend
	var affected hashSet
	var pending []chainhash.Hash
	// Applied winners remain winners even when this plan only mutates a subset.
	// Never include them in an ancestry check or the returned loser set.
	for _, h := range conflictingTxHashes {
		winnerSet[h] = struct{}{}
	}
	for _, h := range conflictingTxHashes {
		if h.Equal(subtree.CoinbasePlaceholderHashValue) {
			return nil, nil, errors.NewProcessingError("[ProcessConflicting][%s] tx is frozen", h.String())
		}
		m, e := s.Get(ctx, &h, fields.Tx, fields.Conflicting, fields.BlockIDs)
		if e != nil {
			return nil, nil, errors.NewProcessingError("[ProcessConflicting][%s] error getting tx", h.String(), e)
		}
		if m == nil {
			return nil, nil, errors.NewTxNotFoundError("[ProcessConflicting][%s] winning tx not found", h.String())
		}
		if m.Tx == nil {
			return nil, nil, errors.NewProcessingError("[ProcessConflicting][%s] winning transaction body is missing", h.String())
		}
		// A process-local processed map cannot prove the side effects of a previous operation. The
		// store can: a winner that is not Conflicting and owns every input is applied. A reverse that
		// stopped before it reached a winner leaves it so, and its block's checklist can be gone. A
		// confirmed winner can have a pruned parent, as in conflictWinnersSettled.
		if !m.Conflicting {
			applied, e := appliedWinnerOwnsInputs(ctx, s, h, m, options.currentChainGuard)
			if e != nil {
				return nil, nil, e
			}
			if !applied {
				return nil, nil, errors.NewProcessingError("[ProcessConflicting][%s] tx is not conflicting and does not own its inputs", h.String())
			}
			continue
		}
		pending = append(pending, h)
		winnerSet[h] = struct{}{}
		hashes, e := s.GetCounterConflicting(ctx, h)
		if e != nil {
			return nil, nil, errors.NewProcessingError("[ProcessConflicting][%s] error getting counter conflicting txs", h.String(), e)
		}
		spends, e := GetSpends(m.Tx)
		if e != nil {
			return nil, nil, e
		}
		winningSpends = append(winningSpends, spends...)
		affected.add(hashes...)
		affected.add(h)
	}
	if len(pending) == 0 {
		// Every winner is applied. The intent records no change, so remove it, unless it still has
		// to hold carried losers until the caller acknowledges them.
		return finishConflictRecoveryWithCarried(ctx, s, intent, conflictingTxHashes, carried, options.deferCompletion)
	}
	allWinners := conflictingTxHashes
	conflictingTxHashes = pending

	if guard.enabled() {
		_, refused, e := filterAncestryConfirmedLosers(ctx, s, affected.order, winnerSet, blockHash, guard)
		if e != nil {
			return nil, nil, e
		}
		if len(refused) > 0 {
			for _, h := range refused {
				prometheusUtxoConflictingDemotionRefused.Inc()
				if options.onRefused != nil {
					options.onRefused(h)
				}
			}
			// Retain the admitted winner set even though the guard refused promotion. A
			// carried result still needs acknowledgement, and replay uses the WAL's input.
			return finishConflictRecoveryWithCarried(ctx, s, intent, allWinners, carried, options.deferCompletion)
		}
	}
	recovery, err = prepareConflictRecovery(ctx, s, affected.order, winningSpends)
	if err != nil {
		return nil, nil, err
	}
	// The checklist names every winner of the block, the applied ones included, so a replay with
	// any subset plans them all. Its transactions and spends cover only the pending winners: an
	// earlier checklist of the block was rolled back above, so a winner this plan skips was applied
	// by an operation that is acknowledged or retired, and this record does not answer for it.
	recovery.Winners = append([]chainhash.Hash(nil), allWinners...)
	for _, h := range affected.order {
		if _, isWinner := winnerSet[h]; !isWinner {
			recovery.Losers = append(recovery.Losers, h)
		}
	}
	recovery.Losers = appendNewHashes(recovery.Losers, carried)
	if err = s.SaveConflictRecovery(ctx, intent.IntentID(), recovery); err != nil {
		return nil, nil, err
	}
	committed := false
	defer func() {
		if err == nil || committed {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.Background(), compensationTimeout)
		defer cancel()
		rolledBack, rollbackErr := RollbackConflictIntent(rollbackCtx, s, intent, options.currentChainGuard)
		// mayBeMarked already tracks the checklist transactions this run changed. A winner the run has
		// not tried to clear keeps its flag, so take only the children outside the checklist from here.
		var checklist hashSet
		checklist.add(conflictTransactionHashes(recovery)...)
		var outside []chainhash.Hash

		for _, h := range rolledBack {
			if !checklist.has(h) {
				outside = append(outside, h)
			}
		}
		mayBeMarked = appendNewHashes(mayBeMarked, outside)
		if rollbackErr != nil {
			err = errors.NewProcessingError("[ProcessConflicting] MANUAL INTERVENTION REQUIRED: original=%s rollback=%s", err.Error(), rollbackErr.Error(), errors.Join(err, rollbackErr))
		}
	}()
	if err = s.SetConflictLocks(ctx, intent.IntentID(), recovery.Parents, true); err != nil {
		return nil, nil, err
	}
	// Confirm that no spend changed between planning and lock acquisition.
	if err = verifyConflictSpends(ctx, s, recovery.OriginalSpends); err != nil {
		return nil, nil, err
	}
	recovery.Phase = ConflictRecoveryMutating
	if err = s.SaveConflictRecovery(ctx, intent.IntentID(), recovery); err != nil {
		return nil, nil, err
	}
	// From here the run and its rollback change flags. Nothing before this point does, so an earlier
	// failure must not evict the cascade. The winners stay Conflicting until the run clears their flag.
	for _, h := range conflictTransactionHashes(recovery) {
		if _, isWinner := winnerSet[h]; !isWinner {
			mayBeMarked = append(mayBeMarked, h)
		}
	}
	// Mark the losers and the winners in separate calls. Aerospike reports the children of a
	// transaction even when it is already Conflicting, and the run never touches a winner's outputs,
	// so a winner's child (for example a conflicting child that arrived with it) is not a cascade
	// change. Checking it would refuse every retry.
	var losersToMark []chainhash.Hash

	for _, h := range affected.order {
		if _, isWinner := winnerSet[h]; !isWinner {
			losersToMark = append(losersToMark, h)
		}
	}
	var discovered []chainhash.Hash

	if len(losersToMark) > 0 {
		if _, discovered, err = s.SetConflicting(ctx, losersToMark, true); err != nil {
			return nil, nil, err
		}
	}
	// A validator can attach a child while the checklist is being prepared.
	// Do not erase that edge without a before-image. Roll back and plan again.
	for _, h := range discovered {
		if !affected.has(h) {
			return nil, nil, errors.NewProcessingError("conflict descendants changed during preparation; retry required")
		}
	}
	if _, _, err = s.SetConflicting(ctx, conflictingTxHashes, true); err != nil {
		return nil, nil, err
	}
	var originalSpent []*Spend

	for _, sp := range recovery.OriginalSpends {
		if sp.SpendingData != nil {
			originalSpent = append(originalSpent, sp.Clone())
		}
	}
	if err = s.Unspend(ctx, originalSpent); err != nil {
		return nil, nil, err
	}
	// Synchronous per-outpoint writes also avoid late batch writes arriving after
	// the compensating rollback has already finished.
	if err = s.RestoreConflictSpends(ctx, cloneConflictSpends(winningSpends), blockHeight); err != nil {
		return nil, nil, err
	}
	// From here a rollback flips the winners back to Conflicting.
	mayBeMarked = conflictTransactionHashes(recovery)

	if _, _, err = s.SetConflicting(ctx, conflictingTxHashes, false); err != nil {
		return nil, nil, err
	}
	recovery.Phase = ConflictRecoveryApplied
	if err = s.SaveConflictRecovery(ctx, intent.IntentID(), recovery); err != nil {
		return nil, nil, err
	}
	committed = true
	return finishConflictRecovery(ctx, s, intent, recovery, options.deferCompletion)
}

// ReverseProcessConflicting undoes the side effects of a previous
// ProcessConflicting call so the UTXO store is restored to the state it would
// have been in had that call never happened. It is the inverse of
// ProcessConflicting and is meant to run inside moveBackBlock when the block
// whose subtree was processed is being removed from the chain.
//
// Inputs: demotedTxHashes is the list of txs originally passed to
// ProcessConflicting as winners (i.e. subtree.ConflictingNodes from the block
// being moved back).
//
// Operation for each demoted tx D, per input (parentHash, vout):
//
//  1. Pick a counter tx C from parent.ConflictingChildren that
//     (a) is not in demotedTxHashes, (b) is currently Conflicting=true, and
//     (c) spends the same (parentHash, vout) as D. C is the original mempool
//     spender that ProcessConflicting demoted.
//  2. Mark D and its spending descendants Conflicting=true (cascade). This
//     undoes Phase 4 of the original call for D and rebuilds the cascade for
//     D's descendants in case any were added after ProcessConflicting ran.
//  3. Unspend(D's inputs) so parent.SpendingDatas no longer points at D.
//  4. Spend(C's tx) so parent.SpendingDatas[vout] points at C again.
//  5. UnmarkConflictingRecursively(C) so C and its descendants flip back to
//     Conflicting=false.
//
// Demoted txs whose Conflicting flag is already true are skipped — the
// previous reverse already ran. Demoted txs with no counter currently
// conflicting are skipped at the per-input level: nothing to restore.
//
// Returns:
//   - cascadedToConflicting: every hash whose Conflicting flag this call flipped
//     to true (the demoted txs + their spending descendants). Callers feed
//     this into the moveForward dequeue path so the queue evicts the
//     unmined-side cascade. On error it lists every hash the call may have
//     flipped, for txmeta cache eviction only; allTouched is then nil.
//   - allTouched: union of cascadedToConflicting and the un-cascade hashes
//     whose flag flipped back to false (counter + descendants). Callers
//     feed this into processedConflictingHashesMap so the subsequent
//     moveForwardBlock pass skips ProcessConflicting on these hashes —
//     re-running it would double-apply the UTXO swap and fail.
func ReverseProcessConflicting(ctx context.Context, s Store, blockHeight uint32, blockHash chainhash.Hash, demotedTxHashes []chainhash.Hash, guards ...CurrentChainGuard) (cascadedToConflicting []chainhash.Hash, allTouched []chainhash.Hash, err error) {
	for _, guard := range guards {
		s = guard.protect(s)
	}
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "ReverseProcessConflicting")
	defer deferFn()

	if len(demotedTxHashes) == 0 {
		return nil, nil, nil
	}

	// Crash-safety write-ahead log (#861): record the reverse intent before any
	// state mutation and remove it on successful completion. See ProcessConflicting
	// for the full rationale. ReverseProcessConflicting self-heals partial state via
	// the isReverseFullyApplied guard, so replay from any step boundary is safe.
	walIntent := ConflictIntent{
		Kind:        ConflictIntentReverse,
		BlockHeight: blockHeight,
		BlockHash:   blockHash,
		TxHashes:    demotedTxHashes,
		StartedAt:   time.Now().UnixNano(),
	}
	if beginErr := s.BeginConflictIntent(ctx, walIntent); beginErr != nil {
		return nil, nil, errors.NewProcessingError("[ReverseProcessConflicting] failed to record WAL intent before processing", beginErr)
	}

	cascadedConflictingSet := make(map[chainhash.Hash]struct{}, 2*len(demotedTxHashes))
	touchedSet := make(map[chainhash.Hash]struct{}, 2*len(demotedTxHashes))

	// Nothing rolls a failed reverse back, so whatever it flipped stays
	// conflicting in the store: hand it to the caller for txmeta cache eviction.
	// Registered before the completion below so it runs after it: a failed
	// completion also returns the error-shaped results, never a success-shaped
	// allTouched together with an error.
	defer func() {
		if err == nil {
			return
		}

		allTouched = nil
		cascadedToConflicting = make([]chainhash.Hash, 0, len(cascadedConflictingSet))

		for h := range cascadedConflictingSet {
			cascadedToConflicting = append(cascadedToConflicting, h)
		}
	}()

	defer func() {
		if err == nil {
			err = CompleteConflictIntent(ctx, s, walIntent.IntentID())
		}
	}()

	for _, guard := range guards {
		if err := guard.checkTransactions(ctx, s, demotedTxHashes); err != nil {
			return nil, nil, err
		}
	}

	demotedSet := make(map[chainhash.Hash]struct{}, len(demotedTxHashes))
	for _, h := range demotedTxHashes {
		demotedSet[h] = struct{}{}
	}

	for i := range demotedTxHashes {
		demotedHash := demotedTxHashes[i]

		if demotedHash.Equal(subtree.CoinbasePlaceholderHashValue) {
			continue
		}

		demotedMeta, getErr := s.Get(ctx, &demotedHash, fields.Tx, fields.Conflicting)
		if getErr != nil {
			return nil, nil, errors.NewProcessingError("[ReverseProcessConflicting][%s] error getting demoted tx meta", demotedHash.String(), getErr)
		}

		if demotedMeta == nil || demotedMeta.Tx == nil {
			continue
		}

		if demotedMeta.Conflicting {
			// D.Conflicting=true alone is NOT sufficient evidence the
			// reverse is fully applied — a previous call may have failed
			// after step 1 (Mark) succeeded but before step 3 (Spend(C))
			// completed, leaving parent.SpendingDatas[vout] empty (cleared
			// in step 2). On retry we must complete the missing
			// Spend(C)+Unmark(C) work, not short-circuit. Confirm full
			// completion via observable parent state: every input of D must
			// have parent.SpendingDatas[vout] pointing to a non-nil,
			// non-D spender. If any input shows nil or still points at D,
			// fall through and re-run the steps below; the Mark and
			// Unspend are idempotent on the already-applied state.
			fullyReversed, checkErr := isReverseFullyApplied(ctx, s, demotedMeta.Tx, demotedHash)
			if checkErr != nil {
				return nil, nil, errors.NewProcessingError("[ReverseProcessConflicting][%s] error confirming reverse completion via parent state", demotedHash.String(), checkErr)
			}

			if fullyReversed {
				continue
			}
		}

		// Step 1: identify counters per input.
		countersToPromote, selErr := selectCountersForDemotedTx(ctx, s, demotedMeta.Tx, demotedSet)
		if selErr != nil {
			return nil, nil, selErr
		}

		// Step 2: re-mark D + descendants Conflicting=true.
		_, markedOrder, markErr := MarkConflictingRecursively(ctx, s, []chainhash.Hash{demotedHash})

		for _, h := range markedOrder {
			cascadedConflictingSet[h] = struct{}{}
			touchedSet[h] = struct{}{}
		}

		if markErr != nil {
			return nil, nil, errors.NewProcessingError("[ReverseProcessConflicting][%s] error marking demoted tx + descendants conflicting", demotedHash.String(), markErr)
		}

		// Step 3: unspend D's input spends so parent.SpendingDatas[vout]
		// no longer points at D.
		demotedSpends, buildErr := spendsForTx(demotedMeta.Tx)
		if buildErr != nil {
			return nil, nil, errors.NewProcessingError("[ReverseProcessConflicting][%s] error building unspend records", demotedHash.String(), buildErr)
		}

		if unspendErr := s.Unspend(ctx, demotedSpends, false); unspendErr != nil {
			return nil, nil, errors.NewProcessingError("[ReverseProcessConflicting][%s] error unspending demoted tx inputs", demotedHash.String(), unspendErr)
		}

		// Step 4 & 5: per counter, re-spend its inputs and un-cascade.
		for _, counterHash := range countersToPromote {
			counterMeta, getCounterErr := s.Get(ctx, &counterHash, fields.Tx)
			if getCounterErr != nil {
				return nil, nil, errors.NewProcessingError("[ReverseProcessConflicting][%s] error getting counter tx %s", demotedHash.String(), counterHash.String(), getCounterErr)
			}

			if counterMeta == nil || counterMeta.Tx == nil {
				continue
			}

			if _, _, spendErr := s.SpendAndCreate(ctx, counterMeta.Tx, blockHeight, WithSpendOnly(),
				WithIgnoreConflicting(true), WithIgnoreLocked(true)); spendErr != nil {
				return nil, nil, errors.NewProcessingError("[ReverseProcessConflicting][%s] error spending counter %s", demotedHash.String(), counterHash.String(), spendErr)
			}

			unmarked, unmarkErr := UnmarkConflictingRecursively(ctx, s, []chainhash.Hash{counterHash})
			if unmarkErr != nil {
				return nil, nil, errors.NewProcessingError("[ReverseProcessConflicting][%s] error un-marking counter %s + descendants", demotedHash.String(), counterHash.String(), unmarkErr)
			}

			for _, h := range unmarked {
				touchedSet[h] = struct{}{}
			}
		}
	}

	if len(touchedSet) == 0 {
		return nil, nil, nil
	}

	cascadedToConflicting = make([]chainhash.Hash, 0, len(cascadedConflictingSet))
	for h := range cascadedConflictingSet {
		cascadedToConflicting = append(cascadedToConflicting, h)
	}

	allTouched = make([]chainhash.Hash, 0, len(touchedSet))
	for h := range touchedSet {
		allTouched = append(allTouched, h)
	}

	return cascadedToConflicting, allTouched, nil
}

// isReverseFullyApplied returns true iff every input of the demoted tx D has
// parent.SpendingDatas[vout] populated with a non-nil spender C that is not D
// itself AND C is no longer flagged Conflicting. Used as the post-D.Conflicting=true
// guard to distinguish a fully applied reverse from a partial one.
//
// Returns false (no error) on:
//   - any input whose parent.SpendingDatas[vout] is nil (post-Unspend, pre-Spend
//     state — Spend(C) failed last time around)
//   - any input whose parent.SpendingDatas[vout].TxID equals demotedHash
//     (Unspend never ran successfully for that input)
//   - any input whose recorded spender C is still Conflicting=true — this is the
//     crash-between-Spend(C)-and-Unmark(C) state (#861): parent[vout] already
//     points at C, but the final UnmarkConflictingRecursively(C) never ran, so
//     C is wrongly still conflicting. Returning false re-runs the steps, whose
//     Mark(D)/Unspend(D)/Spend(C) are idempotent and whose Unmark(C) finishes the job.
//   - any input whose parent has no SpendingDatas slice or is shorter than vout,
//     or whose recorded spender record is missing (defensive: cannot confirm full
//     application, so retry rather than claim done)
//
// Returns true only when ALL inputs unambiguously have a non-D, non-conflicting
// spender. An error is surfaced for any Get failure — that's a store-level
// problem, not a state question, and the caller must abort the reverse rather
// than make assumptions.
func isReverseFullyApplied(ctx context.Context, s Store, demotedTx *bt.Tx, demotedHash chainhash.Hash) (bool, error) {
	for _, input := range demotedTx.Inputs {
		parentHash := input.PreviousTxIDChainHash()
		vout := input.PreviousTxOutIndex

		parentMeta, err := s.Get(ctx, parentHash, fields.Utxos)
		if err != nil {
			return false, errors.NewProcessingError("[isReverseFullyApplied][%s] error getting parent %s meta", demotedHash.String(), parentHash.String(), err)
		}

		if parentMeta == nil {
			return false, nil
		}

		if int(vout) >= len(parentMeta.SpendingDatas) {
			return false, nil
		}

		sd := parentMeta.SpendingDatas[vout]
		if sd == nil || sd.TxID == nil {
			return false, nil
		}

		if sd.TxID.IsEqual(&demotedHash) {
			return false, nil
		}

		// The recorded spender C must also be non-conflicting. A crash between
		// Spend(C) and UnmarkConflictingRecursively(C) leaves parent[vout]->C
		// with C still Conflicting=true; that is NOT a fully-applied reverse.
		spenderMeta, err := s.Get(ctx, sd.TxID, fields.Conflicting)
		if err != nil {
			return false, errors.NewProcessingError("[isReverseFullyApplied][%s] error getting recorded spender %s meta", demotedHash.String(), sd.TxID.String(), err)
		}

		if spenderMeta == nil || spenderMeta.Conflicting {
			return false, nil
		}
	}

	return true, nil
}

// selectCountersForDemotedTx walks the inputs of a demoted tx and returns the
// set of counter txs to restore as canonical spenders.
//
// For each (parent, vout) the demoted tx spends, candidates are entries in
// parent.ConflictingChildren that:
//
//  1. are not themselves being demoted in this call,
//  2. are currently Conflicting=true (the previous ProcessConflicting demoted
//     them), and
//  3. actually spend the same (parent, vout) — guards against sibling-output
//     spenders that wouldn't conflict with this demoted tx.
//
// When more than one candidate matches per input, the function picks the one
// with the lowest CreatedAt (first-seen mempool spender, set once at insert
// in both backends — Aerospike at stores/utxo/aerospike/create.go:706, SQL
// via the inserted_at column populated by getUnbatched / batchDecorateChunk).
// Tiebreak on equal CreatedAt is lexicographic hash compare so the choice is
// deterministic across nodes and across runs.
//
// The same counter may legitimately spend several of the demoted tx's
// inputs; the function deduplicates so we only Spend()/Unmark() it once.
//
// Returns nil with no error when no candidate passes the filters for any
// input — caller demotes D + descendants but leaves SpendingDatas[vout]
// untouched for that input. ReverseProcessConflicting's caller can rely on
// the returned list being the exact set to feed Spend/UnmarkConflicting.
func selectCountersForDemotedTx(ctx context.Context, s Store, demotedTx *bt.Tx, demotedSet map[chainhash.Hash]struct{}) ([]chainhash.Hash, error) {
	seen := make(map[chainhash.Hash]struct{})

	result := make([]chainhash.Hash, 0)

	for _, input := range demotedTx.Inputs {
		parentHash := input.PreviousTxIDChainHash()
		vout := input.PreviousTxOutIndex

		parentMeta, err := s.Get(ctx, parentHash, fields.ConflictingChildren)
		if err != nil {
			return nil, errors.NewProcessingError("[selectCountersForDemotedTx][%s] error getting parent meta", parentHash.String(), err)
		}

		if parentMeta == nil {
			continue
		}

		var (
			best          *chainhash.Hash
			bestCreatedAt int64
		)

		for j := range parentMeta.ConflictingChildren {
			candidate := parentMeta.ConflictingChildren[j]

			if _, demoted := demotedSet[candidate]; demoted {
				continue
			}

			if _, dup := seen[candidate]; dup {
				continue
			}

			// candidateSpendsOutput only compares outpoints, so the inpoints are
			// enough; fields.Tx would rebuild the whole candidate transaction.
			candidateMeta, err := s.Get(ctx, &candidate, fields.TxInpoints, fields.Conflicting, fields.CreatedAt)
			if err != nil {
				return nil, errors.NewProcessingError("[selectCountersForDemotedTx][%s] error getting candidate counter", candidate.String(), err)
			}

			if candidateMeta == nil {
				continue
			}

			if !candidateMeta.Conflicting {
				continue
			}

			if !candidateSpendsOutput(&candidateMeta.TxInpoints, parentHash, vout) {
				continue
			}

			// First-seen wins. Pin the candidate by value because parentMeta is
			// rewritten under us via deeper Get calls.
			candidateCopy := candidate

			if best == nil || isOlderCounter(candidateMeta.CreatedAt, candidateCopy, bestCreatedAt, *best) {
				best = &candidateCopy
				bestCreatedAt = candidateMeta.CreatedAt
			}
		}

		if best != nil {
			seen[*best] = struct{}{}
			result = append(result, *best)
		}
	}

	return result, nil
}

// isOlderCounter returns true when (aCreatedAt, aHash) sorts strictly before
// (bCreatedAt, bHash). CreatedAt comes first, hash bytes are the tiebreak.
// A candidate whose CreatedAt is zero (missing on legacy records) is treated
// as newer than any candidate with a real timestamp — we never prefer the
// unknown-vintage record over one with a known first-seen time.
func isOlderCounter(aCreatedAt int64, aHash chainhash.Hash, bCreatedAt int64, bHash chainhash.Hash) bool {
	switch {
	case aCreatedAt == 0 && bCreatedAt == 0:
		// fall through to hash compare
	case aCreatedAt == 0:
		return false
	case bCreatedAt == 0:
		return true
	case aCreatedAt < bCreatedAt:
		return true
	case aCreatedAt > bCreatedAt:
		return false
	}

	// equal CreatedAt — lex compare the hash bytes for determinism.
	for i := range aHash {
		if aHash[i] != bHash[i] {
			return aHash[i] < bHash[i]
		}
	}

	return false
}

func candidateSpendsOutput(inpoints *subtree.TxInpoints, parentHash *chainhash.Hash, vout uint32) bool {
	for _, inpoint := range inpoints.GetTxInpoints() {
		if inpoint.Index == vout && inpoint.Hash.IsEqual(parentHash) {
			return true
		}
	}

	return false
}

// spendsForTx builds the []*Spend records for tx.Inputs in the same shape
// Unspend / Spend expect.
func spendsForTx(tx *bt.Tx) ([]*Spend, error) {
	spends := make([]*Spend, len(tx.Inputs))

	for i, input := range tx.Inputs {
		utxoHash, err := util.UTXOHashFromInput(input)
		if err != nil {
			return nil, err
		}

		spends[i] = &Spend{
			TxID:         input.PreviousTxIDChainHash(),
			Vout:         input.PreviousTxOutIndex,
			UTXOHash:     utxoHash,
			SpendingData: spendpkg.NewSpendingData(tx.TxIDChainHash(), i),
		}
	}

	return spends, nil
}

// UnmarkConflictingRecursively flips Conflicting=false on the given txs and
// every spending descendant reached via BFS over SpendingDatas. Inverse of
// MarkConflictingRecursively.
//
// Returns the BFS-ordered list of every hash whose flag this call cleared
// (the input set plus every descendant the cascade reached).
func UnmarkConflictingRecursively(ctx context.Context, s Store, hashes []chainhash.Hash) ([]chainhash.Hash, error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "UnmarkConflictingRecursively")
	defer deferFn()

	toProcess := hashes

	visited := make(map[chainhash.Hash]struct{}, len(hashes))
	clearedOrder := make([]chainhash.Hash, 0, len(hashes))

	for _, h := range hashes {
		if _, ok := visited[h]; !ok {
			visited[h] = struct{}{}
			clearedOrder = append(clearedOrder, h)
		}
	}

	for len(toProcess) > 0 {
		_, spendingChildTxs, err := s.SetConflicting(ctx, toProcess, false)
		if err != nil {
			return nil, err
		}

		// filter out already-visited hashes to prevent infinite loops
		nextBatch := spendingChildTxs[:0]
		for _, child := range spendingChildTxs {
			if _, ok := visited[child]; !ok {
				visited[child] = struct{}{}
				clearedOrder = append(clearedOrder, child)
				nextBatch = append(nextBatch, child)
			}
		}

		toProcess = nextBatch
	}

	return clearedOrder, nil
}

// maxRollbackErrors bounds how many individual rollback failures are kept. A failed rollback
// can produce one error per cascaded transaction, which reached 1.4 MB on a wedged node and was
// truncated by log pipelines before the MANUAL INTERVENTION REQUIRED marker could be read.
const maxRollbackErrors = 20

// maxRollbackMessageBytes bounds one kept failure. A store error for a batch can list every spend in it.
const maxRollbackMessageBytes = 1024

// capRollbackFailures flattens the failures to one message and keeps the first
// maxRollbackErrors of them, each cut to maxRollbackMessageBytes. It does not use errors.JoinCapped: that chains *Error values and
// drops the links in the middle, which would hide most of the failures it claims to keep.
func capRollbackFailures(failures []error) error {
	if len(failures) == 0 {
		return nil
	}

	kept := failures
	if len(kept) > maxRollbackErrors {
		kept = kept[:maxRollbackErrors]
	}

	messages := make([]string, 0, len(kept)+1)
	for _, failure := range kept {
		message := failure.Error()
		if len(message) > maxRollbackMessageBytes {
			message = message[:maxRollbackMessageBytes] + "..."
		}

		messages = append(messages, message)
	}

	if omitted := len(failures) - len(kept); omitted > 0 {
		messages = append(messages, fmt.Sprintf("... and %d more rollback failures omitted", omitted))
	}

	return errors.NewProcessingError("%s", strings.Join(messages, "; "))
}

// hashSet is an insertion-ordered set of hashes. The zero value is ready to use.
type hashSet struct {
	seen  map[chainhash.Hash]struct{}
	order []chainhash.Hash
}

func (h *hashSet) has(hash chainhash.Hash) bool {
	_, ok := h.seen[hash]
	return ok
}

// dropAppliedExtraWinners returns the given winners without the ones that the record does not
// hold and that are already applied: not Conflicting, and the spender of every input. An extra
// winner that is Conflicting is refused, because adding it means a rollback of the unacknowledged
// record. An extra winner that is not Conflicting and lost an input is an error.
func dropAppliedExtraWinners(ctx context.Context, s Store, blockHash chainhash.Hash, winners, recorded []chainhash.Hash, guard CurrentChainGuard) ([]chainhash.Hash, error) {
	var known hashSet
	known.add(recorded...)

	kept := winners[:0:0]

	for _, h := range winners {
		if known.has(h) {
			kept = append(kept, h)
			continue
		}

		m, err := s.Get(ctx, &h, fields.Tx, fields.Conflicting, fields.BlockIDs)
		if err != nil {
			return nil, errors.NewProcessingError("[ProcessConflicting][%s] error getting tx", h.String(), err)
		}

		if m == nil {
			return nil, errors.NewTxNotFoundError("[ProcessConflicting][%s] winning tx not found", h.String())
		}

		if m.Conflicting {
			return nil, errors.NewProcessingError("[ProcessConflicting][%s] acknowledge the existing conflict intent before adding winners", blockHash.String())
		}

		applied, err := appliedWinnerOwnsInputs(ctx, s, h, m, guard)
		if err != nil {
			return nil, err
		}

		if !applied {
			return nil, errors.NewProcessingError("[ProcessConflicting][%s] tx is not conflicting and does not own its inputs", h.String())
		}
	}

	return kept, nil
}

// conflictingSubset returns the hashes that are Conflicting in the store, in the given order. A
// hash the store does not know is left out.
func conflictingSubset(ctx context.Context, s Store, hashes []chainhash.Hash) ([]chainhash.Hash, error) {
	var out []chainhash.Hash

	for _, h := range hashes {
		m, err := s.Get(ctx, &h, fields.Conflicting)
		if errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, errors.NewProcessingError("[ProcessConflicting][%s] error getting tx", h.String(), err)
		}
		if m != nil && m.Conflicting {
			out = append(out, h)
		}
	}

	return out, nil
}

// finishConflictRecoveryWithCarried ends a run that changed nothing in the store. Without carried
// losers the record is removed. With them, the record becomes a finished checklist that holds the
// carried losers until the caller acknowledges their subtree updates, as a finished run does.
func finishConflictRecoveryWithCarried(ctx context.Context, s Store, intent ConflictIntent, winners, carried []chainhash.Hash, deferCompletion bool) (txmap.TxMap, []chainhash.Hash, error) {
	if len(carried) == 0 {
		if err := CompleteConflictIntent(ctx, s, intent.IntentID()); err != nil {
			return nil, nil, err
		}

		return txmap.NewSplitSwissMap(0), nil, nil
	}

	r := &ConflictRecovery{Version: 1, Phase: ConflictRecoveryFinished, Winners: append([]chainhash.Hash(nil), winners...), Losers: append([]chainhash.Hash(nil), carried...)}
	if err := s.SaveConflictRecovery(ctx, intent.IntentID(), r); err != nil {
		return nil, nil, err
	}

	return finishConflictRecovery(ctx, s, intent, r, deferCompletion)
}

// appendNewHashes appends the hashes of src that dst does not hold yet.
func appendNewHashes(dst, src []chainhash.Hash) []chainhash.Hash {
	var seen hashSet
	seen.add(dst...)

	for _, h := range src {
		if !seen.has(h) {
			seen.add(h)
			dst = append(dst, h)
		}
	}

	return dst
}

func (h *hashSet) add(hashes ...chainhash.Hash) {
	if h.seen == nil {
		h.seen = make(map[chainhash.Hash]struct{}, len(hashes))
	}

	for _, hash := range hashes {
		if _, ok := h.seen[hash]; !ok {
			h.seen[hash] = struct{}{}
			h.order = append(h.order, hash)
		}
	}
}

// winnerIsApplied reports whether the store records the winner as the spender of every one of
// its inputs, which is the end state of a completed ProcessConflicting run for that winner.
func winnerIsApplied(ctx context.Context, s Store, txHash chainhash.Hash, tx *bt.Tx) (bool, error) {
	return winnerInputsOwned(ctx, s, txHash, tx, absentParentIsError)
}

// absentParent says what winnerInputsOwned reports for an input whose parent is not in the store.
type absentParent int

const (
	// absentParentIsError: an unconfirmed winner needs every parent, so a missing one is an error.
	absentParentIsError absentParent = iota
	// absentParentIsOwned: a confirmed winner can have a pruned parent, so the input counts as owned.
	absentParentIsOwned
	// absentParentIsNotOwned: a reversed winner cannot be the spender of a pruned parent, so the
	// input counts as lost. The pruner removes a parent once a confirmed transaction spent every
	// output, and a Conflicting winner is not confirmed.
	absentParentIsNotOwned
)

func winnerInputsOwned(ctx context.Context, s Store, txHash chainhash.Hash, tx *bt.Tx, absent absentParent) (bool, error) {
	if tx == nil || len(tx.Inputs) == 0 {
		return false, nil
	}

	for _, input := range tx.Inputs {
		parentHash := input.PreviousTxIDChainHash()
		vout := input.PreviousTxOutIndex

		parentMeta, err := s.Get(ctx, parentHash, fields.Utxos)
		if errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound) || (err == nil && parentMeta == nil) {
			switch absent {
			case absentParentIsOwned:
				continue
			case absentParentIsNotOwned:
				return false, nil
			}
		}
		if err != nil {
			return false, errors.NewProcessingError("[ProcessConflicting][%s] error getting parent %s to check the winner is applied", txHash.String(), parentHash.String(), err)
		}

		if parentMeta == nil || int(vout) >= len(parentMeta.SpendingDatas) {
			return false, nil
		}

		sd := parentMeta.SpendingDatas[vout]
		if sd == nil || sd.TxID == nil || !sd.TxID.IsEqual(&txHash) {
			return false, nil
		}
	}

	return true, nil
}

// HealStaleForwardIntent converges a forward intent whose block is no longer on the current chain.
//   - Every winner is Conflicting again: a reverse undid the operation and stopped before it retired
//     the checklist. Retire the intent.
//   - The operation was applied and a winner is confirmed on the current chain: keep the applied
//     state of the confirmed winners, as a reorg keeps a transaction of an orphaned block. When block
//     assembly reached the block, its moveBack reverses the other winners. Otherwise no moveBack
//     follows, so reverse them here. Then release the locks and retire the intent.
//   - Otherwise, roll the operation back.
//
// assemblyReached reports whether block assembly reached the intent's block. It is called only when
// a winner is confirmed and another is not.
//
// It returns the transactions whose Conflicting flag it may have changed, for txmeta cache eviction,
// also on error. It is empty when nothing changed.
func HealStaleForwardIntent(ctx context.Context, s Store, intent ConflictIntent, guard CurrentChainGuard, assemblyReached func(context.Context) (bool, error)) ([]chainhash.Hash, error) {
	r, err := s.LoadConflictRecovery(ctx, intent.IntentID())
	if err != nil {
		return nil, err
	}

	if r != nil && (r.Phase == ConflictRecoveryApplied || r.Phase == ConflictRecoveryFinished) {
		reversed, err := conflictWinnersReversed(ctx, s, r.Winners)
		if err != nil {
			return nil, err
		}

		if reversed {
			return nil, retireConflictIntent(ctx, s, intent.IntentID(), r)
		}

		confirmed, unconfirmed, err := splitConfirmedConflictWinners(ctx, s, r.Winners, guard)
		if err != nil {
			return nil, err
		}

		if confirmed {
			var mayBeMarked []chainhash.Hash

			if len(unconfirmed) > 0 {
				reached, err := assemblyReached(ctx)
				if err != nil {
					return nil, err
				}

				// Reverse before the retirement. The reverse records its own intent, and the checklist
				// stays until the reverse succeeds, so a crash between them loses nothing.
				if !reached {
					if mayBeMarked, _, err = ReverseProcessConflicting(ctx, s, intent.BlockHeight, intent.BlockHash, unconfirmed, guard); err != nil {
						return mayBeMarked, err
					}
				}
			}

			return mayBeMarked, retireConflictIntent(ctx, s, intent.IntentID(), r)
		}
	}

	return rollbackConflictIntent(ctx, s, intent, r, guard)
}

// RetireConflictIntentsForBlocks retires the applied and finished forward intents of blocks that a
// reverse has moved back. It releases the parent locks that each intent still owns, so an intent
// that stopped between applied and finished leaks no lock. Other phases stay pending for recovery.
// A forward intent is keyed by its block, so this needs no scan of the WAL.
func RetireConflictIntentsForBlocks(ctx context.Context, s Store, blockHashes []chainhash.Hash) error {
	for _, blockHash := range blockHashes {
		id := ForwardConflictIntentID(blockHash)

		r, err := s.LoadConflictRecovery(ctx, id)
		if err != nil {
			return err
		}

		if r != nil && (r.Phase == ConflictRecoveryApplied || r.Phase == ConflictRecoveryFinished) {
			if err := retireConflictIntent(ctx, s, id, r); err != nil {
				return err
			}
		}
	}

	return nil
}

// retireConflictIntent releases the parent locks that the intent still owns and completes it. The
// release skips a lock that another operation owns, and a finished checklist owns none.
func retireConflictIntent(ctx context.Context, s Store, id chainhash.Hash, r *ConflictRecovery) error {
	if err := s.ReleaseConflictLocks(ctx, id, r.Parents); err != nil {
		return err
	}

	return CompleteConflictIntent(ctx, s, id)
}

// conflictWinnersReversed reports whether every winner exists, is Conflicting again and no longer
// owns all its inputs. After an applied operation, only a reverse of its block sets that state: it
// flags the winners and gives their inputs back. A flag change alone does not count. A parent that
// the pruner removed after the restored loser was confirmed counts as an input the winner lost.
func conflictWinnersReversed(ctx context.Context, s Store, winners []chainhash.Hash) (bool, error) {
	if len(winners) == 0 {
		return false, nil
	}

	for _, h := range winners {
		m, err := s.Get(ctx, &h, fields.Tx, fields.Conflicting)
		if err != nil {
			return false, errors.NewProcessingError("[conflictWinnersReversed][%s] error getting tx", h.String(), err)
		}

		if m == nil || !m.Conflicting {
			return false, nil
		}

		applied, err := winnerInputsOwned(ctx, s, h, m.Tx, absentParentIsNotOwned)
		if err != nil || applied {
			return false, err
		}
	}

	return true, nil
}

// splitConfirmedConflictWinners reports whether the current chain confirms at least one winner that
// is not Conflicting, and lists the winners it does not confirm. A stale intent's own block is off the
// current chain, so it confirms nothing. A confirmed winner must still own every input: the applied
// state is kept only when the store still shows it, so a mismatch keeps the checklist.
func splitConfirmedConflictWinners(ctx context.Context, s Store, winners []chainhash.Hash, guard CurrentChainGuard) (confirmed bool, unconfirmed []chainhash.Hash, err error) {
	if guard.check == nil {
		return false, nil, nil
	}

	for _, h := range winners {
		m, err := s.Get(ctx, &h, fields.Tx, fields.Conflicting, fields.BlockIDs)
		if err != nil {
			return false, nil, errors.NewProcessingError("[splitConfirmedConflictWinners][%s] error getting tx", h.String(), err)
		}

		if m == nil {
			return false, nil, errors.NewProcessingError("[splitConfirmedConflictWinners][%s] conflict winner is missing", h.String())
		}

		isConfirmed := false
		if !m.Conflicting && len(m.BlockIDs) > 0 {
			if isConfirmed, err = guard.check(ctx, m.BlockIDs); err != nil {
				return false, nil, errors.NewProcessingError("[splitConfirmedConflictWinners][%s] current-chain lookup failed", h.String(), err)
			}
		}

		if !isConfirmed {
			unconfirmed = append(unconfirmed, h)
			continue
		}

		applied, err := winnerInputsOwned(ctx, s, h, m.Tx, absentParentIsOwned)
		if err != nil {
			return false, nil, err
		}

		if !applied {
			return false, nil, errors.NewProcessingError("[splitConfirmedConflictWinners][%s] confirmed conflict winner no longer owns its inputs", h.String())
		}

		confirmed = true
	}

	return confirmed, unconfirmed, nil
}

// conflictWinnersSettled reports whether every winner of an applied checklist is still applied: not
// Conflicting, and the spender of every input. It returns false when a winner is Conflicting, which
// only a reverse of the block sets. A winner that is not Conflicting but lost an input contradicts the
// checklist, and that is an error.
func conflictWinnersSettled(ctx context.Context, s Store, winners []chainhash.Hash, guard CurrentChainGuard) (bool, error) {
	settled := true

	for _, h := range winners {
		m, err := s.Get(ctx, &h, fields.Tx, fields.Conflicting, fields.BlockIDs)
		if err != nil {
			return false, errors.NewProcessingError("[conflictWinnersSettled][%s] error getting tx", h.String(), err)
		}

		if m == nil {
			return false, errors.NewProcessingError("applied conflict winner %s changed before acknowledgement", h.String())
		}

		if m.Conflicting {
			settled = false
			continue
		}

		applied, err := appliedWinnerOwnsInputs(ctx, s, h, m, guard)
		if err != nil {
			return false, err
		}

		if !applied {
			return false, errors.NewProcessingError("applied conflict winner %s no longer owns its inputs", h.String())
		}
	}

	return settled, nil
}

// appliedWinnerOwnsInputs reports whether a winner that is not Conflicting still owns every input. A
// winner that the current chain confirms can have a pruned parent; its other inputs must still be
// owned. m must hold the tx and its block IDs.
func appliedWinnerOwnsInputs(ctx context.Context, s Store, h chainhash.Hash, m *meta.Data, guard CurrentChainGuard) (bool, error) {
	confirmed := false

	if guard.check != nil && len(m.BlockIDs) > 0 {
		var err error

		confirmed, err = guard.check(ctx, m.BlockIDs)
		if err != nil {
			return false, err
		}
	}

	absent := absentParentIsError
	if confirmed {
		absent = absentParentIsOwned
	}

	return winnerInputsOwned(ctx, s, h, m.Tx, absent)
}

// MarkConflictingRecursively marks the given transactions as conflicting, and iteratively marks all their spending
// children as conflicting too using breadth-first traversal.
//
// Parameters:
//   - ctx: The context for managing request-scoped values, cancellation signals, and deadlines.
//   - s: The UTXO store interface used to interact with the underlying data store.
//   - hashes: A slice of transaction hashes to be marked as conflicting.
//
// Returns:
//   - A slice of pointers to Spend structs representing the affected parent spends.
//   - A slice of all transaction hashes that were marked conflicting by this call,
//     including the input hashes and every descendant reached via BFS. Insertion
//     order is BFS order (input level first, then each descendant level) — callers
//     can rely on this for deterministic logs, traces, and eviction ordering.
//     On error it is still returned and lists every hash the call may have
//     flipped: the levels already written plus the level whose write failed.
//     Those stay conflicting in the store, so callers evict them from the txmeta
//     cache on error too; a retry may never re-flip them.
//   - An error if any issues occur during the process.
func MarkConflictingRecursively(ctx context.Context, s Store, hashes []chainhash.Hash) ([]*Spend, []chainhash.Hash, error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "MarkConflictingRecursively")

	defer deferFn()

	var allAffectedSpends []*Spend
	toProcess := hashes

	visited := make(map[chainhash.Hash]struct{}, len(hashes))
	markedOrder := make([]chainhash.Hash, 0, len(hashes))
	for _, h := range hashes {
		if _, ok := visited[h]; !ok {
			visited[h] = struct{}{}
			markedOrder = append(markedOrder, h)
		}
	}

	for len(toProcess) > 0 {
		affectedParentSpends, spendingChildTxs, err := s.SetConflicting(ctx, toProcess, true)
		if err != nil {
			return nil, markedOrder, err
		}

		allAffectedSpends = append(allAffectedSpends, affectedParentSpends...)

		// filter out already-visited hashes to prevent infinite loops
		nextBatch := spendingChildTxs[:0]
		for _, child := range spendingChildTxs {
			if _, ok := visited[child]; !ok {
				visited[child] = struct{}{}
				markedOrder = append(markedOrder, child)
				nextBatch = append(nextBatch, child)
			}
		}
		toProcess = nextBatch
	}

	return allAffectedSpends, markedOrder, nil
}

func GetAndLockChildren(ctx context.Context, s Store, hash chainhash.Hash) ([]chainhash.Hash, error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "GetAndLockChildren")

	defer deferFn()

	if hash.Equal(subtree.CoinbasePlaceholderHashValue) {
		// skip the coinbase placeholder hash
		return nil, errors.NewProcessingError("[GetAndLockChildren][%s] tx is frozen", hash.String())
	}

	visited := make(map[chainhash.Hash]struct{})
	visited[hash] = struct{}{}
	currentLevel := []chainhash.Hash{hash}

	for len(currentLevel) > 0 {
		results := make([]*meta.Data, len(currentLevel))
		g, gCtx := errgroup.WithContext(ctx)
		// Same per-level ceiling as GetConflictingChildren, same rationale — see
		// conflictingWalkFanOut.
		g.SetLimit(conflictingWalkFanOut)

		for i, current := range currentLevel {
			i := i
			current := current
			g.Go(func() error {
				if err := s.SetLocked(gCtx, []chainhash.Hash{current}, true); err != nil {
					return err
				}
				txMeta, err := s.Get(gCtx, &current, fields.Utxos)
				if err != nil {
					return err
				}
				results[i] = txMeta
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return nil, err
		}

		var nextLevel []chainhash.Hash

		for _, txMeta := range results {
			if txMeta == nil {
				continue
			}

			if txMeta.SpendingDatas != nil {
				for _, spendingData := range txMeta.SpendingDatas {
					if spendingData != nil {
						child := *spendingData.TxID
						if _, ok := visited[child]; ok {
							continue
						}

						if child.Equal(subtree.CoinbasePlaceholderHashValue) {
							return nil, errors.NewProcessingError("[GetAndLockChildren][%s] tx is frozen", child.String())
						}

						visited[child] = struct{}{}
						nextLevel = append(nextLevel, child)
					}
				}
			}
		}
		currentLevel = nextLevel
	}

	// exclude the root hash from the result
	delete(visited, hash)

	children := make([]chainhash.Hash, 0, len(visited))
	for child := range visited {
		children = append(children, child)
	}

	return children, nil
}

// GetConflictingChildren walks the descendant graph of the given transaction —
// every recorded spender plus explicit conflicting children — and returns all
// reachable transaction hashes (excluding the root). maxNodes bounds the walk:
// when the total number of visited transactions (including the root) would
// exceed it, the walk fails closed with ERR_UTXO_WALK_LIMIT_EXCEEDED. A
// maxNodes <= 0 disables the bound; the conflict-demotion path relies on this
// to always run to completion (issue 1391).
func GetConflictingChildren(ctx context.Context, s Store, hash chainhash.Hash, maxNodes int) ([]chainhash.Hash, error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "GetConflictingChildren",
		tracing.WithHistogram(prometheusUtxoConflictingWalkDuration),
	)

	defer deferFn()

	if hash.Equal(subtree.CoinbasePlaceholderHashValue) {
		// skip the coinbase placeholder hash
		return nil, nil
	}

	visited := make(map[chainhash.Hash]struct{})
	visited[hash] = struct{}{}
	currentLevel := []chainhash.Hash{hash}
	depth := 0

	// visit adds a child to the walk. The frozen sentinel stays in the result set
	// (callers check for it) but is never enqueued — it is not a real record. The
	// budget counts every visited transaction including the root; the check runs
	// per insert so a single wide level cannot overshoot it.
	var nextLevel []chainhash.Hash

	visit := func(child chainhash.Hash) error {
		if _, ok := visited[child]; ok {
			return nil
		}

		visited[child] = struct{}{}

		if maxNodes > 0 && len(visited) > maxNodes {
			return errors.NewUtxoWalkLimitExceededError("[GetConflictingChildren][%s] conflicting-descendant walk exceeded %d transactions (utxostore_conflictingChildrenMaxNodes)", hash.String(), maxNodes)
		}

		if !child.Equal(subtree.FrozenBytesTxHash) {
			nextLevel = append(nextLevel, child)
		}

		return nil
	}

	for len(currentLevel) > 0 {
		depth++

		results := make([]*meta.Data, len(currentLevel))
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(conflictingWalkFanOut)

		for i, current := range currentLevel {
			i := i
			current := current
			g.Go(func() error {
				txMeta, err := s.Get(gCtx, &current, fields.Utxos, fields.ConflictingChildren)
				if err != nil {
					return err
				}
				results[i] = txMeta
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return nil, err
		}

		nextLevel = nil

		for _, txMeta := range results {
			if txMeta == nil {
				continue
			}

			if txMeta.ConflictingChildren != nil {
				for _, child := range txMeta.ConflictingChildren {
					if err := visit(child); err != nil {
						return nil, err
					}
				}
			}

			if txMeta.SpendingDatas != nil {
				for _, spendingData := range txMeta.SpendingDatas {
					if spendingData != nil {
						if err := visit(*spendingData.TxID); err != nil {
							return nil, err
						}
					}
				}
			}
		}
		currentLevel = nextLevel
	}

	prometheusUtxoConflictingWalkNodes.Observe(float64(len(visited)))
	prometheusUtxoConflictingWalkDepth.Observe(float64(depth))

	// exclude the root hash from the result
	delete(visited, hash)

	conflictingChildren := make([]chainhash.Hash, 0, len(visited))
	for child := range visited {
		conflictingChildren = append(conflictingChildren, child)
	}

	return conflictingChildren, nil
}

// GetConflictingChildrenFromRoots is GetConflictingChildren for many roots at
// once. It walks each pass of roots in order, as one BFS per pass, on a single
// visited set shared by every pass, so a transaction reachable from several roots
// is read once rather than once per root. It returns, per pass, the descendants
// that pass newly visited: never a root, and never anything an earlier pass
// visited.
//
// maxNodes is the per-root budget of GetConflictingChildren. The walk as a whole
// may visit maxNodes times the number of distinct roots, which is the aggregate
// bound of walking each root separately, and fails closed with
// ERR_UTXO_WALK_LIMIT_EXCEEDED past that. The visited set can therefore reach
// that many entries, and a single runaway cone is stopped only once the walk has
// read that many transactions, not maxNodes. A maxNodes <= 0 disables the bound.
//
// Two differences from GetConflictingChildren, both deliberate:
//   - a root equal to the frozen sentinel is skipped rather than walked; the
//     sentinel is still returned when reached as a child, for the callers'
//     frozen checks.
//   - a transaction the store does not hold (ErrTxNotFound, or a nil record) is a
//     leaf, and the rest of the walk carries on. It is returned in missing, roots
//     included, so the caller can decide whether a gap fails its check. Any other
//     read error aborts the walk.
//
// A walk that stops on its budget or a read error still returns what it reached
// with the error: cones has one entry per pass, the pass it stopped in holds the
// descendants found so far, and later passes are empty. Every hash returned is a
// descendant of its pass's roots, so a caller may act on a partial cone.
//
// The walk histograms get one observation per call, for the whole walk; a walk
// that stops gets none.
func GetConflictingChildrenFromRoots(ctx context.Context, s Store, passes [][]chainhash.Hash, maxNodes int) (cones [][]chainhash.Hash, missing []chainhash.Hash, err error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "GetConflictingChildrenFromRoots",
		tracing.WithHistogram(prometheusUtxoConflictingWalkDuration),
	)

	defer deferFn()

	roots := distinctWalkRoots(passes)

	w := &multiRootWalk{
		s:        s,
		visited:  make(map[chainhash.Hash]struct{}, roots),
		budget:   multiRootWalkBudget(maxNodes, roots),
		roots:    roots,
		maxNodes: maxNodes,
	}

	cones = make([][]chainhash.Hash, len(passes))

	for p, pass := range passes {
		children, err := w.walkPass(ctx, pass)

		cones[p] = children

		if err != nil {
			return cones, w.missing, err
		}
	}

	prometheusUtxoConflictingWalkNodes.Observe(float64(len(w.visited)))
	prometheusUtxoConflictingWalkDepth.Observe(float64(w.depth))

	return cones, w.missing, nil
}

// distinctWalkRoots counts the distinct real roots across passes: the frozen
// sentinel is not a record and is never walked.
func distinctWalkRoots(passes [][]chainhash.Hash) int {
	roots := make(map[chainhash.Hash]struct{})

	for _, pass := range passes {
		for _, root := range pass {
			if !root.Equal(subtree.CoinbasePlaceholderHashValue) {
				roots[root] = struct{}{}
			}
		}
	}

	return len(roots)
}

// multiRootWalkBudget is maxNodes per root, or 0 (unbounded) when maxNodes <= 0
// or the product overflows, since no int budget could be exceeded then.
func multiRootWalkBudget(maxNodes, roots int) int {
	if maxNodes <= 0 {
		return 0
	}

	perRoot := max(1, roots)
	if perRoot > math.MaxInt/maxNodes {
		return 0
	}

	return maxNodes * perRoot
}

// multiRootWalk is the state GetConflictingChildrenFromRoots shares across its
// passes.
type multiRootWalk struct {
	s        Store
	visited  map[chainhash.Hash]struct{}
	budget   int
	roots    int
	maxNodes int
	depth    int
	missing  []chainhash.Hash
}

// add marks hash visited, reporting whether it was new, and fails closed once
// the walk outgrows its budget.
func (w *multiRootWalk) add(hash chainhash.Hash) (bool, error) {
	if _, ok := w.visited[hash]; ok {
		return false, nil
	}

	w.visited[hash] = struct{}{}

	if w.budget > 0 && len(w.visited) > w.budget {
		return false, errors.NewUtxoWalkLimitExceededError("[GetConflictingChildrenFromRoots] conflicting-descendant walk from %d root(s) exceeded %d transactions (%d per root, utxostore_conflictingChildrenMaxNodes)", w.roots, w.budget, w.maxNodes)
	}

	return true, nil
}

// walkPass walks one pass of roots breadth first and returns the descendants it
// newly visited. On an error it returns those visited so far with it.
func (w *multiRootWalk) walkPass(ctx context.Context, pass []chainhash.Hash) ([]chainhash.Hash, error) {
	var children, level []chainhash.Hash

	for _, root := range pass {
		if root.Equal(subtree.CoinbasePlaceholderHashValue) {
			continue
		}

		added, err := w.add(root)
		if err != nil {
			return children, err
		}

		if added {
			level = append(level, root)
		}
	}

	for len(level) > 0 {
		w.depth++

		current := level
		level = nil

		levelMetas, err := readConflictingWalkLevel(ctx, w.s, current)
		if err != nil {
			return children, err
		}

		for i, txMeta := range levelMetas {
			if txMeta == nil {
				w.missing = append(w.missing, current[i])
				continue
			}

			for _, child := range conflictingWalkChildren(txMeta) {
				added, err := w.add(child)
				if err != nil {
					return children, err
				}

				if !added {
					continue
				}

				children = append(children, child)

				// the frozen sentinel stays in the result for the callers'
				// frozen checks, but it is not a record and is never read
				if !child.Equal(subtree.FrozenBytesTxHash) {
					level = append(level, child)
				}
			}
		}
	}

	return children, nil
}

// readConflictingWalkLevel reads one BFS level, index-aligned, at most
// conflictingWalkFanOut at a time. A transaction the store does not hold is a nil
// entry, so it becomes a leaf; any other read error fails the level.
func readConflictingWalkLevel(ctx context.Context, s Store, level []chainhash.Hash) ([]*meta.Data, error) {
	levelMetas := make([]*meta.Data, len(level))

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(conflictingWalkFanOut)

	for i := range level {
		g.Go(func() error {
			txMeta, err := s.Get(gCtx, &level[i], fields.Utxos, fields.ConflictingChildren)
			if err != nil {
				if errors.Is(err, errors.ErrTxNotFound) {
					return nil
				}

				return err
			}

			levelMetas[i] = txMeta

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return levelMetas, nil
}

// conflictingWalkChildren returns the children the walk follows from one record,
// conflicting children first, as GetConflictingChildren orders them.
func conflictingWalkChildren(txMeta *meta.Data) []chainhash.Hash {
	children := make([]chainhash.Hash, 0, len(txMeta.ConflictingChildren)+len(txMeta.SpendingDatas))
	children = append(children, txMeta.ConflictingChildren...)

	for _, spendingData := range txMeta.SpendingDatas {
		if spendingData != nil {
			children = append(children, *spendingData.TxID)
		}
	}

	return children
}

// GetCounterConflictingTxHashes returns the given transaction plus, for every
// input, the transaction the store records as spending that same output (the
// counter-conflicting transaction) and that spender's full descendant set.
// maxNodes bounds each descendant walk (see GetConflictingChildren); <= 0
// means unbounded.
func GetCounterConflictingTxHashes(ctx context.Context, s Store, txHash chainhash.Hash, maxNodes int) ([]chainhash.Hash, error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "GetCounterConflictingTxHashes")

	defer deferFn()

	// Only the parent hashes are read below, so ask for the inpoints rather than
	// the whole transaction.
	txMeta, err := s.Get(ctx, &txHash, fields.TxInpoints)
	if err != nil {
		return nil, err
	}

	// A missing record surfaces as (nil, nil) on some backends (aerospike returns
	// nil for a not-found tx), which previously dereferenced straight to a panic.
	if txMeta == nil {
		return nil, errors.NewTxNotFoundError("[GetCounterConflictingTxHashes][%s] tx not found", txHash.String())
	}

	counterConflictingMap := make(map[chainhash.Hash]struct{})
	counterConflictingMap[txHash] = struct{}{}

	// get the unique parent txs
	parentTxs := make(map[chainhash.Hash][]*chainhash.Hash)

	for _, parentHash := range txMeta.TxInpoints.GetParentTxHashes() {
		// get the parent tx
		parentTxs[parentHash] = nil
	}

	for parentTx := range parentTxs {
		parentTxHash := &parentTx

		parentTxMeta, err := s.Get(ctx, parentTxHash, fields.Utxos)
		if err != nil {
			return nil, err
		}

		// Same (nil, nil) contract as the lookup above. Report it as not found
		// rather than skipping the parent: a parent the store does not hold says
		// nothing about who spent its outputs, so skipping would silently shrink
		// the counter-conflicting set.
		if parentTxMeta == nil {
			return nil, errors.NewTxNotFoundError("[GetCounterConflictingTxHashes][%s] parent tx %s not found", txHash.String(), parentTxHash.String())
		}

		spendingTxIDs := make([]*chainhash.Hash, len(parentTxMeta.SpendingDatas))

		for idx, spendingData := range parentTxMeta.SpendingDatas {
			if spendingData == nil {
				continue
			}

			spendingTxIDs[idx] = spendingData.TxID
		}

		parentTxs[*parentTxHash] = spendingTxIDs
	}

	// Walk the inpoints, not txMeta.Tx.Inputs. The Get above asks for
	// fields.TxInpoints only, so txMeta.Tx is nil on the SQL store on both read
	// paths. getUnbatched and batchDecorateChunk attach meta.Data.Tx only for
	// fields.Tx or fields.Outputs. settings.conf sets utxostore_getBatcherSize to
	// 4096, so the batched path is the live one, and sendGetBatch decorates each
	// field set separately, so a batch-mate asking for fields.Tx does not widen
	// this read.
	//
	// GetTxInpoints returns the outpoints grouped parent-then-vout rather than in
	// input order. Only the order of the descendant walks below changes with it,
	// and the result is accumulated into counterConflictingMap, so the returned
	// set is the same. It does decide which walk spends the maxNodes budget
	// first, which was already unspecified for a transaction with several
	// conflicting parents.
	inpoints := txMeta.TxInpoints.GetTxInpoints()

	// Collect the unique counter-spenders: several inputs are typically spent by
	// the same counter tx and its descendant walk must run only once, not once
	// per input. Dedupe on a dedicated set: counterConflictingMap is seeded with
	// txHash, and a spender equal to txHash itself must still be walked.
	seenSpenders := make(map[chainhash.Hash]struct{}, len(inpoints))
	uniqueSpendingTxIDs := make([]chainhash.Hash, 0, len(inpoints))

	for _, inpoint := range inpoints {
		parenTxIDS, ok := parentTxs[inpoint.Hash]
		if ok {
			// check the length of the spending txs, if it's less than the index, then the input is not spent
			if len(parenTxIDS) <= int(inpoint.Index) {
				// throw an error
				return nil, errors.NewProcessingError("[GetCounterConflictingTxHashes][%s] cannot process counter conflicting, input %d of %s is out of range (len: %d, %v)", txHash.String(), inpoint.Index, inpoint.Hash.String(), len(parenTxIDS), parenTxIDS)
			}

			spendingTxID := parenTxIDS[inpoint.Index]
			if spendingTxID != nil {
				counterConflictingMap[*spendingTxID] = struct{}{}

				if _, ok := seenSpenders[*spendingTxID]; !ok {
					seenSpenders[*spendingTxID] = struct{}{}
					uniqueSpendingTxIDs = append(uniqueSpendingTxIDs, *spendingTxID)
				}
			}
		}
	}

	for _, spendingTxID := range uniqueSpendingTxIDs {
		// call the package-level walk directly (not the Store method) so the
		// caller-chosen maxNodes budget flows into the BFS
		childHashes, err := GetConflictingChildren(ctx, s, spendingTxID, maxNodes)
		if err != nil {
			return nil, err
		}

		for _, childHash := range childHashes {
			if childHash.Equal(subtree.FrozenBytesTxHash) {
				return nil, errors.NewProcessingError("[GetCounterConflictingTxHashes][%s] tx has frozen child", spendingTxID.String())
			}

			counterConflictingMap[childHash] = struct{}{}
		}
	}

	counterConflicting := make([]chainhash.Hash, 0, len(counterConflictingMap))

	for child := range counterConflictingMap {
		counterConflicting = append(counterConflicting, child)
	}

	// fmt.Printf("counterConflicting: %v\n", counterConflicting)

	return counterConflicting, nil
}

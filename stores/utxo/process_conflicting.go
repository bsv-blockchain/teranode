// //go:build aerospike

// Package utxo provides UTXO (Unspent Transaction Output) management for the BSV Blockchain Teranode implementation.
//
// This file implements conflicting transaction processing functionality for handling double-spend scenarios
// and transaction conflicts in the UTXO store. It requires the aerospike build tag.
package utxo

import (
	"context"
	"fmt"
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

// step5RetryDelays controls the bounded back-off when SetLocked(false) fails at the very
// last step of ProcessConflicting. The slice length is the number of attempts; the value
// at index i is the delay BEFORE attempt i (so index 0 is always zero — the first attempt
// is immediate). Rolling back at step 5 would create more inconsistency than retrying a
// simple state update — see ProcessConflicting for rationale.
//
// Declared as a package-level var so tests can shrink the delays.
var step5RetryDelays = []time.Duration{0, 50 * time.Millisecond, 200 * time.Millisecond}

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
// A failure after step 2 restores the recorded owners and flags (RollbackConflictIntent). A retry
// of a run that reached applied or finished skips the mutation and returns the saved result.
//
// ProcessConflicting returns:
//   - losers: the transactions displaced by the winners, with their descendants. It never contains
//     a winner. Callers mark these in the subtrees.
//   - marked: the same set as a list, in discovery order. Block assembly uses it to reject children
//     of conflicting parents that arrive after the operation.
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
	intent := ConflictIntent{Recovery: &ConflictRecovery{Version: 1, Phase: ConflictRecoveryPlanning}, Kind: ConflictIntentForward, BlockHeight: blockHeight, BlockHash: blockHash, TxHashes: conflictingTxHashes, StartedAt: time.Now().UnixNano()}
	recovery, err := s.LoadConflictRecovery(ctx, intent.IntentID())
	if err != nil {
		return nil, nil, err
	}
	if recovery == nil {
		pending, e := s.PendingConflictIntents(ctx)
		if e != nil {
			return nil, nil, e
		}
		for _, old := range pending {
			if old.IntentID() == intent.IntentID() {
				return nil, nil, errors.NewProcessingError("legacy conflict intent %s has no recovery checklist; manual repair required", intent.IntentID().String())
			}
		}
	}
	if err = s.BeginConflictIntent(ctx, intent); err != nil {
		return nil, nil, errors.NewProcessingError("failed to record conflict WAL intent", err)
	}
	if recovery != nil {
		if err = recovery.Validate(); err != nil {
			return nil, nil, err
		}
		switch recovery.Phase {
		case ConflictRecoveryPrepared, ConflictRecoveryMutating, ConflictRecoveryRollingBack:
			if err = RollbackConflictIntent(ctx, s, intent, options.currentChainGuard); err != nil {
				return nil, nil, err
			}
		case ConflictRecoveryApplied, ConflictRecoveryFinished:
			// Recover the original output set, not edges that Unspend has erased.
			for _, hash := range conflictingTxHashes {
				m, e := s.Get(ctx, &hash, fields.Tx, fields.Conflicting)
				if e != nil {
					return nil, nil, e
				}
				if m == nil || m.Conflicting {
					return nil, nil, errors.NewProcessingError("applied conflict winner %s changed before acknowledgement", hash.String())
				}
				applied, e := winnerIsApplied(ctx, s, hash, m.Tx)
				if e != nil {
					return nil, nil, e
				}
				if !applied {
					return nil, nil, errors.NewProcessingError("applied conflict winner %s no longer owns its inputs", hash.String())
				}
			}
			return finishConflictRecovery(ctx, s, intent, recovery, options.deferCompletion)
		}
	}
	winnerSet := make(map[chainhash.Hash]struct{}, len(conflictingTxHashes))
	var winningSpends []*Spend
	var affected hashSet
	for _, h := range conflictingTxHashes {
		winnerSet[h] = struct{}{}
	}
	for _, h := range conflictingTxHashes {
		if h.Equal(subtree.CoinbasePlaceholderHashValue) {
			return nil, nil, errors.NewProcessingError("[ProcessConflicting][%s] tx is frozen", h.String())
		}
		m, e := s.Get(ctx, &h, fields.Tx, fields.Conflicting)
		if e != nil {
			return nil, nil, errors.NewProcessingError("[ProcessConflicting][%s] error getting tx", h.String(), e)
		}
		if m == nil {
			return nil, nil, errors.NewTxNotFoundError("[ProcessConflicting][%s] winning tx not found", h.String())
		}
		// A process-local processed map cannot prove the side effects of a previous
		// operation. Only a durable checklist authorises the applied retry above.
		if !m.Conflicting {
			return nil, nil, errors.NewProcessingError("[ProcessConflicting][%s] tx is not conflicting; no recovery checklist proves completion", h.String())
		}
		if m.Tx == nil {
			return nil, nil, errors.NewProcessingError("[ProcessConflicting][%s] winning transaction body is missing", h.String())
		}
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
			if e := CompleteConflictIntent(ctx, s, intent.IntentID()); e != nil {
				return nil, nil, e
			}
			return txmap.NewSplitSwissMap(0), nil, nil
		}
	}
	recovery, err = prepareConflictRecovery(ctx, s, affected.order, winningSpends)
	if err != nil {
		return nil, nil, err
	}
	recovery.Winners = append([]chainhash.Hash(nil), conflictingTxHashes...)
	for _, h := range affected.order {
		if _, isWinner := winnerSet[h]; !isWinner {
			recovery.Losers = append(recovery.Losers, h)
		}
	}
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
		if rollbackErr := RollbackConflictIntent(rollbackCtx, s, intent, options.currentChainGuard); rollbackErr != nil {
			err = errors.NewProcessingError("[ProcessConflicting] MANUAL INTERVENTION REQUIRED: original=%s rollback=%s", err.Error(), rollbackErr.Error(), errors.Join(err, rollbackErr))
		}
		losers = nil
		marked = nil
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
	var discovered []chainhash.Hash
	if _, discovered, err = s.SetConflicting(ctx, affected.order, true); err != nil {
		return nil, nil, err
	}
	// A validator can attach a child while the checklist is being prepared.
	// Do not erase that edge without a before-image. Roll back and plan again.
	for _, h := range discovered {
		if !affected.has(h) {
			return nil, nil, errors.NewProcessingError("conflict descendants changed during preparation; retry required")
		}
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
//     unmined-side cascade.
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

	cascadedConflictingSet := make(map[chainhash.Hash]struct{}, 2*len(demotedTxHashes))
	touchedSet := make(map[chainhash.Hash]struct{}, 2*len(demotedTxHashes))

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
		if markErr != nil {
			return nil, nil, errors.NewProcessingError("[ReverseProcessConflicting][%s] error marking demoted tx + descendants conflicting", demotedHash.String(), markErr)
		}

		for _, h := range markedOrder {
			cascadedConflictingSet[h] = struct{}{}
			touchedSet[h] = struct{}{}
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
// can produce one error per cascaded transaction, which reached 1.4 MB in issue 4991 and was
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
	if tx == nil || len(tx.Inputs) == 0 {
		return false, nil
	}

	for _, input := range tx.Inputs {
		parentHash := input.PreviousTxIDChainHash()
		vout := input.PreviousTxOutIndex

		parentMeta, err := s.Get(ctx, parentHash, fields.Utxos)
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

// setLockedWithRetry retries SetLocked with a bounded back-off — a deliberate exception
// to "always roll back". By the time we reach step 5 every other phase is committed and
// correct; the only inconsistency a SetLocked failure introduces is parents that are
// still locked. Rolling back here would re-introduce conflicting markers and unspend the
// winner — strictly worse than retrying a simple state update.
func setLockedWithRetry(ctx context.Context, s Store, hashes []chainhash.Hash, value bool) error {
	var err error

	for _, delay := range step5RetryDelays {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return errors.NewProcessingError("setLockedWithRetry aborted by context", ctx.Err())
			case <-time.After(delay):
			}
		}

		if err = s.SetLocked(ctx, hashes, value); err == nil {
			return nil
		}
	}

	return err
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
			return nil, nil, err
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

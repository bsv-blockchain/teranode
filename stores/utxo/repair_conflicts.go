package utxo

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// BlockHeaderInfo holds the minimal block header data needed by RepairConflictingChains.
type BlockHeaderInfo struct {
	Hash   *chainhash.Hash
	Height uint32
}

// BlockchainQuerier is the subset of blockchain.ClientI needed by RepairConflictingChains.
// Using a local interface with primitive types avoids an import cycle between stores/utxo and services/blockchain.
type BlockchainQuerier interface {
	GetBestBlockHeaderInfo(ctx context.Context) (BlockHeaderInfo, error)
	GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error)
}

// RepairReport contains the results of a conflict repair run.
type RepairReport struct {
	UnminedSinceFixed int // Txs fixed in step 0 (had block_ids on main chain but unmined_since still set)
	CaseAFixed        int // Losers fixed (missing Conflicting=true mark, cascaded to subtree)
	CaseCFixed        int // Inverted winner/loser pairs fixed via ProcessConflicting
	CaseDFixed        int // Orphan-conflicting parents unmarked (Conflicting=true with non-conflicting recorded spender and no evidence of a real conflict)
	CaseDCascaded     int // Legit-conflicting parents whose non-conflicting children were cascaded via MarkConflictingRecursively
}

// RepairProgressFunc is called by RepairConflictingChains to report progress.
type RepairProgressFunc func(format string, args ...interface{})

// cascadeMaxVisited bounds cascadeConflictingViaSpendingData so a corrupted or pathological
// SpendingData graph cannot exhaust memory. Exceeding this cap aborts repair with an error;
// a cascade this large indicates store corruption beyond what offline repair should resolve
// silently.
const cascadeMaxVisited = 100_000

// RepairOptions controls optional behavior of RepairConflictingChains. The zero value is
// safe and enables every step.
type RepairOptions struct {
	// SkipUnminedSinceScan skips step 0 — the full-store consistency scan that finds mined
	// transactions still carrying UnminedSince. That scan is the slowest part of repair on
	// a large store (hundreds of millions of records) and rarely finds anything once fixed.
	// Skipping it lets operators re-run steps 1-4 cheaply during iterative debugging. Only
	// set this when you're certain step 0 has run cleanly at least once since the last
	// change to the store.
	SkipUnminedSinceScan bool
}

// RepairConflictingChains detects and fixes inconsistent conflicting transaction state in the UTXO store.
// progressFn is optional — pass nil to suppress progress output.
//
// Any DB error encountered during detection or repair aborts the run and returns that error.
// The repair tool must not silently continue on a read or write failure — a partial repair
// leaves the store dirty, which is the condition we are trying to eliminate.
func RepairConflictingChains(ctx context.Context, s Store, blockchainClient BlockchainQuerier, dryRun bool, progressFn RepairProgressFunc, opts ...RepairOptions) (RepairReport, error) {
	var opt RepairOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	var report RepairReport

	logProgress := func(format string, args ...interface{}) {
		if progressFn != nil {
			progressFn(format, args...)
		}
	}

	// Best-chain header data is needed by Case A / Case C regardless of whether we skip
	// step 0, so fetch it unconditionally up front.
	bestHeader, err := blockchainClient.GetBestBlockHeaderInfo(ctx)
	if err != nil {
		return report, err
	}

	scanHeaders := uint64(bestHeader.Height + 1)

	bestBlockHeaderIDs, err := blockchainClient.GetBlockHeaderIDs(ctx, bestHeader.Hash, scanHeaders)
	if err != nil {
		return report, err
	}

	bestBlockIDsMap := make(map[uint32]bool, len(bestBlockHeaderIDs))
	for _, id := range bestBlockHeaderIDs {
		bestBlockIDsMap[id] = true
	}

	// Step 0: fix unmined_since inconsistencies — prerequisite so mined txs don't appear in
	// the iterator. This is the most expensive step (full-store scan, hundreds of millions
	// of records on a large node) and almost always a no-op after the first run, so
	// operators can opt out via RepairOptions.SkipUnminedSinceScan while iterating on
	// steps 1-4.
	if opt.SkipUnminedSinceScan {
		logProgress("[step 0/4] skipped (SkipUnminedSinceScan enabled)")
	} else {
		logProgress("[step 0/4] fixing unmined_since inconsistencies...")
		scanIt, sErr := s.ScanInconsistentUnminedTxs()
		if sErr != nil {
			return report, sErr
		}

		if scanIt != nil {
			defer scanIt.Close()

			lastReported := int64(0)
			for {
				batch, bErr := scanIt.Next(ctx)
				if bErr != nil {
					return report, bErr
				}
				if batch == nil {
					break
				}

				scanned := scanIt.TotalScanned()
				if scanned-lastReported >= 500_000 {
					logProgress("[step 0/4] scanned %d records, fixed %d unmined_since inconsistencies so far", scanned, report.UnminedSinceFixed)
					lastReported = scanned
				}

				var toMark []chainhash.Hash
				for _, rec := range batch {
					if rec.UnminedSince == 0 {
						continue
					}
					for _, blockID := range rec.BlockIDs {
						if bestBlockIDsMap[blockID] {
							toMark = append(toMark, rec.Hash)
							break
						}
					}
				}

				if len(toMark) > 0 {
					if !dryRun {
						if mErr := s.MarkTransactionsOnLongestChain(ctx, toMark, true); mErr != nil {
							return report, mErr
						}
					}
					report.UnminedSinceFixed += len(toMark)
				}
			}
			logProgress("[step 0/4] done — scanned %d total records, fixed %d unmined_since inconsistencies", scanIt.TotalScanned(), report.UnminedSinceFixed)
		}
	}

	// Steps 1-4: conflict detection and repair.
	type caseCPair struct {
		loser  chainhash.Hash
		winner chainhash.Hash
	}

	var caseALosers []chainhash.Hash
	var caseCPairs []caseCPair
	var caseDOrphans []chainhash.Hash
	// caseDDirectChildren records, for each orphan parent candidate, the non-conflicting
	// unmined children we saw spend it during step 1. These are needed if the parent turns
	// out to be a legit loser in step 4: cascadeConflictingViaSpendingData walks parent.SD,
	// which is often empty (spentUtxos=0) when the parent was already conflicting at the
	// time the children spent it — so the real children are invisible from the parent side.
	// We seed the cascade from these direct children instead, whose own SpendingData is
	// properly populated.
	caseDDirectChildren := map[chainhash.Hash][]chainhash.Hash{}

	// classificationCache memoizes classifyConflictingChildren results by parent hash
	// for the duration of step 1. The parent's ConflictingChildren list is stable during
	// detection (no writes happen before step 2), and the same big parents appear many
	// times — e.g. one mainnet parent had 341 entries in its list and was visited by
	// ~14k non-conflicting unmined children, each of which would otherwise re-run
	// ~341 Gets + grandparent lookups for every visit (5M+ Gets, many hitting external
	// storage). Caching makes step 1 linear in the number of distinct conflicting parents
	// rather than the number of scanned children times list size.
	type classification struct {
		orphans  []chainhash.Hash
		hasLegit bool
	}
	classificationCache := map[chainhash.Hash]classification{}

	classifyCached := func(parentHash chainhash.Hash, children []chainhash.Hash) ([]chainhash.Hash, bool, error) {
		if c, ok := classificationCache[parentHash]; ok {
			return c.orphans, c.hasLegit, nil
		}
		orphans, hasLegit, cErr := classifyConflictingChildren(ctx, s, children, logProgress)
		if cErr != nil {
			return nil, false, cErr
		}
		classificationCache[parentHash] = classification{orphans: orphans, hasLegit: hasLegit}
		return orphans, hasLegit, nil
	}

	// parentMetaCache memoizes parent Get() results during step 1 for the same reason —
	// a single big parent can be referenced by many children's inputs, and each Get on
	// an external tx triggers file-store I/O.
	parentMetaCache := map[chainhash.Hash]*meta.Data{}
	parentMetaNotFound := map[chainhash.Hash]bool{}
	fetchParent := func(parentHash *chainhash.Hash) (*meta.Data, error) {
		if parentMetaNotFound[*parentHash] {
			return nil, errors.ErrTxNotFound
		}
		if m, ok := parentMetaCache[*parentHash]; ok {
			return m, nil
		}
		m, err := s.Get(ctx, parentHash, fields.Utxos, fields.ConflictingChildren, fields.Conflicting)
		if err != nil {
			if errors.Is(err, errors.ErrTxNotFound) {
				parentMetaNotFound[*parentHash] = true
			}
			return nil, err
		}
		parentMetaCache[*parentHash] = m
		return m, nil
	}

	logProgress("[step 1/4] scanning unmined transactions for conflicts...")
	unminedIt, err := s.GetUnminedTxIterator()
	if err != nil {
		return report, err
	}
	defer unminedIt.Close()

	unminedScanned := 0
	lastLogScan := 0
	for {
		batch, bErr := unminedIt.Next(ctx)
		if bErr != nil {
			return report, bErr
		}
		if batch == nil {
			break
		}
		unminedScanned += len(batch)
		if unminedScanned-lastLogScan >= 10000 {
			logProgress("[step 1/4] scanned %d unmined txs, found %d Case A, %d Case C, %d Case D so far", unminedScanned, len(caseALosers), len(caseCPairs), len(caseDOrphans))
			lastLogScan = unminedScanned
		}

		for _, tx := range batch {
			if tx.Node == nil {
				continue
			}

			txHash := tx.Node.Hash
			if tx.Skip {
				continue
			}

			txMeta, gErr := s.Get(ctx, &txHash, fields.Conflicting, fields.Tx)
			if gErr != nil {
				if errors.Is(gErr, errors.ErrTxNotFound) {
					continue
				}
				return report, gErr
			}
			if txMeta == nil || txMeta.Conflicting || txMeta.Tx == nil {
				continue
			}

		inputLoop:
			for _, input := range txMeta.Tx.Inputs {
				parentHash := input.PreviousTxIDChainHash()
				vout := input.PreviousTxOutIndex

				// Fetch parent SpendingDatas + ConflictingChildren + Conflicting in one call
				// (memoized — the same big parent can be referenced by thousands of input
				// walks, and each Get on an external tx hits the file store).
				// ConflictingChildren is needed for Case C: siblings of txHash that are
				// recorded as (PARENT, sibling) in conflicting_children — not (txHash, sibling).
				// Conflicting is needed for Case D: parent is marked Conflicting=true but
				// the recorded spender (txHash) is not — indicates an orphan conflicting mark.
				parentMeta, pErr := fetchParent(parentHash)
				if pErr != nil {
					// TX_NOT_FOUND is a legitimate response: the parent simply isn't in the store
					// (e.g. an external reference pruned long ago, or a chain tip we never fetched).
					// Real DB errors still abort — this branch only filters the benign case.
					if errors.Is(pErr, errors.ErrTxNotFound) {
						continue
					}
					logProgress("[step 1/4] child %s: Get(parent %s) error: %v", txHash.String(), parentHash.String(), pErr)
					return report, pErr
				}
				if parentMeta == nil {
					logProgress("[step 1/4] child %s: parent %s not found in store (nil meta)", txHash.String(), parentHash.String())
					continue
				}
				if int(vout) >= len(parentMeta.SpendingDatas) {
					// Corrupted store: child tx names parent[vout] but parent has fewer outputs
					// than the requested vout. Abort — silently skipping this input would risk
					// missing a real conflict and leave the store in an unclear state.
					logProgress("[step 1/4] child %s: parent %s SpendingDatas len=%d, vout=%d out of range",
						txHash.String(), parentHash.String(), len(parentMeta.SpendingDatas), vout)
					return report, errors.NewProcessingError("[repair] corrupt store: %s input vout %d exceeds parent %s SpendingDatas length %d", txHash.String(), vout, parentHash.String(), len(parentMeta.SpendingDatas))
				}

				spendingData := parentMeta.SpendingDatas[vout]
				if spendingData == nil || spendingData.TxID == nil {
					// SpendingData may be unpopulated if the parent was already conflicting
					// when this child's Spend ran (some code paths skip the SD write).
					// If the parent is flagged conflicting with no *legit* conflicting
					// children, that's a Case D orphan candidate — enqueue it, along with
					// any orphan conflicting children found (which themselves need unmarking
					// so they stop blocking every parent that references them).
					if parentMeta.Conflicting {
						orphanChildren, hasLegit, cErr := classifyCached(*parentHash, parentMeta.ConflictingChildren)
						if cErr != nil {
							return report, cErr
						}
						if !hasLegit {
							caseDOrphans = append(caseDOrphans, *parentHash)
							caseDOrphans = append(caseDOrphans, orphanChildren...)
							caseDDirectChildren[*parentHash] = append(caseDDirectChildren[*parentHash], txHash)
						}
					}
					continue
				}

				if !spendingData.TxID.IsEqual(&txHash) {
					// Case A: this tx is a loser not yet marked conflicting.
					caseALosers = append(caseALosers, txHash)
					break inputLoop
				}

				// txHash is the recorded winner per SpendingData — check PARENT's
				// ConflictingChildren for any sibling that is confirmed on the best chain.
				// GetCounterConflictingTxHashes is intentionally avoided here: it traverses
				// txHash's own ConflictingChildren, missing siblings stored as (PARENT, sibling).
				for _, sibling := range parentMeta.ConflictingChildren {
					siblingMeta, sErr := s.Get(ctx, &sibling, fields.BlockIDs, fields.Conflicting)
					if sErr != nil {
						if errors.Is(sErr, errors.ErrTxNotFound) {
							continue
						}
						return report, sErr
					}
					if siblingMeta == nil {
						continue
					}
					// Stale back-reference: sibling is listed in ConflictingChildren but has
					// since been unmarked (e.g. by an earlier repair). ProcessConflicting
					// requires Conflicting=true on the winner and would fail with "tx is not
					// conflicting" — which since errors are fatal would abort the whole run
					// over a leftover pointer.
					if !siblingMeta.Conflicting {
						continue
					}
					for _, blockID := range siblingMeta.BlockIDs {
						if bestBlockIDsMap[blockID] {
							// Case C: sibling is confirmed on best chain — it's the real winner,
							// txHash is the fake winner.
							caseCPairs = append(caseCPairs, caseCPair{loser: txHash, winner: sibling})
							break
						}
					}
				}

				// Case D: parent is marked Conflicting=true, but its own SpendingData records
				// txHash (the non-conflicting unmined tx currently being scanned) as the spender
				// of this output, and parent has no *active* ConflictingChildren. The iterator
				// would skip parent (conflicting filter), so validateParentChain later fails with
				// "parent is unmined but not in processing list". Collect for step 4 validation
				// + unmarking.
				//
				// "Active" conflicting children means children that are themselves still
				// conflicting=true — after Case D unmarks a parent, its entry in a grandparent's
				// conflictingCs becomes a stale back-reference that must not block orphan
				// detection one level up.
				if parentMeta.Conflicting {
					orphanChildren, hasLegit, cErr := classifyCached(*parentHash, parentMeta.ConflictingChildren)
					if cErr != nil {
						return report, cErr
					}
					if !hasLegit {
						caseDOrphans = append(caseDOrphans, *parentHash)
						caseDOrphans = append(caseDOrphans, orphanChildren...)
						caseDDirectChildren[*parentHash] = append(caseDDirectChildren[*parentHash], txHash)
					}
				}

				break inputLoop
			}
		}
	}

	logProgress("[step 1/4] done — scanned %d unmined txs, found %d Case A, %d Case C, %d Case D", unminedScanned, len(caseALosers), len(caseCPairs), len(caseDOrphans))

	// Fix Case C first so SpendingData is corrected before the Case A sweep.
	// Dedup by WINNER: the same real-winner may appear as the correction target for multiple
	// losers, but ProcessConflicting needs to run on each distinct winner exactly once.
	// Deduping by loser would silently drop subsequent distinct winners and leave their
	// Conflicting=true flag set. ProcessConflicting's own dedup map only bypasses the
	// "must be conflicting" reentry check during reorgs; it does not help here.
	logProgress("[step 2/4] fixing %d Case C (inverted winner/loser) pairs...", len(caseCPairs))
	currentBlockHeight := bestHeader.Height
	seenCaseCWinners := map[chainhash.Hash]bool{}
	for _, pair := range caseCPairs {
		if seenCaseCWinners[pair.winner] {
			continue
		}
		seenCaseCWinners[pair.winner] = true
		if !dryRun {
			// processedConflictingHashesMap here is local to this single call — callers
			// outside reorg flows pass a fresh empty map.
			if _, pErr := ProcessConflicting(ctx, s, currentBlockHeight, []chainhash.Hash{pair.winner}, map[chainhash.Hash]bool{}); pErr != nil {
				return report, pErr
			}
		}
		report.CaseCFixed++
	}

	logProgress("[step 3/4] fixing %d Case A (unmarked losers)...", len(caseALosers))
	// Fix Case A, skipping any already resolved by Case C.
	for _, loser := range caseALosers {
		freshMeta, gErr := s.Get(ctx, &loser, fields.Conflicting)
		if gErr != nil {
			if errors.Is(gErr, errors.ErrTxNotFound) {
				continue
			}
			return report, gErr
		}
		if freshMeta != nil && freshMeta.Conflicting {
			continue
		}
		if !dryRun {
			if _, mErr := MarkConflictingRecursively(ctx, s, []chainhash.Hash{loser}); mErr != nil {
				return report, mErr
			}
		}
		report.CaseAFixed++
	}

	// Step 4: Case D — unmark orphan-conflicting parents.
	// An orphan conflicting parent is a tx with Conflicting=true whose recorded spender
	// (per its own SpendingData) is a non-conflicting unmined tx, and which has no entries
	// in ConflictingChildren. Such a parent is invisible to GetUnminedTxIterator (filtered
	// by Conflicting=true) yet still has unminedSince set, so BlockAssembler's parent-chain
	// validation fails with "parent is unmined but not in processing list" on every restart.
	//
	// Before unmarking, we verify there is no legitimate reason for parent to be conflicting
	// by checking each of parent's inputs: if any grandparent's SpendingData[vout] names a
	// different tx, parent is a legit loser and we leave it alone.
	logProgress("[step 4/4] fixing %d Case D (orphan conflicting parents)...", len(caseDOrphans))
	seenCaseD := map[chainhash.Hash]bool{}
	worklist := append([]chainhash.Hash(nil), caseDOrphans...)
	for len(worklist) > 0 {
		var nextWave []chainhash.Hash
		for _, parentHash := range worklist {
			if seenCaseD[parentHash] {
				continue
			}
			seenCaseD[parentHash] = true

			// Re-fetch fresh. Previous steps may have cascaded conflict marks that change the picture.
			freshParent, gErr := s.Get(ctx, &parentHash, fields.Conflicting, fields.ConflictingChildren, fields.Tx, fields.Utxos)
			if gErr != nil {
				if errors.Is(gErr, errors.ErrTxNotFound) {
					continue
				}
				return report, gErr
			}
			if freshParent == nil || !freshParent.Conflicting {
				continue
			}
			_, hasLegit, cErr := classifyConflictingChildren(ctx, s, freshParent.ConflictingChildren, nil)
			if cErr != nil {
				return report, cErr
			}
			if hasLegit {
				continue
			}
			if freshParent.Tx == nil {
				// Cannot verify without inputs — skip for safety.
				continue
			}

			legitConflict := false
			// grandparentCandidates holds grandparents that might themselves be orphan-conflicting:
			// they are ancestors that (per their own SpendingData) name `parentHash` as the spender
			// OR have no SpendingData for the relevant output (typical when the grandparent was
			// already conflicting when the child's Spend ran). We record them while walking
			// parent's inputs so that after we unmark parent, we can extend the worklist upward —
			// chains of orphan conflicting parents need iterative unmarking because Case D
			// detection in step 1 only fires from a non-conflicting child, and grandparents are
			// invisible until their direct child is unmarked.
			var grandparentCandidates []chainhash.Hash
			for _, pin := range freshParent.Tx.Inputs {
				gpHash := pin.PreviousTxIDChainHash()
				gpVout := pin.PreviousTxOutIndex
				gpMeta, gpErr := s.Get(ctx, gpHash, fields.Utxos)
				if gpErr != nil {
					if errors.Is(gpErr, errors.ErrTxNotFound) {
						continue
					}
					return report, gpErr
				}
				if gpMeta == nil {
					continue
				}
				if int(gpVout) < len(gpMeta.SpendingDatas) {
					gpSD := gpMeta.SpendingDatas[gpVout]
					if gpSD != nil && gpSD.TxID != nil && !gpSD.TxID.IsEqual(&parentHash) {
						legitConflict = true
						break
					}
				}
				grandparentCandidates = append(grandparentCandidates, *gpHash)
			}

			if legitConflict {
				// Parent is genuinely a loser (some grandparent's SpendingData names a different
				// spender). Cascade the conflicting mark down via SpendingData so the non-conflicting
				// child we scanned — and any descendants — also become conflicting. Without this the
				// child stays visible to GetUnminedTxIterator and validateParentChain still trips.
				//
				// Why not MarkConflictingRecursively: its cascade relies on SetConflicting's return
				// value, which is built from GetSpend. GetSpend reports Status=CONFLICTING on a
				// parent's outputs once the parent is flagged, masking the real spender and returning
				// an empty child list — so the recursion stops at the parent. We walk SpendingData
				// directly here to bypass that filter.
				//
				// Parent's own SpendingDatas can be empty (spentUtxos=0) when the parent was already
				// conflicting at the time its children ran Spend — the SD write gets skipped. In
				// that case cascadeConflictingViaSpendingData(parent) finds zero descendants and
				// the real children stay visible to the iterator. Seed the cascade from the direct
				// children we saw in step 1, whose own SpendingDatas are properly populated.
				if !dryRun {
					if cErr := cascadeConflictingViaSpendingData(ctx, s, parentHash); cErr != nil {
						return report, cErr
					}
					if directChildren := caseDDirectChildren[parentHash]; len(directChildren) > 0 {
						if _, _, sErr := s.SetConflicting(ctx, directChildren, true); sErr != nil {
							return report, sErr
						}
						for _, childHash := range directChildren {
							if cErr := cascadeConflictingViaSpendingData(ctx, s, childHash); cErr != nil {
								return report, cErr
							}
						}
					}
				}
				report.CaseDCascaded++
				continue
			}

			if !dryRun {
				if _, _, sErr := s.SetConflicting(ctx, []chainhash.Hash{parentHash}, false); sErr != nil {
					return report, sErr
				}
			}
			report.CaseDFixed++

			// Parent was unmarked. Any grandparent that (a) is Conflicting=true, (b) has
			// SpendingData naming this parent as the recorded spender OR no SpendingData for
			// the relevant vout at all, and (c) has no *active* conflicting children is a
			// candidate orphan one level up. Enqueue for re-evaluation.
			for _, gpHash := range grandparentCandidates {
				if seenCaseD[gpHash] {
					continue
				}
				gpMeta, ggErr := s.Get(ctx, &gpHash, fields.Conflicting, fields.ConflictingChildren)
				if ggErr != nil {
					if errors.Is(ggErr, errors.ErrTxNotFound) {
						continue
					}
					return report, ggErr
				}
				if gpMeta == nil || !gpMeta.Conflicting {
					continue
				}
				gpOrphans, gpHasLegit, gpCErr := classifyConflictingChildren(ctx, s, gpMeta.ConflictingChildren, nil)
				if gpCErr != nil {
					return report, gpCErr
				}
				if gpHasLegit {
					continue
				}
				nextWave = append(nextWave, gpHash)
				// Any orphan children blocking gp's classification should also be unmarked.
				nextWave = append(nextWave, gpOrphans...)
			}
		}
		worklist = nextWave
	}

	logProgress("[done] repair complete — fixed %d unmined_since, %d Case A, %d Case C, %d Case D (unmark), %d Case D (cascade)", report.UnminedSinceFixed, report.CaseAFixed, report.CaseCFixed, report.CaseDFixed, report.CaseDCascaded)
	return report, nil
}

// classifyConflictingChildren walks a parent's ConflictingChildren list and splits the
// entries into three buckets:
//   - hasLegit=true: at least one entry is a legit loser (Conflicting=true AND some
//     grandparent's SpendingData names a different spender for one of that child's
//     inputs). If hasLegit is true, the parent must NOT be treated as a Case D orphan.
//   - orphans: entries that are currently Conflicting=true but have no credible reason
//     (every input's grandparent either names the child itself, has nil SpendingData, or
//     is missing). These should be unmarked alongside the parent — they're typically
//     what blocks detection (parent.conflictingCs points at an already-orphan child).
//   - stale back-references (child.Conflicting=false now) are silently ignored.
//
// Without this classification, a conflictingCs entry that is itself orphan would leave
// every parent that references it stuck conflicting forever: the orphan child can never
// be reached via the unmined iterator (conflicting=true filters it), and the child's
// presence in the list blocks the parent from being added as a Case D candidate.
//
// Any read error is propagated — callers must abort rather than guess at the state,
// per the project rule that DB errors never get swallowed during repair.
//
// If logReason is non-nil, it is invoked with a human-readable explanation whenever an
// entry is classified as legit — useful for step-1 debug diagnostics.
func classifyConflictingChildren(ctx context.Context, s Store, children []chainhash.Hash, logReason RepairProgressFunc) (orphans []chainhash.Hash, hasLegit bool, err error) {
	for i := range children {
		h := children[i]
		m, gErr := s.Get(ctx, &h, fields.Conflicting, fields.Tx)
		if gErr != nil {
			if errors.Is(gErr, errors.ErrTxNotFound) {
				// Stale back-reference — child no longer exists.
				continue
			}
			return nil, false, gErr
		}
		if m == nil || !m.Conflicting {
			// Stale back-reference or already-unmarked child — ignore.
			continue
		}
		if m.Tx == nil {
			// Can't verify without inputs — treat conservatively as legit.
			hasLegit = true
			if logReason != nil {
				logReason("  classifyConflictingChildren: child %s has Conflicting=true, cannot verify (no Tx) — treating as legit", h.String())
			}
			continue
		}

		legitLoser := false
		for _, in := range m.Tx.Inputs {
			gp, gpErr := s.Get(ctx, in.PreviousTxIDChainHash(), fields.Utxos)
			if gpErr != nil {
				if errors.Is(gpErr, errors.ErrTxNotFound) {
					continue
				}
				return nil, false, gpErr
			}
			if gp == nil {
				continue
			}
			if int(in.PreviousTxOutIndex) >= len(gp.SpendingDatas) {
				continue
			}
			sd := gp.SpendingDatas[in.PreviousTxOutIndex]
			if sd != nil && sd.TxID != nil && !sd.TxID.IsEqual(&h) {
				legitLoser = true
				break
			}
		}

		if legitLoser {
			hasLegit = true
			if logReason != nil {
				logReason("  classifyConflictingChildren: child %s is legit loser (grandparent SpendingData names a different spender)", h.String())
			}
		} else {
			orphans = append(orphans, h)
		}
	}
	return orphans, hasLegit, nil
}

// cascadeConflictingViaSpendingData walks SpendingData descendants of rootHash and marks
// each one Conflicting=true. Unlike MarkConflictingRecursively, this traversal reads
// SpendingData directly from each tx's Utxos field, so it is not blocked by GetSpend
// returning Status_CONFLICTING once a parent is flagged — which stops the standard
// cascade the moment the root is already conflicting.
//
// The visited set is capped at cascadeMaxVisited: exceeding the cap aborts with an error
// rather than growing unbounded on a pathological or corrupted SpendingData graph.
// SetConflicting is issued per-level so a frontier of N children becomes one call, not N.
func cascadeConflictingViaSpendingData(ctx context.Context, s Store, rootHash chainhash.Hash) error {
	visited := map[chainhash.Hash]struct{}{rootHash: {}}
	frontier := []chainhash.Hash{rootHash}

	for len(frontier) > 0 {
		var nextHashes []chainhash.Hash
		for _, h := range frontier {
			meta, err := s.Get(ctx, &h, fields.Utxos)
			if err != nil {
				if errors.Is(err, errors.ErrTxNotFound) {
					continue
				}
				return err
			}
			if meta == nil {
				continue
			}
			for _, sd := range meta.SpendingDatas {
				if sd == nil || sd.TxID == nil {
					continue
				}
				childHash := *sd.TxID
				if _, seen := visited[childHash]; seen {
					continue
				}
				if len(visited) >= cascadeMaxVisited {
					return errors.NewProcessingError("[repair] cascade exceeded %d descendants from %s — store corruption or runaway graph", cascadeMaxVisited, rootHash.String())
				}
				visited[childHash] = struct{}{}
				nextHashes = append(nextHashes, childHash)
			}
		}
		if len(nextHashes) > 0 {
			if _, _, sErr := s.SetConflicting(ctx, nextHashes, true); sErr != nil {
				return sErr
			}
		}
		frontier = nextHashes
	}

	return nil
}

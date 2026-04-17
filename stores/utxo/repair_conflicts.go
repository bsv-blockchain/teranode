package utxo

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
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

// RepairConflictingChains detects and fixes inconsistent conflicting transaction state in the UTXO store.
// progressFn is optional — pass nil to suppress progress output.
//
// Any DB error encountered during detection or repair aborts the run and returns that error.
// The repair tool must not silently continue on a read or write failure — a partial repair
// leaves the store dirty, which is the condition we are trying to eliminate.
func RepairConflictingChains(ctx context.Context, s Store, blockchainClient BlockchainQuerier, dryRun bool, progressFn RepairProgressFunc) (RepairReport, error) {
	var report RepairReport

	logProgress := func(format string, args ...interface{}) {
		if progressFn != nil {
			progressFn(format, args...)
		}
	}

	// Step 0: fix unmined_since inconsistencies — prerequisite so mined txs don't appear in the iterator.
	logProgress("[step 0/4] fixing unmined_since inconsistencies...")
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

	scanIt, err := s.ScanInconsistentUnminedTxs()
	if err != nil {
		return report, err
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

	// Steps 1-4: conflict detection and repair.
	type caseCPair struct {
		loser  chainhash.Hash
		winner chainhash.Hash
	}

	var caseALosers []chainhash.Hash
	var caseCPairs []caseCPair
	var caseDOrphans []chainhash.Hash

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

				// Fetch parent SpendingDatas + ConflictingChildren + Conflicting in one call.
				// ConflictingChildren is needed for Case C: siblings of txHash that are
				// recorded as (PARENT, sibling) in conflicting_children — not (txHash, sibling).
				// Conflicting is needed for Case D: parent is marked Conflicting=true but
				// the recorded spender (txHash) is not — indicates an orphan conflicting mark.
				parentMeta, pErr := s.Get(ctx, parentHash, fields.Utxos, fields.ConflictingChildren, fields.Conflicting)
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
					// If the parent is flagged conflicting with no active conflicting children,
					// that's still a Case D orphan candidate — enqueue it. We can't run Case A
					// or Case C with no SpendingData, so fall through to the next input.
					if parentMeta.Conflicting {
						logProgress("[step 1/4] child %s input[vout=%d] → parent %s: Conflicting=true, SD=nil, ConflictingChildren=%v",
							txHash.String(), vout, parentHash.String(), parentMeta.ConflictingChildren)
						active, hErr := hasActiveConflictingChildren(ctx, s, parentMeta.ConflictingChildren, logProgress)
						if hErr != nil {
							return report, hErr
						}
						if !active {
							logProgress("[step 1/4]   → adding %s to Case D orphans (nil SD branch)", parentHash.String())
							caseDOrphans = append(caseDOrphans, *parentHash)
						} else {
							logProgress("[step 1/4]   → NOT adding %s (active conflicting children found)", parentHash.String())
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
					siblingMeta, sErr := s.Get(ctx, &sibling, fields.BlockIDs)
					if sErr != nil {
						if errors.Is(sErr, errors.ErrTxNotFound) {
							continue
						}
						return report, sErr
					}
					if siblingMeta == nil {
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
					logProgress("[step 1/4] child %s input[vout=%d] → parent %s: Conflicting=true, SD.TxID=%s, ConflictingChildren=%v",
						txHash.String(), vout, parentHash.String(), spendingData.TxID.String(), parentMeta.ConflictingChildren)
					active, hErr := hasActiveConflictingChildren(ctx, s, parentMeta.ConflictingChildren, logProgress)
					if hErr != nil {
						return report, hErr
					}
					if !active {
						logProgress("[step 1/4]   → adding %s to Case D orphans (match SD branch)", parentHash.String())
						caseDOrphans = append(caseDOrphans, *parentHash)
					} else {
						logProgress("[step 1/4]   → NOT adding %s (active conflicting children found)", parentHash.String())
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
			active, hErr := hasActiveConflictingChildren(ctx, s, freshParent.ConflictingChildren, nil)
			if hErr != nil {
				return report, hErr
			}
			if active {
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
				if !dryRun {
					if cErr := cascadeConflictingViaSpendingData(ctx, s, parentHash); cErr != nil {
						return report, cErr
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
				gpActive, gpHErr := hasActiveConflictingChildren(ctx, s, gpMeta.ConflictingChildren, nil)
				if gpHErr != nil {
					return report, gpHErr
				}
				if gpActive {
					continue
				}
				nextWave = append(nextWave, gpHash)
			}
		}
		worklist = nextWave
	}

	logProgress("[done] repair complete — fixed %d unmined_since, %d Case A, %d Case C, %d Case D (unmark), %d Case D (cascade)", report.UnminedSinceFixed, report.CaseAFixed, report.CaseCFixed, report.CaseDFixed, report.CaseDCascaded)
	return report, nil
}

// hasActiveConflictingChildren returns true if any hash in children still has
// Conflicting=true. Stale entries in ConflictingChildren (e.g. after Case D unmarks a
// formerly-conflicting child) must not block orphan detection upstream.
//
// Any read error is propagated — callers must abort rather than guess at the state,
// per the project rule that DB errors never get swallowed during repair.
//
// If logReason is non-nil, it is invoked with a human-readable explanation whenever the
// function returns true — useful for step-1 debug diagnostics to see why a parent was
// NOT classified as an orphan candidate.
func hasActiveConflictingChildren(ctx context.Context, s Store, children []chainhash.Hash, logReason RepairProgressFunc) (bool, error) {
	for i := range children {
		h := children[i]
		m, err := s.Get(ctx, &h, fields.Conflicting)
		if err != nil {
			if errors.Is(err, errors.ErrTxNotFound) {
				// Child recorded in ConflictingChildren no longer exists in the store;
				// treat as no longer active and keep scanning. A real DB error still propagates.
				continue
			}
			return false, err
		}
		if m != nil && m.Conflicting {
			if logReason != nil {
				logReason("  hasActiveConflictingChildren: child %s is still Conflicting=true", h.String())
			}
			return true, nil
		}
	}
	return false, nil
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

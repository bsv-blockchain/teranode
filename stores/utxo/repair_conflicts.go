package utxo

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
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
	UnminedSinceFixed int     // Txs fixed in step 0 (had block_ids on main chain but unmined_since still set)
	CaseAFixed        int     // Losers fixed (missing Conflicting=true mark, cascaded to subtree)
	CaseCFixed        int     // Inverted winner/loser pairs fixed via ProcessConflicting
	CaseDFixed        int     // Orphan-conflicting parents unmarked (Conflicting=true with non-conflicting recorded spender and no evidence of a real conflict)
	CaseDCascaded     int     // Legit-conflicting parents whose non-conflicting children were cascaded via MarkConflictingRecursively
	Errors            []error // Non-fatal errors encountered during repair
}

// RepairProgressFunc is called by RepairConflictingChains to report progress.
type RepairProgressFunc func(format string, args ...interface{})

// RepairConflictingChains detects and fixes inconsistent conflicting transaction state in the UTXO store.
// progressFn is optional — pass nil to suppress progress output.
func RepairConflictingChains(ctx context.Context, s Store, blockchainClient BlockchainQuerier, dryRun bool, progressFn ...RepairProgressFunc) (RepairReport, error) {
	var report RepairReport

	logProgress := func(format string, args ...interface{}) {
		if len(progressFn) > 0 && progressFn[0] != nil {
			progressFn[0](format, args...)
		}
	}

	// Step 0: fix unmined_since inconsistencies — prerequisite so mined txs don't appear in the iterator.
	logProgress("[step 0/3] fixing unmined_since inconsistencies...")
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
				logProgress("[step 0/3] scanned %d records, fixed %d unmined_since inconsistencies so far", scanned, report.UnminedSinceFixed)
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
		logProgress("[step 0/3] done — scanned %d total records, fixed %d unmined_since inconsistencies", scanIt.TotalScanned(), report.UnminedSinceFixed)
	}

	// Steps 1-3: conflict detection and repair.
	type caseCPair struct {
		loser  chainhash.Hash
		winner chainhash.Hash
	}

	var caseALosers []chainhash.Hash
	var caseCPairs []caseCPair
	var caseDOrphans []chainhash.Hash
	processedMap := map[chainhash.Hash]bool{}

	logProgress("[step 1/3] scanning unmined transactions for conflicts...")
	unminedIt, err := s.GetUnminedTxIterator()
	if err != nil {
		return report, err
	}
	defer unminedIt.Close()

	unminedScanned := 0
	for {
		batch, bErr := unminedIt.Next(ctx)
		if bErr != nil {
			return report, bErr
		}
		if batch == nil {
			break
		}
		unminedScanned += len(batch)
		if unminedScanned%10000 == 0 {
			logProgress("[step 1/4] scanned %d unmined txs, found %d Case A, %d Case C, %d Case D so far", unminedScanned, len(caseALosers), len(caseCPairs), len(caseDOrphans))
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
				report.Errors = append(report.Errors, gErr)
				continue
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
					report.Errors = append(report.Errors, pErr)
					continue
				}
				if parentMeta == nil {
					continue
				}
				if int(vout) >= len(parentMeta.SpendingDatas) {
					continue
				}

				spendingData := parentMeta.SpendingDatas[vout]
				if spendingData == nil || spendingData.TxID == nil {
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
					sibling := sibling
					siblingMeta, sErr := s.Get(ctx, &sibling, fields.BlockIDs)
					if sErr != nil {
						report.Errors = append(report.Errors, sErr)
						continue
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
				// of this output, and parent has no ConflictingChildren. The iterator would skip
				// parent (conflicting filter), so validateParentChain later fails with "parent is
				// unmined but not in processing list". Collect for step 4 validation + unmarking.
				if parentMeta.Conflicting && len(parentMeta.ConflictingChildren) == 0 {
					caseDOrphans = append(caseDOrphans, *parentHash)
				}

				break inputLoop
			}
		}
	}

	logProgress("[step 1/4] done — scanned %d unmined txs, found %d Case A, %d Case C, %d Case D", unminedScanned, len(caseALosers), len(caseCPairs), len(caseDOrphans))

	// Fix Case C first so SpendingData is corrected before the Case A sweep.
	// Dedup key is pair.loser (the fake winner tx being corrected): each unmined loser should be
	// processed at most once even if multiple inputs point to the same real winner.
	logProgress("[step 2/4] fixing %d Case C (inverted winner/loser) pairs...", len(caseCPairs))
	currentBlockHeight := bestHeader.Height
	for _, pair := range caseCPairs {
		if processedMap[pair.loser] {
			continue
		}
		if !dryRun {
			if _, pErr := ProcessConflicting(ctx, s, currentBlockHeight, []chainhash.Hash{pair.winner}, processedMap); pErr != nil {
				report.Errors = append(report.Errors, pErr)
				continue
			}
		}
		report.CaseCFixed++
		processedMap[pair.loser] = true
	}

	logProgress("[step 3/4] fixing %d Case A (unmarked losers)...", len(caseALosers))
	// Fix Case A, skipping any already resolved by Case C.
	for _, loser := range caseALosers {
		freshMeta, gErr := s.Get(ctx, &loser, fields.Conflicting)
		if gErr != nil {
			report.Errors = append(report.Errors, gErr)
			continue
		}
		if freshMeta != nil && freshMeta.Conflicting {
			continue
		}
		if !dryRun {
			if _, mErr := MarkConflictingRecursively(ctx, s, []chainhash.Hash{loser}); mErr != nil {
				report.Errors = append(report.Errors, mErr)
				continue
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
	for _, parentHash := range caseDOrphans {
		parentHash := parentHash
		if seenCaseD[parentHash] {
			continue
		}
		seenCaseD[parentHash] = true

		// Re-fetch fresh. Previous steps may have cascaded conflict marks that change the picture.
		freshParent, gErr := s.Get(ctx, &parentHash, fields.Conflicting, fields.ConflictingChildren, fields.Tx)
		if gErr != nil {
			report.Errors = append(report.Errors, gErr)
			continue
		}
		if freshParent == nil || !freshParent.Conflicting || len(freshParent.ConflictingChildren) > 0 {
			continue
		}
		if freshParent.Tx == nil {
			// Cannot verify without inputs — skip for safety.
			continue
		}

		legitConflict := false
		for _, pin := range freshParent.Tx.Inputs {
			gpHash := pin.PreviousTxIDChainHash()
			gpVout := pin.PreviousTxOutIndex
			gpMeta, gpErr := s.Get(ctx, gpHash, fields.Utxos)
			if gpErr != nil {
				// Cannot verify this input — be conservative and treat parent as legit.
				legitConflict = true
				break
			}
			if gpMeta == nil || int(gpVout) >= len(gpMeta.SpendingDatas) {
				continue
			}
			gpSD := gpMeta.SpendingDatas[gpVout]
			if gpSD == nil || gpSD.TxID == nil {
				continue
			}
			if !gpSD.TxID.IsEqual(&parentHash) {
				legitConflict = true
				break
			}
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
					report.Errors = append(report.Errors, cErr)
					continue
				}
			}
			report.CaseDCascaded++
			continue
		}

		if !dryRun {
			if _, _, sErr := s.SetConflicting(ctx, []chainhash.Hash{parentHash}, false); sErr != nil {
				report.Errors = append(report.Errors, sErr)
				continue
			}
		}
		report.CaseDFixed++
	}

	logProgress("[done] repair complete — fixed %d unmined_since, %d Case A, %d Case C, %d Case D (unmark), %d Case D (cascade)", report.UnminedSinceFixed, report.CaseAFixed, report.CaseCFixed, report.CaseDFixed, report.CaseDCascaded)
	return report, nil
}

// cascadeConflictingViaSpendingData walks SpendingData descendants of rootHash and marks
// each one Conflicting=true. Unlike MarkConflictingRecursively, this traversal reads
// SpendingData directly from each tx's Utxos field, so it is not blocked by GetSpend
// returning Status_CONFLICTING once a parent is flagged — which stops the standard
// cascade the moment the root is already conflicting.
func cascadeConflictingViaSpendingData(ctx context.Context, s Store, rootHash chainhash.Hash) error {
	visited := map[chainhash.Hash]struct{}{rootHash: {}}
	frontier := []chainhash.Hash{rootHash}

	for len(frontier) > 0 {
		var next []chainhash.Hash
		for _, h := range frontier {
			h := h
			meta, err := s.Get(ctx, &h, fields.Utxos)
			if err != nil {
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
				visited[childHash] = struct{}{}

				if _, _, sErr := s.SetConflicting(ctx, []chainhash.Hash{childHash}, true); sErr != nil {
					return sErr
				}
				next = append(next, childHash)
			}
		}
		frontier = next
	}

	return nil
}

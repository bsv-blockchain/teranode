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

		scannedBatches := 0
		for {
			batch, bErr := scanIt.Next(ctx)
			if bErr != nil {
				return report, bErr
			}
			if batch == nil {
				break
			}
			scannedBatches++
			if scannedBatches%100 == 0 {
				logProgress("[step 0/3] scanned %d batches, fixed %d unmined_since inconsistencies so far", scannedBatches, report.UnminedSinceFixed)
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
	}
	logProgress("[step 0/3] done — fixed %d unmined_since inconsistencies", report.UnminedSinceFixed)

	// Steps 1-3: conflict detection and repair.
	type caseCPair struct {
		loser  chainhash.Hash
		winner chainhash.Hash
	}

	var caseALosers []chainhash.Hash
	var caseCPairs []caseCPair
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
			logProgress("[step 1/3] scanned %d unmined txs, found %d Case A, %d Case C so far", unminedScanned, len(caseALosers), len(caseCPairs))
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

				// Fetch parent SpendingDatas + ConflictingChildren in one call.
				// ConflictingChildren is needed for Case C: siblings of txHash that are
				// recorded as (PARENT, sibling) in conflicting_children — not (txHash, sibling).
				parentMeta, pErr := s.Get(ctx, parentHash, fields.Utxos, fields.ConflictingChildren)
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

				break inputLoop
			}
		}
	}

	logProgress("[step 1/3] done — scanned %d unmined txs, found %d Case A, %d Case C", unminedScanned, len(caseALosers), len(caseCPairs))

	// Fix Case C first so SpendingData is corrected before the Case A sweep.
	// Dedup key is pair.loser (the fake winner tx being corrected): each unmined loser should be
	// processed at most once even if multiple inputs point to the same real winner.
	logProgress("[step 2/3] fixing %d Case C (inverted winner/loser) pairs...", len(caseCPairs))
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

	logProgress("[step 3/3] fixing %d Case A (unmarked losers)...", len(caseALosers))
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

	logProgress("[done] repair complete — fixed %d unmined_since, %d Case A, %d Case C", report.UnminedSinceFixed, report.CaseAFixed, report.CaseCFixed)
	return report, nil
}

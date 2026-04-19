package utxo

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// BlockHeaderInfo holds the minimal block header data needed by CleanupUnmined.
type BlockHeaderInfo struct {
	Hash   *chainhash.Hash
	Height uint32
}

// BlockchainQuerier is the subset of blockchain.ClientI needed by CleanupUnmined.
// Using a local interface with primitive types avoids an import cycle between stores/utxo and services/blockchain.
type BlockchainQuerier interface {
	GetBestBlockHeaderInfo(ctx context.Context) (BlockHeaderInfo, error)
	GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error)
}

// CleanupReport contains the results of a cleanup run.
type CleanupReport struct {
	// UnminedSinceFixed is the number of records whose unmined_since marker was
	// cleared in step 0 because the record is now confirmed on the best chain.
	UnminedSinceFixed int
	// ConflictingUnminedDeleted is the number of (Conflicting=true,
	// UnminedSince>0) records deleted from the store in step 1.
	ConflictingUnminedDeleted int
	// OrphanParentUnminedDeleted is the number of non-conflicting unmined
	// records deleted in step 3 because at least one of their parents is mined
	// on a block that is not on the best chain (an orphan fork).
	OrphanParentUnminedDeleted int
}

// CleanupProgressFunc is called by CleanupUnmined to report progress.
type CleanupProgressFunc func(format string, args ...interface{})

// CleanupOptions controls optional behavior of CleanupUnmined. The zero
// value is safe and runs every step.
type CleanupOptions struct {
	// SkipUnminedSinceScan skips step 0 — the fixup pass that clears
	// unmined_since on records already confirmed on the best chain. Step 0 is
	// the slowest part of a run on a large store (hundreds of millions of
	// records) and rarely finds anything once it has run cleanly. Skipping it
	// lets operators iterate on steps 1-3 cheaply. Only set this when you are
	// certain step 0 has run cleanly at least once since the last change to
	// the store.
	SkipUnminedSinceScan bool
}

// orphanParentBatchSize controls how many non-conflicting unmined transactions
// are BatchDecorate'd for parent-state classification at a time during step 3.
// Chosen to match the scan-iterator batch size so both steps consume similar
// memory.
const orphanParentBatchSize = 1024

// CleanupUnmined removes inconsistent unmined transaction state from the UTXO
// store that would otherwise trip BlockAssembly's validateParentChain.
//
// The unmined set is ephemeral by design — BSV propagation re-arrives valid
// txs within seconds and the next block sweeps them up — so the correct
// remedy for an inconsistent record is to delete it rather than to
// reverse-engineer proper state from a graph whose writers never fully clean
// up after themselves.
//
// Steps:
//
//  0. (optional, --skip-unmined-since-scan) For every record whose block_ids
//     intersect the best chain yet still carries UnminedSince>0, call
//     MarkTransactionsOnLongestChain(true) to clear the stray marker.
//  1. Collect every record with Conflicting=true AND UnminedSince>0 via a
//     single pass over ScanInconsistentUnminedTxs.
//  2. Delete the collected conflicting-unmined records.
//  3. Iterate GetUnminedTxIterator (non-conflicting unmined) and batch-fetch
//     parent metadata. Any child whose parent is mined on a block that is
//     not on the best chain, or whose parent is mined with no block_ids at
//     all (inconsistency), is deleted. Missing parents are left alone —
//     BA's validateParentChain tolerates those.
//
// BA's validateParentChain also tolerates missing parents, so non-conflicting
// children of records we deleted in step 2 remain harmlessly untouched —
// they'll be mined in the next block or pruned.
//
// Any DB error aborts the run — a partial cleanup leaves the store dirty,
// which is the condition we are trying to eliminate.
func CleanupUnmined(ctx context.Context, s Store, blockchainClient BlockchainQuerier, dryRun bool, progressFn CleanupProgressFunc, opts ...CleanupOptions) (CleanupReport, error) {
	var opt CleanupOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	var report CleanupReport

	logProgress := func(format string, args ...interface{}) {
		if progressFn != nil {
			progressFn(format, args...)
		}
	}

	// Heartbeat ticker — prints "still working: <phase>" every 15s so a silent
	// minute inside a single large batch is still visible in the operator log.
	var currentPhase atomic.Pointer[string]
	setPhase := func(p string) {
		currentPhase.Store(&p)
	}
	setPhase("initializing")
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-t.C:
				if p := currentPhase.Load(); p != nil {
					logProgress("[heartbeat] still working: %s", *p)
				}
			}
		}
	}()

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

	// ----- steps 0 + 1: combined scan -----
	setPhase("scanning store")
	if opt.SkipUnminedSinceScan {
		logProgress("[scan] starting — SkipUnminedSinceScan enabled, step 0 fixups skipped")
	} else {
		logProgress("[scan] starting — combined step 0 (unmined_since fixup) + step 1 (conflicting-unmined collection)")
	}

	scanIt, err := s.ScanInconsistentUnminedTxs()
	if err != nil {
		return report, err
	}

	var purgeSet []chainhash.Hash

	if scanIt != nil {
		defer scanIt.Close()

		lastReportedScan := int64(0)
		lastReportedTime := time.Now()
		const progressInterval = 30 * time.Second

		for {
			batch, bErr := scanIt.Next(ctx)
			if bErr != nil {
				return report, bErr
			}
			if batch == nil {
				break
			}

			scanned := scanIt.TotalScanned()
			if scanned-lastReportedScan >= 500_000 || time.Since(lastReportedTime) >= progressInterval {
				logProgress("[scan] %d records scanned — %d unmined_since fixes, %d queued for delete", scanned, report.UnminedSinceFixed, len(purgeSet))
				lastReportedScan = scanned
				lastReportedTime = time.Now()
			}

			var toMark []chainhash.Hash
			for _, rec := range batch {
				if rec.UnminedSince == 0 {
					continue
				}

				if !opt.SkipUnminedSinceScan {
					onMain := false
					for _, blockID := range rec.BlockIDs {
						if bestBlockIDsMap[blockID] {
							onMain = true
							break
						}
					}
					if onMain {
						// Step 0 fixup: mined tx still carrying unmined_since.
						toMark = append(toMark, rec.Hash)
						continue
					}
				}

				// Step 1 collect: conflicting unmined → queued for deletion.
				if rec.Conflicting {
					purgeSet = append(purgeSet, rec.Hash)
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

		logProgress("[scan] done — %d total records scanned, %d unmined_since fixes, %d queued for delete", scanIt.TotalScanned(), report.UnminedSinceFixed, len(purgeSet))
	} else {
		logProgress("[scan] store has no consistency-scan iterator — step 0/1 skipped")
	}

	// ----- step 2: delete conflicting-unmined -----
	if len(purgeSet) > 0 {
		setPhase("deleting conflicting-unmined records")
		logProgress("[delete] conflicting-unmined: %d records", len(purgeSet))

		const deleteBatchSize = 1000
		for i := 0; i < len(purgeSet); i += deleteBatchSize {
			end := i + deleteBatchSize
			if end > len(purgeSet) {
				end = len(purgeSet)
			}

			for j := i; j < end; j++ {
				h := purgeSet[j]
				if !dryRun {
					if dErr := s.Delete(ctx, &h); dErr != nil {
						return report, dErr
					}
				}
				report.ConflictingUnminedDeleted++
			}

			logProgress("[delete] conflicting-unmined: %d/%d", end, len(purgeSet))
		}
	}

	// ----- step 3: delete non-conflicting unmined children of orphan-mined parents -----
	setPhase("scanning unmined for orphan-mined parents")
	logProgress("[orphan-scan] iterating non-conflicting unmined transactions to find orphan-mined parents")

	unminedIt, err := s.GetUnminedTxIterator()
	if err != nil {
		return report, err
	}
	defer unminedIt.Close()

	parentsCache := map[chainhash.Hash]*parentClass{}

	var orphanDeleteSet []chainhash.Hash
	orphanScanned := 0
	lastOrphanScan := 0
	lastOrphanTime := time.Now()
	const orphanProgressInterval = 30 * time.Second

	for {
		batch, bErr := unminedIt.Next(ctx)
		if bErr != nil {
			return report, bErr
		}
		if batch == nil {
			break
		}

		// Collect unique parent hashes we haven't classified yet.
		toResolve := []chainhash.Hash{}
		for _, tx := range batch {
			if tx.Node == nil || tx.Skip || tx.TxInpoints == nil {
				continue
			}
			for _, ph := range tx.TxInpoints.GetParentTxHashes() {
				if _, ok := parentsCache[ph]; ok {
					continue
				}
				// placeholder so we don't queue the same hash twice in this batch
				parentsCache[ph] = nil
				toResolve = append(toResolve, ph)
			}
		}

		if len(toResolve) > 0 {
			unresolved := make([]*UnresolvedMetaData, len(toResolve))
			for i, h := range toResolve {
				unresolved[i] = &UnresolvedMetaData{Hash: h, Idx: i}
			}
			if err := s.BatchDecorate(ctx, unresolved, fields.BlockIDs, fields.UnminedSince, fields.Conflicting); err != nil {
				return report, err
			}
			for _, u := range unresolved {
				parentsCache[u.Hash] = classifyParent(u, bestBlockIDsMap)
			}
		}

		for _, tx := range batch {
			if tx.Node == nil || tx.Skip || tx.TxInpoints == nil {
				continue
			}
			orphanScanned++
			for _, ph := range tx.TxInpoints.GetParentTxHashes() {
				pc := parentsCache[ph]
				if pc != nil && pc.deleteChild {
					orphanDeleteSet = append(orphanDeleteSet, tx.Node.Hash)
					break
				}
			}
		}

		if orphanScanned-lastOrphanScan >= 10_000 || time.Since(lastOrphanTime) >= orphanProgressInterval {
			logProgress("[orphan-scan] %d unmined scanned, %d queued for delete", orphanScanned, len(orphanDeleteSet))
			lastOrphanScan = orphanScanned
			lastOrphanTime = time.Now()
		}
	}

	logProgress("[orphan-scan] done — %d unmined scanned, %d queued for delete", orphanScanned, len(orphanDeleteSet))

	if len(orphanDeleteSet) > 0 {
		setPhase("deleting orphan-parent children")
		logProgress("[delete] orphan-parent children: %d records", len(orphanDeleteSet))

		const deleteBatchSize = 1000
		for i := 0; i < len(orphanDeleteSet); i += deleteBatchSize {
			end := i + deleteBatchSize
			if end > len(orphanDeleteSet) {
				end = len(orphanDeleteSet)
			}

			for j := i; j < end; j++ {
				h := orphanDeleteSet[j]
				if !dryRun {
					if dErr := s.Delete(ctx, &h); dErr != nil {
						return report, dErr
					}
				}
				report.OrphanParentUnminedDeleted++
			}

			logProgress("[delete] orphan-parent children: %d/%d", end, len(orphanDeleteSet))
		}
	}

	setPhase("done")
	logProgress("[done] cleanup complete — %d unmined_since fixed, %d conflicting-unmined deleted, %d orphan-parent children deleted",
		report.UnminedSinceFixed, report.ConflictingUnminedDeleted, report.OrphanParentUnminedDeleted)

	return report, nil
}

// parentClass is the minimal per-parent classification needed to decide
// whether each of its unmined children should be deleted.
type parentClass struct {
	// deleteChild is true when the parent's state is incompatible with BA's
	// validateParentChain for a non-conflicting unmined child that spends it —
	// so deleting the child is the surgical fix.
	deleteChild bool
}

// classifyParent determines what to do with a parent observed through the
// unmined iterator. The rules mirror validateParentChain:
//
//   - missing parent (Err / nil Data)  → tolerate (BA skips), no child delete
//   - Conflicting=true                  → child is dangling; delete child
//   - UnminedSince>0                    → parent is unmined; BA will find it
//     in the iterator (non-conflicting),
//     no child delete
//   - UnminedSince=0 and BlockIDs empty → inconsistency; delete child
//   - UnminedSince=0 and BlockIDs set   → on best chain → ok
//     all off best chain → delete child
func classifyParent(u *UnresolvedMetaData, bestBlockIDs map[uint32]bool) *parentClass {
	if u == nil || u.Err != nil || u.Data == nil {
		return &parentClass{deleteChild: false}
	}

	m := u.Data

	if m.Conflicting {
		return &parentClass{deleteChild: true}
	}

	if m.UnminedSince > 0 {
		return &parentClass{deleteChild: false}
	}

	if len(m.BlockIDs) == 0 {
		return &parentClass{deleteChild: true}
	}

	for _, bid := range m.BlockIDs {
		if bestBlockIDs[bid] {
			return &parentClass{deleteChild: false}
		}
	}

	return &parentClass{deleteChild: true}
}

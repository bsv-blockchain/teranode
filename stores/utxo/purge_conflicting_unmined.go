package utxo

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// BlockHeaderInfo holds the minimal block header data needed by PurgeConflictingUnmined.
type BlockHeaderInfo struct {
	Hash   *chainhash.Hash
	Height uint32
}

// BlockchainQuerier is the subset of blockchain.ClientI needed by PurgeConflictingUnmined.
// Using a local interface with primitive types avoids an import cycle between stores/utxo and services/blockchain.
type BlockchainQuerier interface {
	GetBestBlockHeaderInfo(ctx context.Context) (BlockHeaderInfo, error)
	GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error)
}

// PurgeReport contains the results of a purge run.
type PurgeReport struct {
	// UnminedSinceFixed is the number of records whose unmined_since marker was
	// cleared in step 0 because the record is now confirmed on the best chain.
	UnminedSinceFixed int
	// ConflictingUnminedPurged is the number of (Conflicting=true, UnminedSince>0)
	// records deleted from the store in step 1.
	ConflictingUnminedPurged int
}

// PurgeProgressFunc is called by PurgeConflictingUnmined to report progress.
type PurgeProgressFunc func(format string, args ...interface{})

// PurgeOptions controls optional behavior of PurgeConflictingUnmined. The zero
// value is safe and runs every step.
type PurgeOptions struct {
	// SkipUnminedSinceScan skips step 0 — the fixup pass that clears
	// unmined_since on records already confirmed on the best chain. Step 0 is
	// the slowest part of a purge on a large store (hundreds of millions of
	// records) and rarely finds anything once it has run cleanly. Skipping it
	// lets operators iterate on step 1 cheaply. Only set this when you are
	// certain step 0 has run cleanly at least once since the last change to
	// the store.
	SkipUnminedSinceScan bool
}

// PurgeConflictingUnmined performs a surgical purge of every record in the UTXO
// store whose Conflicting=true flag and UnminedSince>0 marker are both set.
//
// The unmined set is ephemeral by design — BSV propagation re-arrives valid txs
// within seconds and the next block sweeps them up — so the correct remedy for
// an inconsistent conflicting-unmined record is to delete it rather than try to
// reverse-engineer its proper state from a graph whose writers never fully
// clean up after themselves.
//
// The function makes a single pass over ScanInconsistentUnminedTxs and, in one
// combined step:
//   - Step 0 (optional): clears unmined_since on records whose block_ids
//     intersect the best chain (mined txs that still carry the marker).
//   - Step 1: collects (Conflicting=true, UnminedSince>0) records for deletion.
//
// Step 2 then deletes the collected records via Store.Delete. BA's
// validateParentChain tolerates missing parents, so non-conflicting children
// whose parents were purged remain untouched — they'll be mined in the next
// block or pruned.
//
// Any DB error aborts the run — a partial purge leaves the store dirty, which
// is the condition we are trying to eliminate.
func PurgeConflictingUnmined(ctx context.Context, s Store, blockchainClient BlockchainQuerier, dryRun bool, progressFn PurgeProgressFunc, opts ...PurgeOptions) (PurgeReport, error) {
	var opt PurgeOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	var report PurgeReport

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

	// Single pass: one scan, classify each record as step-0 fixup, step-1 purge
	// candidate, or skip.
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
				logProgress("[scan] %d records scanned — %d unmined_since fixes, %d queued for purge", scanned, report.UnminedSinceFixed, len(purgeSet))
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

		logProgress("[scan] done — %d total records scanned, %d unmined_since fixes, %d queued for purge", scanIt.TotalScanned(), report.UnminedSinceFixed, len(purgeSet))
	} else {
		logProgress("[scan] store has no consistency-scan iterator — nothing to purge")
	}

	// Step 2: delete the collected records.
	if len(purgeSet) > 0 {
		setPhase("purging conflicting-unmined records")
		logProgress("[purge] deleting %d conflicting-unmined records", len(purgeSet))

		const batchSize = 1000
		for i := 0; i < len(purgeSet); i += batchSize {
			end := i + batchSize
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
				report.ConflictingUnminedPurged++
			}

			logProgress("[purge] deleted %d/%d", end, len(purgeSet))
		}
	}

	setPhase("done")
	logProgress("[done] purge complete — %d unmined_since fixed, %d conflicting-unmined purged", report.UnminedSinceFixed, report.ConflictingUnminedPurged)

	return report, nil
}

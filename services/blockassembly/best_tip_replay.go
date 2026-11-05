package blockassembly

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/retry"
)

// BestTipPositionStateKey is the key under which block assembly persists its replay position, the
// place in the blockchain store's best-tip order of the last block it handled (a serialised
// model.BestTipKey). It is kept apart from StateKey so an older binary, which decodes that blob
// strictly, can still start after a rollback. It replaces BlockAssemblerBestTipSeq, an 8-byte
// sequence number an unreleased build wrote, which is never read.
const BestTipPositionStateKey = "BlockAssemblerBestTipPos"

// SkippedForkMarkStateKey is the key under which block assembly records, as one byte, whether a
// mark of skipped fork transactions (markSkippedForkTxs) is unfinished, so a restart finishes it.
const SkippedForkMarkStateKey = "BlockAssemblerSkippedForkMark"

const (
	// bestTipPageSize is how many blocks one BlocksBetterThan call returns.
	bestTipPageSize = 1000

	// bestTipPositionSaveInterval is the least time between two saves of the replay position
	// after a pass. A restart replays at most this much of blocks already handled, each a no-op.
	bestTipPositionSaveInterval = 10 * time.Second

	// bestTipPositionShutdownSaveTimeout bounds the save of the replay position on shutdown.
	bestTipPositionShutdownSaveTimeout = 5 * time.Second

	// maxBestTipReplayFailures is how many passes in a row the replay may fail before block
	// assembly follows the best header once instead, so a block it cannot apply does not hold
	// it off the tip for ever.
	maxBestTipReplayFailures = 3

	// bestTipProbeInterval is how often block assembly asks a blockchain service that did not
	// serve BlocksBetterThan whether it does now, as it will once it is upgraded.
	bestTipProbeInterval = time.Minute

	// skippedForkMarkRetryBase is how long an unfinished mark of skipped fork transactions waits
	// before it runs again after its first failure; each failure after it doubles the wait, up
	// to skippedForkMarkRetryMax.
	skippedForkMarkRetryBase = 5 * time.Second
	skippedForkMarkRetryMax  = 5 * time.Minute

	// maxRestampPresizePerBlock bounds how many hashes per block restampStaleBranch allocates
	// up front from the blocks' transaction counts; blockTxHashes grows it past that once.
	maxRestampPresizePerBlock = 10_000_000
)

// errBestTipReset reports that applying a block from the replay ran a full reset, which leaves
// block assembly on the chain's current best tip rather than on that block.
var errBestTipReset = errors.NewBlockAssemblyResetError("reset while replaying a change of the best tip")

// bestTipProbeKey sorts above every block: chain work is 32 bytes, and none reaches all ones. A
// read better than it is empty and touches no index entry, so it costs a probe nothing.
var bestTipProbeKey = model.BestTipKey{ChainWork: bytes.Repeat([]byte{0xff}, 32)}

// isStateNotFound reports whether a GetState error means the key is not set.
func isStateNotFound(err error) bool {
	return errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), sql.ErrNoRows.Error())
}

// loadBestTipPosition reads the replay position at start-up. A persisted position is used as it
// is: the best tips made while block assembly was down sort better than it. Without one, block
// assembly starts at the best tip when it is on it, since there is nothing to replay, and at its
// own tip otherwise, where the height floor in replayBestTipChanges limits the replay to recent
// blocks. If the blockchain service does not serve BlocksBetterThan the position stays unknown,
// and a wake-up asks again after bestTipProbeInterval.
func (b *BlockAssembler) loadBestTipPosition(ctx context.Context) {
	if _, _, _, err := b.blockchainClient.BlocksBetterThan(ctx, bestTipProbeKey, nil, 0, 1); err != nil {
		b.lastBestTipProbe.Store(time.Now().UnixNano())

		if b.bestTipUnsupportedLogged.CompareAndSwap(false, true) {
			b.logger.Warnf("[BlockAssembler] blockchain service does not serve the best-tip order, following the best header and asking again every %s: %v", bestTipProbeInterval, err)
		} else {
			b.logger.Debugf("[BlockAssembler] blockchain service still does not serve the best-tip order: %v", err)
		}

		return
	}

	var pos model.BestTipKey

	data, err := b.blockchainClient.GetState(ctx, BestTipPositionStateKey)

	switch {
	case err == nil:
		if pos, err = model.NewBestTipKeyFromBytes(data); err != nil {
			b.logger.Warnf("[BlockAssembler] ignoring persisted best tip position: %v", err)

			if pos, err = b.startingBestTipPosition(ctx); err != nil {
				b.logger.Warnf("[BlockAssembler] error choosing a best tip position, following the best header until it can be chosen: %v", err)
				return
			}
		}
	case isStateNotFound(err):
		if pos, err = b.startingBestTipPosition(ctx); err != nil {
			b.logger.Warnf("[BlockAssembler] error choosing a best tip position, following the best header until it can be chosen: %v", err)
			return
		}
	default:
		b.logger.Warnf("[BlockAssembler] error reading best tip position, following the best header until it can be read: %v", err)
		return
	}

	b.setBestTipPos(pos)
	b.bestTipPosSaved.Store(&pos)
	b.bestTipPosKnown.Store(true)

	b.logger.Infof("[BlockAssembler] best tip replay position %s (id %d)", pos.Hash, pos.ID)
}

// followBestTipChanges runs one replay pass and reports whether the caller should still move to
// the best header. It does when the blockchain service does not serve the best-tip order, when
// the replay has failed maxBestTipReplayFailures passes in a row, and when the pass found nothing
// although block assembly is not on the best tip, which no move of the tip should ever leave.
func (b *BlockAssembler) followBestTipChanges(ctx context.Context) bool {
	if !b.bestTipPosKnown.Load() {
		if time.Since(time.Unix(0, b.lastBestTipProbe.Load())) < bestTipProbeInterval {
			return true
		}

		b.loadBestTipPosition(ctx)

		if !b.bestTipPosKnown.Load() {
			return true
		}
	}

	found, err := b.replayBestTipChanges(ctx)
	if err != nil {
		failures := b.bestTipReplayFailures.Add(1)
		prometheusBlockAssemblyProcessingStuck.WithLabelValues("best_tip_replay").Inc()

		if failures < maxBestTipReplayFailures {
			b.logger.Errorf("[BlockAssembler] replay of best tip changes failed (%d in a row), will retry: %v", failures, err)
			return false
		}

		// The history given up may hold blocks that were the best tip only briefly, also when
		// block assembly is on the best tip already, for example after a reset, which does not
		// pass them. Giving it up persists the replay position past them, so the mark they owe
		// is recorded first; without the record the history is kept, and the next pass, a
		// failure again, tries once more.
		if saveErr := b.saveSkippedForkMark(ctx, true); saveErr != nil {
			prometheusBlockAssemblyProcessingStuck.WithLabelValues("skipped_fork_mark_record").Inc()
			b.logger.Errorf("[BlockAssembler] replay of best tip changes failed %d times in a row, keeping its history: the mark of skipped fork transactions giving it up owes cannot be recorded: %v (replay: %v)", failures, saveErr, err)

			return false
		}

		b.skippedForkMarkPending = true
		b.bestTipReplayFailures.Store(0)

		// The best-header path moves straight to the tip, so the blocks before it are
		// given up. A block that sorts better after this read is replayed on the next pass.
		//
		// That includes the history a reset left pending. Holding on would keep every
		// candidate empty for as long as the failure lasts. When the reset finished, it
		// rebuilt the pool on its target, so giving the history up leaves the pool as it
		// was before the replay existed. When it rotated the tx map and then failed
		// (bestTipResetIncomplete), the pool may be partial: the flag stays set, so every
		// later pass retries the reset's reload, and candidates are built from what the
		// pool holds until one succeeds.
		if b.bestTipResetReplayPending.Load() {
			if b.bestTipResetIncomplete.Load() {
				b.logger.Errorf("[BlockAssembler] replay failed %d times in a row, giving up the history a reset left pending so mining candidates are built again from a pool the reset may have left partial, retrying its reload on every pass: %v", failures, err)
			} else {
				b.logger.Errorf("[BlockAssembler] replay failed %d times in a row, giving up the history a reset left pending so mining candidates are built again: %v", failures, err)
			}

			prometheusBlockAssemblyProcessingStuck.WithLabelValues("best_tip_reset_replay_abandoned").Inc()
			b.bestTipResetReplayPending.Store(false)
		}

		b.logger.Errorf("[BlockAssembler] replay of best tip changes failed %d times in a row, following the best header instead: %v", failures, err)

		b.giveUpBestTipHistory(ctx)

		return true
	}

	b.bestTipReplayFailures.Store(0)

	if found > 0 {
		return false
	}

	best, _, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		b.logger.Errorf("[BlockAssembler] error getting best block header after replay: %v", err)
		return false
	}

	tip, _ := b.CurrentBlock()
	if tip != nil && best.Hash().IsEqual(tip.Hash()) {
		return false
	}

	// A block stored after the read above is replayed by its own notification. Look once more
	// before giving up on the replay, so that case does not take the best-header path.
	if found, err = b.replayBestTipChanges(ctx); err != nil || found > 0 {
		return false
	}

	b.logger.Warnf("[BlockAssembler] no block better than the replay position leads to the best block %s, following the best header", best.Hash())

	return true
}

// markSkippedForkTxs runs after block assembly followed the best header instead of replaying
// the changes of the best tip, or reached it by a reset the replay does not follow. That skips a
// block that was the best tip only briefly: block validation marked its transactions mined, but
// block assembly never moved onto it and back, so reorgBlocks never marked them again; following
// the best header may even reach the best tip by a plain catch-up. Those transactions would stay
// marked mined while off the longest chain, and a restart, which reloads only unmined
// transactions, would drop them.
//
// It marks the transactions of recent fork blocks that no block of the best chain contains
// (branchOnlyTxs) and that are marked mined as not on the longest chain, as the replay's restamp
// does (markOffLongestChain). Which fork block was the best tip does not matter: one still
// stamped unmined is left as it is. When one it marked is not in the pool and is not conflicting,
// which the pool never loads, the pool needs a reload (skippedForkReloadPending), which it runs
// unless beforeReload. beforeReload says a load of the unmined transactions follows anyway, at
// start-up or in the reset that comes next, and the need stays recorded until that load is done;
// it also lets the run start off the best tip, since that load is on the best tip. Otherwise the
// run waits for block assembly to be on the best tip, where its own blocks are not fork blocks:
// it returns errSkippedForkMarkOffTip and stays pending, without a back-off.
//
// The fork blocks are the valid blocks at or above the floor that the best chain does not hold
// (skippedForkBlocks, skippedForkMarkFloor). Of them, the ones with subtrees are read: block
// validation sets subtrees_set before it marks a block's transactions mined. One whose mined_set
// is not set yet is left for a later run, so a setTxMined still writing it cannot clear
// unmined_since again after the mark. A block is recorded as soon as its transactions are marked,
// and is not read again; the pool reload it may need is kept apart (skippedForkReloadPending).
//
// It returns an error when the run did not finish, for example on a fork block it cannot read.
// The run then stays pending, in memory and under SkippedForkMarkStateKey: every later pass runs
// it again, the idle reconcile wakes one after a back-off (noteSkippedForkMarkFailure), and the
// next start finishes it before it loads the unmined transactions, or fails. A fork block whose
// subtree the subtree store does not have is given up, counted and logged, not retried: the file
// can be gone lawfully, for example after the retention was raised, and nothing else could read
// it.
func (b *BlockAssembler) markSkippedForkTxs(ctx context.Context, beforeReload bool) (err error) {
	best, bestMeta, bestErr := b.blockchainClient.GetBestBlockHeader(ctx)

	if bestErr == nil && !beforeReload {
		if tip, _ := b.CurrentBlock(); tip == nil || !tip.Hash().IsEqual(best.Hash()) {
			b.logger.Debugf("[BlockAssembler] block assembly is not on the best tip %s, marking skipped fork transactions once it is", best.Hash())
			return errSkippedForkMarkOffTip
		}
	}

	b.skippedForkMarkPending = true
	b.skippedForkMarkRan = true

	defer func() {
		if err == nil {
			b.skippedForkMarkPending = false
		}

		// A record left saying pending would only repeat a finished run at the next start, but
		// the run stays pending until the record is right.
		if saveErr := b.saveSkippedForkMark(ctx, b.skippedForkMarkPending); saveErr != nil && err == nil {
			b.skippedForkMarkPending = true
			err = saveErr
		}

		if err == nil {
			b.noteSkippedForkMarkSuccess()
			return
		}

		prometheusBlockAssemblyProcessingStuck.WithLabelValues("skipped_fork_mark_unfinished").Inc()
		b.noteSkippedForkMarkFailure(time.Now())
		b.logger.Errorf("[BlockAssembler] mark of skipped fork transactions unfinished, retrying at %s: %v", b.skippedForkMarkRetryAt.Format(time.RFC3339), err)
	}()

	if bestErr != nil {
		return errors.NewProcessingError("[markSkippedForkTxs] error getting the best block header", bestErr)
	}

	if err = b.recheckSkippedForkMarks(ctx); err != nil {
		return err
	}

	floor := b.skippedForkMarkFloor(bestMeta.Height)

	for hash, height := range b.skippedForkBlocksMarked {
		if height < floor {
			delete(b.skippedForkBlocksMarked, hash)
		}
	}

	forkBlocks, err := b.skippedForkBlocks(ctx, best, bestMeta.Height, floor)
	if err != nil {
		return errors.NewProcessingError("[markSkippedForkTxs] error listing the fork blocks", err)
	}

	if len(forkBlocks) == 0 {
		return b.reloadAfterSkippedForkMark(ctx, beforeReload)
	}

	if b.skippedForkBlocksMarked == nil {
		b.skippedForkBlocksMarked = make(map[chainhash.Hash]uint32)
	}

	txMap := b.subtreeProcessor.GetCurrentTxMap()
	unread, unprocessed := 0, 0

	// Block by block, so a block that cannot be read does not hold up the others; it is not
	// recorded, and the next run reads it again.
	for _, fb := range forkBlocks {
		b.heartbeat.BeatIfStarted()

		hash := *fb.header.Hash()

		// The header read can be served from a cache SetBlockMinedSet does not clear, so a block
		// it reads as unfinished is asked again, as block validation does.
		if !fb.meta.MinedSet {
			mined, err := b.blockchainClient.GetBlockIsMined(ctx, &hash)
			if err != nil {
				return errors.NewProcessingError("[markSkippedForkTxs] error checking whether fork block %s is processed", hash, err)
			}

			if !mined {
				b.logger.Infof("[BlockAssembler] fork block %s is not processed by block validation yet, marking its skipped transactions on a later run", hash)
				unprocessed++

				continue
			}
		}

		hashes, err := b.blockTxHashes(ctx, fb.header, nil)
		if err != nil {
			// A subtree file the store does not have is not read by a later run either. It can
			// be gone lawfully: a raised retention, block assembly's own subtrees stored with
			// the global retention, or a reused subtree that kept an earlier delete height.
			// The block's transactions are given up rather than block assembly stopped. The
			// subtree read reports any failure of its fallback read as not found too, so the
			// store is asked directly; a failure to answer is retried like any other.
			gone := false
			if errors.Is(err, errors.ErrNotFound) {
				var existsErr error
				if gone, existsErr = b.subtreeFilesGone(ctx, fb.header); existsErr != nil {
					b.logger.Errorf("[BlockAssembler] error checking the subtree files of fork block %s: %v", hash, existsErr)
				}
			}

			if gone {
				prometheusBlockAssemblyProcessingStuck.WithLabelValues("skipped_fork_subtree_lost").Inc()
				b.logger.Errorf("[BlockAssembler] fork block %s has no subtree in the subtree store, giving up the mark of its skipped transactions: %v", hash, err)
				b.skippedForkBlocksMarked[hash] = fb.meta.Height

				continue
			}

			b.logger.Errorf("[BlockAssembler] error reading fork block %s to mark skipped fork transactions: %v", hash, err)
			unread++

			continue
		}

		branchOnly, _, err := b.branchOnlyMeta(ctx, hashes, map[uint32]struct{}{fb.meta.ID: {}}, best, fields.UnminedSince, fields.Conflicting)
		if err != nil {
			return errors.NewProcessingError("[markSkippedForkTxs] error finding the transactions of fork block %s off the best chain", hash, err)
		}

		toMark := make([]chainhash.Hash, 0, len(branchOnly))
		poolable := make(map[chainhash.Hash]struct{})

		for _, u := range branchOnly {
			// Still stamped unmined: no block marked it mined on the longest chain, as for one
			// only a fork block that never was the best tip has.
			if u.Data == nil || u.Data.UnminedSince != 0 {
				continue
			}

			toMark = append(toMark, u.Hash)

			if !u.Data.Conflicting && !txMap.Exists(u.Hash) {
				poolable[u.Hash] = struct{}{}
			}
		}

		if len(toMark) > 0 {
			// Set before the write: once it is written, a later run finds those transactions
			// unmined already and would not know the pool still lacks them.
			reloadOwed := b.skippedForkReloadPending
			if len(poolable) > 0 {
				b.skippedForkReloadPending = true
			}

			b.heartbeat.BeatIfStarted()

			onBest, err := b.markOffLongestChain(ctx, toMark)
			if err != nil {
				return errors.NewProcessingError("[markSkippedForkTxs] error marking %d transactions of fork block %s as not on the longest chain", len(toMark), hash, err)
			}

			b.heartbeat.BeatIfStarted()

			// Those the best chain holds by now are marked mined again and stay out of the pool,
			// and a reload would stamp the pool's copies of them unmined.
			for _, txHash := range onBest {
				delete(poolable, txHash)
			}

			b.skippedForkReloadPending = reloadOwed || len(poolable) > 0

			b.logger.Warnf("[BlockAssembler] followed the best header without the replay: marked %d transactions of fork block %s as not on the longest chain", len(toMark)-len(onBest), hash)

			// The block may have joined the best chain during the run and could leave it again
			// without block assembly moving onto it, so it is read again on a later run.
			if len(onBest) > 0 {
				continue
			}
		}

		b.skippedForkBlocksMarked[hash] = fb.meta.Height
	}

	// A reload is a reset, which waits for every block block validation has not finished; with
	// such a fork block in the run it would hold block assembly until that block is done. The
	// reload waits for the run that finds none: until block validation sets mined_set on each
	// such fork block above the floor, or the floor passes it. Until then the transactions the
	// run marked are unmined and not in the pool, so this node does not mine them; a restart
	// loads them.
	if unprocessed == 0 {
		if err = b.reloadAfterSkippedForkMark(ctx, beforeReload); err != nil {
			return err
		}
	}

	if unread > 0 {
		return errors.NewProcessingError("[markSkippedForkTxs] %d of %d fork blocks could not be read", unread, len(forkBlocks))
	}

	if unprocessed > 0 {
		return &skippedForkUnprocessedError{count: unprocessed}
	}

	return nil
}

// errSkippedForkMarkOffTip is markSkippedForkTxs waiting for block assembly to reach the best tip.
var errSkippedForkMarkOffTip = errors.NewProcessingError("[markSkippedForkTxs] block assembly is not on the best tip")

// skippedForkUnprocessedError reports a run of markSkippedForkTxs that read every fork block it
// could except some block validation has not finished.
type skippedForkUnprocessedError struct {
	count int
}

func (e *skippedForkUnprocessedError) Error() string {
	return fmt.Sprintf("[markSkippedForkTxs] %d fork blocks are not processed by block validation yet", e.count)
}

// skippedForkWorkOwed reports whether a mark of skipped fork transactions is unfinished, the pool
// reload one needs has not run, or transactions it wrote off the longest chain still need their
// check against the best chain (skippedForkRecheck). Every pass and wake that finishes that work reads this, so
// neither can be left owed with nothing to retry it.
func (b *BlockAssembler) skippedForkWorkOwed() bool {
	return b.skippedForkMarkPending || b.skippedForkReloadPending || len(b.skippedForkRecheck) > 0
}

// markOffLongestChain marks the transactions as not on the longest chain, then marks again as on
// it each one a block of the best chain contains by then, which it returns. The write is decided
// on a read of the best chain: a block that became the best tip since, and whose setTxMined
// cleared unmined_since before the write, would otherwise be left with it set while it is mined
// on the best chain, and moving forward onto that block does not mark it again.
//
// A failure leaves the hashes in skippedForkRecheck, so the next run of markSkippedForkTxs
// checks them again: the block that holds one may be on the best chain now, and no fork block it
// reads would show it. That includes a failed write, which the stores do in chunks and may have
// done in part. A restart's load of the unmined transactions marks such a transaction mined as
// well.
func (b *BlockAssembler) markOffLongestChain(ctx context.Context, hashes []chainhash.Hash) ([]chainhash.Hash, error) {
	if err := b.utxoStore.MarkTransactionsOnLongestChain(ctx, hashes, false); err != nil {
		b.addSkippedForkRecheck(hashes)
		return nil, err
	}

	onBest, err := b.remarkOnBestChain(ctx, hashes)
	if err != nil {
		b.addSkippedForkRecheck(hashes)
		return nil, err
	}

	return onBest, nil
}

// remarkOnBestChain marks as on the longest chain each of the transactions a block of the best
// chain, read now, contains, and returns them.
func (b *BlockAssembler) remarkOnBestChain(ctx context.Context, hashes []chainhash.Hash) ([]chainhash.Hash, error) {
	best, _, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, errors.NewProcessingError("[remarkOnBestChain] error getting the best block header", err)
	}

	_, onBest, err := b.branchOnlyMeta(ctx, hashes, nil, best)
	if err != nil {
		return nil, err
	}

	if len(onBest) > 0 {
		if err = b.utxoStore.MarkTransactionsOnLongestChain(ctx, onBest, true); err != nil {
			return nil, errors.NewProcessingError("[remarkOnBestChain] error marking %d transactions of the best chain as mined", len(onBest), err)
		}
	}

	return onBest, nil
}

// recheckSkippedForkMarks finishes the check against the best chain of transactions an earlier
// mark wrote off it without finishing (skippedForkRecheck).
func (b *BlockAssembler) recheckSkippedForkMarks(ctx context.Context) error {
	if len(b.skippedForkRecheck) == 0 {
		return nil
	}

	hashes := make([]chainhash.Hash, 0, len(b.skippedForkRecheck))
	for hash := range b.skippedForkRecheck {
		hashes = append(hashes, hash)
	}

	if _, err := b.remarkOnBestChain(ctx, hashes); err != nil {
		return errors.NewProcessingError("[recheckSkippedForkMarks] error checking %d transactions written off the longest chain", len(hashes), err)
	}

	b.skippedForkRecheck = nil

	return nil
}

// addSkippedForkRecheck adds transactions to skippedForkRecheck, each once however often a
// write of it fails.
func (b *BlockAssembler) addSkippedForkRecheck(hashes []chainhash.Hash) {
	if b.skippedForkRecheck == nil {
		b.skippedForkRecheck = make(map[chainhash.Hash]struct{}, len(hashes))
	}

	for _, hash := range hashes {
		b.skippedForkRecheck[hash] = struct{}{}
	}
}

// skippedForkMarkFloor is the lowest height of a fork block markSkippedForkTxs reads: the replay
// floor, raised to where subtree files are surely still kept. A subtree file is kept until the
// UTXO store's height when it was written, normally one below its block, plus the subtree
// retention. A lower fork block may have lost it, and was the best tip, if ever, a retention ago.
func (b *BlockAssembler) skippedForkMarkFloor(bestHeight uint32) uint32 {
	floor := b.bestTipFloor(bestHeight)

	if retention := b.settings.GetSubtreeValidationBlockHeightRetention(); retention > 0 && bestHeight+2 > retention {
		floor = max(floor, bestHeight+2-retention)
	}

	return floor
}

// skippedForkBlocks returns the blocks markSkippedForkTxs has to read: the valid blocks at or
// above floor that the best chain does not hold, that block validation has set subtrees for, and
// that are not recorded as marked already, with their meta. GetBlockHeadersFromHeight returns
// every stored block in a window of heights, of every chain, so a page holds whole heights and
// a height never needs more than one read. The windows go on past the best tip, which a fork of
// less work can reach, until one is empty; no chain leaves a gap in its heights. The best chain is
// read from the best header down. Neither read computes a branch length, which on the SQL store
// costs up to a 1000-block walk per stale leaf (GetChainTips).
func (b *BlockAssembler) skippedForkBlocks(ctx context.Context, best *model.BlockHeader, bestHeight, floor uint32) ([]blockHeaderWithMeta, error) {
	pageSize := uint32(bestTipPageSize)
	if limit := b.settings.BlockChain.MaxBlockHeadersPerRequest; limit > 0 && uint64(limit) < uint64(pageSize) {
		pageSize = uint32(limit) //nolint:gosec // limit is positive and below bestTipPageSize
	}

	onBest := make(map[chainhash.Hash]struct{}, bestHeight-floor+1)

	for next, height := best.Hash(), bestHeight; height >= floor; {
		b.heartbeat.BeatIfStarted()

		headers, metas, err := b.blockchainClient.GetBlockHeaders(ctx, next, uint64(min(pageSize, height-floor+1)))
		if err != nil {
			return nil, err
		}

		if len(headers) == 0 || len(metas) != len(headers) {
			return nil, errors.NewProcessingError("[skippedForkBlocks] read %d headers with %d metas from %s", len(headers), len(metas), next)
		}

		for _, header := range headers {
			onBest[*header.Hash()] = struct{}{}
		}

		last := metas[len(metas)-1]
		if last.Height == 0 {
			break
		}

		next, height = headers[len(headers)-1].HashPrevBlock, last.Height-1
	}

	var forks []blockHeaderWithMeta

	for start := floor; ; start += pageSize {
		b.heartbeat.BeatIfStarted()

		// The window's metadata carries no flags; GetBlockHeader below reads them.
		headers, _, err := b.blockchainClient.GetBlockHeadersFromHeight(ctx, start, pageSize)
		if err != nil {
			return nil, err
		}

		if len(headers) == 0 && start > bestHeight {
			return forks, nil
		}

		for _, header := range headers {
			hash := *header.Hash()

			if _, main := onBest[hash]; main {
				continue
			}

			if _, done := b.skippedForkBlocksMarked[hash]; done {
				continue
			}

			_, meta, err := b.blockchainClient.GetBlockHeader(ctx, &hash)
			if err != nil {
				return nil, err
			}

			// A block without subtrees has not been through block validation, which marked
			// nothing mined in it; it is looked at again on a later run. An invalid block's
			// transactions are block validation's to unmark.
			if meta.Invalid || !meta.SubtreesSet {
				continue
			}

			forks = append(forks, blockHeaderWithMeta{header: header, meta: meta})
		}
	}
}

// reloadAfterSkippedForkMark reloads the pool on block assembly's tip, as the replay's restamp
// does, once markSkippedForkTxs marked a transaction the pool could hold and does not, such as
// one only a skipped fork block had. It is a full reset, so it runs only then. The need is kept
// until a reset succeeds (onResetDone), since a later run finds those transactions unmined
// already. Before a load of the unmined transactions (beforeReload) that load does it.
//
// A reset whose reload did not finish leaves the pool partial. Such a reset happens only with a
// replay position, and the replay retries it (retryIncompleteReset); its reload covers this one,
// so the run waits for it rather than run a second reset in the same pass.
func (b *BlockAssembler) reloadAfterSkippedForkMark(ctx context.Context, beforeReload bool) error {
	if beforeReload || !b.skippedForkReloadPending {
		return nil
	}

	if b.bestTipResetIncomplete.Load() {
		return errors.NewProcessingError("[reloadAfterSkippedForkMark] the pool reload waits for the retry of a reset that did not finish")
	}

	tip, height := b.CurrentBlock()

	// The reset marks tip history pending when the replay position is known, which keeps mining
	// candidates empty until a replay passes it. It stays on block assembly's own tip, so it
	// leaves no history behind, also when its reload fails: bestTipResetIncomplete then has it
	// retried.
	wasPending := b.bestTipResetReplayPending.Load()
	err := b.resetToBlockAndRecord(ctx, tip, height, true)
	b.bestTipResetReplayPending.Store(wasPending)

	if err != nil {
		return errors.NewProcessingError("[reloadAfterSkippedForkMark] error reloading the pool after marking skipped fork transactions", err)
	}

	return nil
}

// saveSkippedForkMark records under SkippedForkMarkStateKey whether a run of markSkippedForkTxs
// is unfinished, so a restart finishes it.
func (b *BlockAssembler) saveSkippedForkMark(ctx context.Context, pending bool) error {
	value := byte(0)
	if pending {
		value = 1
	}

	if err := b.blockchainClient.SetState(ctx, SkippedForkMarkStateKey, []byte{value}); err != nil {
		return errors.NewStorageError("[saveSkippedForkMark] error saving state[%s]=%d", SkippedForkMarkStateKey, value, err)
	}

	return nil
}

// finishSkippedForkMarkAtStart runs a mark of skipped fork transactions left pending before a
// restart, before the start loads the unmined transactions, so it finds those the mark makes
// unmined. Without a record there is nothing pending; a record that is not the single byte 0 is
// read as pending. A run that does not finish fails the start: the pool would be loaded without
// the transactions it owes. Block validation starts alongside block assembly, so a fork block it
// is still finishing is waited for and the run tried once more first.
func (b *BlockAssembler) finishSkippedForkMarkAtStart(ctx context.Context) error {
	data, err := b.blockchainClient.GetState(ctx, SkippedForkMarkStateKey)

	switch {
	case err != nil && isStateNotFound(err):
		return nil
	case err != nil:
		return errors.NewStorageError("[BlockAssembler] error reading state[%s]", SkippedForkMarkStateKey, err)
	case len(data) == 1 && data[0] == 0:
		return nil
	}

	b.logger.Warnf("[BlockAssembler] a mark of skipped fork transactions was left unfinished before the restart, finishing it")

	err = b.markSkippedForkTxs(ctx, true)

	if _, unprocessed := err.(*skippedForkUnprocessedError); unprocessed {
		b.logger.Warnf("[BlockAssembler] waiting for block validation before marking skipped fork transactions again: %v", err)

		if !b.skipWaitForPendingBlocks {
			if err = b.subtreeProcessor.WaitForPendingBlocks(ctx); err != nil {
				return errors.NewProcessingError("[BlockAssembler] error waiting for pending blocks to finish a mark of skipped fork transactions", err)
			}
		}

		err = b.markSkippedForkTxs(ctx, true)
	}

	if err != nil {
		return errors.NewProcessingError("[BlockAssembler] a mark of skipped fork transactions left unfinished before the restart could not finish, not loading the unmined transactions without it", err)
	}

	return nil
}

// noteSkippedForkMarkFailure schedules the next run of an unfinished mark of skipped fork
// transactions: skippedForkMarkRetryBase after the first failure, twice as long after each one
// that follows, up to skippedForkMarkRetryMax. A run that cannot read a fork block would
// otherwise be repeated on every idle tick, each one in StateReconciling, where mining
// candidates are empty.
func (b *BlockAssembler) noteSkippedForkMarkFailure(now time.Time) {
	if b.skippedForkMarkWait == 0 {
		b.skippedForkMarkWait = skippedForkMarkRetryBase
	} else {
		b.skippedForkMarkWait = retry.CappedExponentialBackoff(b.skippedForkMarkWait, 2, skippedForkMarkRetryMax)
	}

	b.skippedForkMarkRetryAt = now.Add(b.skippedForkMarkWait)
}

// noteSkippedForkMarkSuccess clears the back-off once a run finishes.
func (b *BlockAssembler) noteSkippedForkMarkSuccess() {
	b.skippedForkMarkWait = 0
	b.skippedForkMarkRetryAt = time.Time{}
}

// runOwedSkippedForkWork runs the work skippedForkWorkOwed reports. Every pass ends with it
// (processNewBlockAnnouncement, handleResetRequests), whatever path the pass took, so owed work
// never waits for one path in particular. A pass that ran the mark already, before a reset, does
// not run it a second time: a run that did not finish waits for the next pass, or for the idle
// wake after its back-off, and is counted once.
func (b *BlockAssembler) runOwedSkippedForkWork(ctx context.Context) {
	if b.skippedForkMarkRan || !b.skippedForkWorkOwed() {
		return
	}

	if b.skippedForkMarkPending || b.skippedForkReloadPending {
		_ = b.markSkippedForkTxs(ctx, false)
		return
	}

	// Only a recheck is owed, for example after the replay's restamp failed: it needs no run of
	// the mark, and not block assembly on the best tip.
	if err := b.recheckSkippedForkMarks(ctx); err != nil {
		prometheusBlockAssemblyProcessingStuck.WithLabelValues("skipped_fork_mark_unfinished").Inc()
		b.noteSkippedForkMarkFailure(time.Now())
		b.logger.Errorf("[BlockAssembler] recheck of skipped fork transactions unfinished, retrying at %s: %v", b.skippedForkMarkRetryAt.Format(time.RFC3339), err)

		return
	}

	b.noteSkippedForkMarkSuccess()
}

// skippedForkMarkDue reports whether the work of a mark of skipped fork transactions
// (skippedForkWorkOwed) is due to run again.
func (b *BlockAssembler) skippedForkMarkDue(now time.Time) bool {
	return b.skippedForkWorkOwed() && !now.Before(b.skippedForkMarkRetryAt)
}

// blockTxHashes appends to dst the hashes of the transactions in a stored block, without its
// coinbase, growing dst at most once.
func (b *BlockAssembler) blockTxHashes(ctx context.Context, header *model.BlockHeader, dst []chainhash.Hash) ([]chainhash.Hash, error) {
	block, err := b.blockchainClient.GetBlock(ctx, header.Hash())
	if err != nil {
		return dst, errors.NewProcessingError("[blockTxHashes] error getting block %s", header.Hash(), err)
	}

	subtrees, err := block.GetSubtrees(ctx, b.logger, b.subtreeStore, b.settings.Block.GetAndValidateSubtreesConcurrency)
	if err != nil {
		return dst, errors.NewProcessingError("[blockTxHashes] error getting subtrees of block %s", header.Hash(), err)
	}

	count := 0
	for _, st := range subtrees {
		count += len(st.Nodes)
	}

	if cap(dst)-len(dst) < count {
		grown := make([]chainhash.Hash, len(dst), len(dst)+count)
		copy(grown, dst)
		dst = grown
	}

	for _, st := range subtrees {
		for _, node := range st.Nodes {
			if !node.Hash.IsEqual(subtree.CoinbasePlaceholderHash) {
				dst = append(dst, node.Hash)
			}
		}
	}

	return dst, nil
}

// subtreeFilesGone reports whether the subtree store has neither the subtree file nor the one
// still to be checked for some subtree of a stored block. An error means the store did not
// answer.
func (b *BlockAssembler) subtreeFilesGone(ctx context.Context, header *model.BlockHeader) (bool, error) {
	block, err := b.blockchainClient.GetBlock(ctx, header.Hash())
	if err != nil {
		return false, errors.NewProcessingError("[subtreeFilesGone] error getting block %s", header.Hash(), err)
	}

	for _, subtreeHash := range block.Subtrees {
		for _, fileType := range []fileformat.FileType{fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeToCheck} {
			exists, err := b.subtreeStore.Exists(ctx, subtreeHash[:], fileType)
			if err != nil {
				return false, errors.NewProcessingError("[subtreeFilesGone] error checking subtree %s of block %s", subtreeHash, header.Hash(), err)
			}

			if exists {
				break
			}

			if fileType == fileformat.FileTypeSubtreeToCheck {
				return true, nil
			}
		}
	}

	return false, nil
}

// giveUpBestTipHistory moves the replay position to the best tip after the replay kept failing,
// persists it, and then clears a low-water mark, so the next pass does not replay the history
// that kept failing. A failure to read the best tip leaves the position where it is.
func (b *BlockAssembler) giveUpBestTipHistory(ctx context.Context) {
	_, bestKey, err := b.bestBlockKey(ctx)
	if err != nil {
		b.logger.Errorf("[BlockAssembler] error getting best block header to give up the replay history: %v", err)
		return
	}

	b.setBestTipPos(bestKey)

	_, markData, err := b.readBestTipLowWater(ctx)
	if err != nil || markData == nil {
		return
	}

	b.settleBestTipLowWater(ctx, markData)
}

// readBestTipLowWater reads the blockchain store's low-water mark of the best tip. It returns no
// mark when none is set. An unreadable mark is cleared, since nothing can replay from it, and
// reported as none.
func (b *BlockAssembler) readBestTipLowWater(ctx context.Context) (*model.BestTipLowWater, []byte, error) {
	data, err := b.blockchainClient.GetState(ctx, model.BestTipLowWaterStateKey)
	if err != nil {
		if isStateNotFound(err) {
			return nil, nil, nil
		}

		return nil, nil, errors.NewProcessingError("[readBestTipLowWater] error reading the best tip low-water mark", err)
	}

	mark, err := model.NewBestTipLowWaterFromBytes(data)
	if err != nil {
		b.logger.Errorf("[BlockAssembler] clearing unreadable best tip low-water mark: %v", err)

		if clearErr := b.blockchainClient.ClearBestTipLowWater(ctx, data); clearErr != nil {
			b.logger.Errorf("[BlockAssembler] error clearing unreadable best tip low-water mark: %v", clearErr)
		}

		return nil, nil, nil
	}

	return &mark, data, nil
}

// readBestTipLowWaterIfDue reads the low-water mark unless it cannot have changed since a read
// that found none. It can after a BlockMinedUnset notification, which the blockchain service
// sends for every invalidation, so that clears bestTipLowWaterQuiet. A lost notification is
// covered by reading anyway once per blockassembly_reconcileInterval. A delete records a drop as
// well, but only the offline rewind tool deletes blocks, and the first pass after a start reads
// the mark. Skipping the read on the other passes saves a call to the blockchain service per
// block, which counts during initial sync.
//
// The quiet flag is set before the read, so a notification that arrives during it clears the flag
// again. A read that fails or finds a mark clears it too, so the next pass reads again.
func (b *BlockAssembler) readBestTipLowWaterIfDue(ctx context.Context) (*model.BestTipLowWater, []byte, error) {
	now := time.Now()

	if b.bestTipLowWaterQuiet.Load() {
		interval := b.settings.BlockAssembly.ReconcileInterval
		if interval <= 0 || now.Sub(time.Unix(0, b.lastBestTipLowWaterRead.Load())) < interval {
			return nil, nil, nil
		}
	}

	b.bestTipLowWaterQuiet.Store(true)
	b.lastBestTipLowWaterRead.Store(now.UnixNano())

	mark, data, err := b.readBestTipLowWater(ctx)
	if err != nil || mark != nil {
		b.bestTipLowWaterQuiet.Store(false)
	}

	return mark, data, err
}

// settleBestTipLowWater persists the replay position and then clears the low-water mark the
// replay read, if the mark still holds data. It runs once the position has moved past the mark.
// The order makes a crash between the two harmless: the mark is still there on restart and the
// replay goes over the same blocks again, each a no-op or a move that ends where it started. A
// drop recorded after the read changed the mark, so the clear leaves it for the next pass.
func (b *BlockAssembler) settleBestTipLowWater(ctx context.Context, data []byte) {
	pos := b.bestTipPos.Load()
	if pos == nil {
		return
	}

	if err := b.blockchainClient.SetState(ctx, BestTipPositionStateKey, pos.Bytes()); err != nil {
		b.logger.Errorf("[BlockAssembler] error saving best tip position before clearing the low-water mark, keeping the mark: %v", err)
		return
	}

	b.bestTipPosSaved.Store(pos)

	if err := b.blockchainClient.ClearBestTipLowWater(ctx, data); err != nil {
		b.logger.Errorf("[BlockAssembler] error clearing the best tip low-water mark, the next pass replays from it again: %v", err)
	}
}

// replayBestTipChanges moves block assembly, in order, through every valid block that sorts
// better than its position in the blockchain store's best-tip order, and moves the position to
// each one it handles. It returns how many blocks it read, and stops at the first one it fails
// to apply, leaving the position on the one before so the next pass retries it.
//
// Without an invalidation the best tip only ever moves to a block that sorts better, so every
// block that was the best tip since the position is among these, in an order it could have
// become the tip in. Moving to each in turn is what makes a branch that won and lost again
// before block assembly looked go forward and back through the subtree processor: its
// transactions are re-added ahead of any child spending them, its conflicts are swapped both
// ways, and its transactions are re-stamped unmined. A block that a later block of the same page
// builds on is not moved to on its own, because moving to the later one passes through it.
//
// A drop of the best tip, by an invalidation or a delete, is recorded by the store in a
// low-water mark. The pass reads the mark when it may have changed (readBestTipLowWaterIfDue) and also replays the blocks at or above it, so the
// block the tip dropped to and every branch that won after it are handled although they may
// sort below the position. The mark also names the blocks that were the best tip when it
// dropped. One that block assembly never applied is in no read of valid blocks, so the pass
// resets for it first (resetForUnappliedDroppedTips). Once the position has moved past the mark,
// the position is persisted and the mark cleared. Valid fork blocks that sort better but were never the tip are moved to
// as well; the pool still ends right, at the cost of those extra moves.
//
// Blocks below the height floor are skipped: retention bounds how far back block assembly can
// move, and a deeper move is a reset either way.
func (b *BlockAssembler) replayBestTipChanges(ctx context.Context) (int, error) {
	read := 0

	// A reset can finalize its target before its reload or state write fails.
	// Retry that rebuild before the hash-equality fast path can acknowledge it.
	if b.bestTipResetIncomplete.Load() {
		historyGivenUp := !b.bestTipResetReplayPending.Load()

		if err := b.retryIncompleteReset(ctx); err != nil {
			if !historyGivenUp {
				return read, err
			}

			// The history was given up after this reset kept failing, and candidates are
			// built from the pool as it is. A failed retry must not hold them empty again or
			// stop the replay; the next pass retries.
			b.bestTipResetReplayPending.Store(false)
			prometheusBlockAssemblyProcessingStuck.WithLabelValues("best_tip_reset_reload").Inc()
			b.logger.Errorf("[BlockAssembler] retry of a reset that did not finish its reload failed, the pool may be partial, retrying on the next pass: %v", err)
		}
	}

	mark, markData, err := b.readBestTipLowWaterIfDue(ctx)
	if err != nil {
		return read, err
	}

	var from *model.BestTipKey
	if mark != nil {
		from = &mark.Key
		b.logger.Infof("[BlockAssembler] best tip dropped to %s since the last replay, replaying from there", mark.Key.Hash)

		// The mark is cleared only once the reset has run and the position is saved, so a
		// crash in between repeats the reset, which is safe.
		if err = b.resetForUnappliedDroppedTips(ctx, mark); err != nil {
			return read, err
		}
	}

	// settle clears the mark once the position has moved past it, also when the pass stops
	// at a failure after that.
	settle := func() {
		if markData != nil && from == nil {
			b.settleBestTipLowWater(ctx, markData)
			markData = nil
		}
	}

	for {
		pos := b.bestTipPos.Load()
		if pos == nil {
			return read, errors.NewProcessingError("[replayBestTipChanges] no replay position")
		}

		_, tipHeight := b.CurrentBlock()

		headers, metas, more, err := b.blockchainClient.BlocksBetterThan(ctx, *pos, from, b.bestTipFloor(tipHeight), bestTipPageSize)
		if err != nil {
			return read, errors.NewProcessingError("[replayBestTipChanges] error getting blocks better than the replay position", err)
		}

		if len(metas) != len(headers) {
			return read, errors.NewProcessingError("[replayBestTipChanges] %d headers with %d metas", len(headers), len(metas))
		}

		read += len(headers)

		if len(headers) == 0 {
			// Nothing at or above the mark is left within the floor, so it is spent too.
			from = nil
			settle()
			b.bestTipResetReplayPending.Store(false)

			return read, nil
		}

		// A block whose child is later in the page is on the child's path.
		parents := make(map[chainhash.Hash]struct{}, len(headers))
		for _, h := range headers {
			parents[*h.HashPrevBlock] = struct{}{}
		}

		reachedBest := false

		for i := range headers {
			hash := headers[i].Hash()

			if _, hasChild := parents[*hash]; hasChild && i+1 < len(headers) {
				continue
			}

			// The best tip sorts better than every other valid block, so a page with nothing
			// after it ends with the best tip. That saves a read of the best header per pass.
			last := !more && i+1 == len(headers)

			if err = b.applyBestTipChange(ctx, headers[i], metas[i], last); err != nil && !errors.Is(err, errors.ErrBlockAssemblyReset) {
				// The position stays before this block, which the next pass retries. The
				// blocks skipped above are on its path, so they are retried with it.
				settle()
				return read, err
			}

			// Valid blocks reset to their own target. Invalid ones reset after
			// validation has made their transactions recoverable.
			b.setBestTipPos(model.NewBestTipKey(headers[i], metas[i]))
			from = nil
			reachedBest = last
		}

		settle()

		if reachedBest {
			b.bestTipResetReplayPending.Store(false)
			return read, nil
		}
	}
}

// retryIncompleteReset runs again a reset that rotated the tx map and then failed, on block
// assembly's current tip, or to the best tip when that tip is invalid now.
func (b *BlockAssembler) retryIncompleteReset(ctx context.Context) error {
	tip, height := b.CurrentBlock()
	if tip == nil {
		return errors.NewProcessingError("cannot retry incomplete reset without a tip")
	}

	_, meta, err := b.blockchainClient.GetBlockHeader(ctx, tip.Hash())
	if err != nil {
		return err
	}

	if meta.Invalid {
		return b.resetAndRecord(ctx, true)
	}

	return b.resetToBlockAndRecord(ctx, tip, height, true)
}

// applyBestTipChange moves block assembly to one block the replay read. last says the block is
// the best tip now.
//
// A block already on block assembly's chain is skipped unless it is the best tip. Moving back to
// it and forward again to a later block nets out, but when it is the best tip the tip moved back
// to it, for example after an invalidation of block assembly's tip, and block assembly must move
// back too.
//
// A block that is invalid is never moved to. The replay reads only valid blocks, so this guards
// a caller that passes metadata read earlier. If block assembly's chain contains the block, a
// later block moves off it, through the reset rule for invalid blocks. If not, block assembly
// never applied it, and its block-only transactions reach the pool only through a reset, once
// block validation has stamped them unmined again, which it does when it processes the
// invalidation.
func (b *BlockAssembler) applyBestTipChange(ctx context.Context, header *model.BlockHeader, meta *model.BlockHeaderMeta, last bool) error {
	tip, tipHeight := b.CurrentBlock()
	if tip == nil {
		return errors.NewProcessingError("[applyBestTipChange] block assembly has no tip")
	}

	hash := header.Hash()

	if hash.IsEqual(tip.Hash()) {
		return nil
	}

	// A block extending the tip cannot be on its chain, so it needs no ancestry check.
	if !last && !header.HashPrevBlock.IsEqual(tip.Hash()) && meta.Height <= tipHeight {
		onChain, err := b.blockchainClient.CheckBlockIsAncestorOfBlock(ctx, []uint32{meta.ID}, tip.Hash())
		if err != nil {
			return errors.NewProcessingError("[applyBestTipChange] error checking whether %s is on block assembly's chain", hash, err)
		}

		if onChain {
			// Already applied, for example by a reset that went straight to a later tip, and
			// a later change moves block assembly on from it.
			return nil
		}
	}

	if meta.Invalid {
		b.logger.Warnf("[BlockAssembler] best tip change %s is invalid now and was never applied, resetting so its transactions are reloaded", hash)

		if err := b.resetAfterInvalidBlocks(ctx, []chainhash.Hash{*hash}); err != nil {
			return errors.NewProcessingError("[applyBestTipChange] error resetting after invalid block %s", hash, err)
		}

		return errBestTipReset
	}

	if !last && !header.HashPrevBlock.IsEqual(tip.Hash()) {
		restamped, err := b.restampPastStaleBranch(ctx, header, meta)
		if err != nil || restamped {
			return err
		}
	}

	b.logger.Infof("[BlockAssembler] replaying best tip change %d: %s", meta.Height, hash)

	return b.moveToBlock(ctx, header, meta)
}

// resetAfterInvalidBlocks runs one reset to the best tip for invalid blocks block assembly never
// applied. Their block-only transactions are in no valid block and in no pool, and block
// validation stamps them unmined when it processes the invalidation, so the reset's reload finds
// them. The reset waits for that, a bounded wait per block, and resets anyway once it gives up,
// as a reset that moves back over an invalid block does.
func (b *BlockAssembler) resetAfterInvalidBlocks(ctx context.Context, hashes []chainhash.Hash) error {
	for i := range hashes {
		if err := b.waitForBlockMinedSet(ctx, &hashes[i]); err != nil {
			if ctx.Err() != nil {
				return err
			}

			b.logger.Warnf("[BlockAssembler] gave up waiting for invalid block %s to be processed: %v (resetting anyway)", hashes[i], err)
		}
	}

	return b.resetAndRecord(ctx, false)
}

// resetForUnappliedDroppedTips resets block assembly once if the low-water mark names a block
// that was the best tip when it was invalidated or deleted and that block assembly never applied.
// The replay reads only valid blocks, so without this such a block's block-only transactions
// would reach the pool only at the next reset or restart.
//
// A dropped tip on block assembly's own chain needs no reset here: the replay moves block
// assembly off it, and a move back over an invalid block is a reset already. One that is valid
// again needs none either, since it sorts better than the mark and the replay moves to it like
// any other block. A mark that lost tips (DroppedTipsOverflow) resets regardless. One reset
// covers every tip the mark names.
func (b *BlockAssembler) resetForUnappliedDroppedTips(ctx context.Context, mark *model.BestTipLowWater) error {
	unapplied := make([]chainhash.Hash, 0, len(mark.DroppedTips))

	for _, hash := range mark.DroppedTips {
		needs, err := b.droppedTipNeedsReset(ctx, hash)
		if err != nil {
			return err
		}

		if needs {
			unapplied = append(unapplied, hash)
		}
	}

	if len(unapplied) == 0 && !mark.DroppedTipsOverflow {
		return nil
	}

	if len(unapplied) == 0 {
		b.logger.Warnf("[BlockAssembler] best tip low-water mark lost track of some dropped tips, resetting so their transactions are reloaded")
	} else {
		b.logger.Warnf("[BlockAssembler] %d dropped best tips were never applied (first %s), resetting so their transactions are reloaded", len(unapplied), unapplied[0])
	}

	if err := b.resetAfterInvalidBlocks(ctx, unapplied); err != nil {
		return errors.NewProcessingError("[resetForUnappliedDroppedTips] error resetting after dropped best tips", err)
	}

	return nil
}

// droppedTipNeedsReset reports whether a block that was the best tip when it dropped needs a reset
// to bring its transactions back: it is invalid or gone, and not on block assembly's chain.
func (b *BlockAssembler) droppedTipNeedsReset(ctx context.Context, hash chainhash.Hash) (bool, error) {
	tip, tipHeight := b.CurrentBlock()
	if tip != nil && tip.Hash().IsEqual(&hash) {
		return false, nil
	}

	_, meta, err := b.blockchainClient.GetBlockHeader(ctx, &hash)
	if err != nil {
		if errors.Is(err, errors.ErrBlockNotFound) || errors.Is(err, errors.ErrNotFound) {
			// Deleted, and not block assembly's tip.
			return true, nil
		}

		return false, errors.NewProcessingError("[droppedTipNeedsReset] error getting dropped best tip %s", hash, err)
	}

	if !meta.Invalid {
		return false, nil
	}

	if tip == nil || meta.Height > tipHeight {
		return true, nil
	}

	onChain, err := b.blockchainClient.CheckBlockIsAncestorOfBlock(ctx, []uint32{meta.ID}, tip.Hash())
	if err != nil {
		return false, errors.NewProcessingError("[droppedTipNeedsReset] error checking whether %s is on block assembly's chain", hash, err)
	}

	return !onChain, nil
}

// restampPastStaleBranch handles a past change whose move would take block assembly back over
// blocks of the current best chain in a way that cannot be undone or cannot run. It reports
// whether it handled the change, in which case block assembly has not moved.
//
// Two cases need it:
//
//   - Coinbase spends. Moving back a block deletes every child spend of its coinbase
//     (removeCoinbaseUtxos). On a real reorg those spends are on the losing side. Here the
//     blocks moved back are on the chain that won, and moving forward again recreates only the
//     coinbases, so confirmed transactions would be lost and the coinbases would read as
//     unspent. A coinbase can be spent from coinbase maturity blocks above it, so a move back is
//     safe while the best tip is less than maturity blocks above the lowest block it moves back.
//     That always holds for a change replayed from the change before it, which is the move the
//     chain itself made. It can fail when block assembly is ahead of the change: after a reset
//     straight to the best tip, after a crash late in a long pass, or on a start without a
//     persisted position.
//   - Conflict swaps. Moving back a block that resolved a double spend reverses its swap, and the
//     current-chain guard refuses to demote a winner mined in a block of the current chain. The
//     move would fail on every pass until the replay gives its history up, which skips the branch
//     again. That can happen at any depth: the best tip goes from X to its sibling Y and then to
//     X's child X2, so replaying Y moves X back while X is on the best chain. Block assembly's
//     own tip may have lost meanwhile: on X2 when the best tip went to Y and then to Z2, another
//     child of X, the move back to Y passes X2, which is off the best chain, and then X, which
//     is on it. So every block moved back that is on the best chain is checked.
//
// Such a change is applied at its own depth instead. Block assembly never applied the stale
// branch, so it never swapped the branch's conflicts or created its coinbases, and what is left to
// undo is the mined mark block validation gave the branch's transactions. restampStaleBranch
// clears that mark and reloads the pool on block assembly's current tip. It costs a pool reload,
// so the plain move is kept for every other change.
func (b *BlockAssembler) restampPastStaleBranch(ctx context.Context, header *model.BlockHeader, meta *model.BlockHeaderMeta) (bool, error) {
	moveBack, moveForward, err := b.getReorgBlockHeaders(ctx, header, meta.Height)
	if err != nil {
		return false, errors.NewProcessingError("[restampPastStaleBranch] error getting the path to %s", header.Hash(), err)
	}

	if len(moveBack) == 0 {
		return false, nil
	}

	best, bestMeta, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return false, errors.NewProcessingError("[restampPastStaleBranch] error getting best block header", err)
	}

	lowest := moveBack[len(moveBack)-1]
	maturity := uint64(b.settings.ChainCfgParams.CoinbaseMaturity) //nolint:gosec // maturity is a small positive chain parameter

	// Spendable from height lowest+maturity, and the next block is bestHeight+1.
	pastMaturity := uint64(bestMeta.Height)+1 >= uint64(lowest.meta.Height)+maturity

	// moveBack runs from block assembly's tip down to the fork point, so the blocks on the best
	// chain are a run at its end. If the lowest is off it, none is.
	onBest, err := b.blockchainClient.CheckBlockIsAncestorOfBlock(ctx, []uint32{lowest.meta.ID}, best.Hash())
	if err != nil {
		return false, errors.NewProcessingError("[restampPastStaleBranch] error checking whether %s is on the best chain", lowest.header.Hash(), err)
	}

	if !onBest {
		// Every block moved back is on a branch that lost, with its coinbase spends and its
		// conflict swaps.
		return false, nil
	}

	reason := "past coinbase maturity"

	if !pastMaturity {
		onBestRun, err := b.bestChainRun(ctx, moveBack, best)
		if err != nil {
			return false, err
		}

		resolved, err := b.blocksResolvedConflicts(ctx, onBestRun)
		if err != nil || !resolved {
			return false, err
		}

		reason = "with resolved double spends"
	}

	b.logger.Warnf("[BlockAssembler] best tip change %d: %s is on a stale branch forking %d blocks below the best tip, restamping its %d blocks instead of moving back over the best chain %s", meta.Height, header.Hash(), bestMeta.Height-lowest.meta.Height+1, len(moveForward), reason)

	return true, b.restampStaleBranch(ctx, moveForward, best)
}

// bestChainRun returns the blocks at the end of moveBack that are on the best chain, given that
// the last one is. moveBack runs down from block assembly's tip, so the run stops at the first
// block, going up, that is off the best chain.
func (b *BlockAssembler) bestChainRun(ctx context.Context, moveBack []blockHeaderWithMeta, best *model.BlockHeader) ([]blockHeaderWithMeta, error) {
	start := len(moveBack) - 1

	for start > 0 {
		hm := moveBack[start-1]

		onBest, err := b.blockchainClient.CheckBlockIsAncestorOfBlock(ctx, []uint32{hm.meta.ID}, best.Hash())
		if err != nil {
			return nil, errors.NewProcessingError("[bestChainRun] error checking whether %s is on the best chain", hm.header.Hash(), err)
		}

		if !onBest {
			break
		}

		start--
	}

	return moveBack[start:], nil
}

// blocksResolvedConflicts reports whether any of the blocks lists conflicting transactions in its
// subtrees, so that moving it back would reverse a conflict swap.
func (b *BlockAssembler) blocksResolvedConflicts(ctx context.Context, blocks []blockHeaderWithMeta) (bool, error) {
	for _, hm := range blocks {
		block, err := b.blockchainClient.GetBlock(ctx, hm.header.Hash())
		if err != nil {
			return false, errors.NewProcessingError("[blocksResolvedConflicts] error getting block %s", hm.header.Hash(), err)
		}

		for _, subtreeHash := range block.Subtrees {
			conflicting, err := b.subtreeConflictingNodes(ctx, subtreeHash)
			if err != nil {
				return false, errors.NewProcessingError("[blocksResolvedConflicts] error reading conflicting nodes of subtree %s in block %s", subtreeHash, hm.header.Hash(), err)
			}

			if len(conflicting) > 0 {
				return true, nil
			}
		}
	}

	return false, nil
}

// subtreeConflictingNodes reads the conflicting nodes of one subtree, from the stored subtree or,
// failing that, the one still waiting to be checked, as the subtree processor does.
func (b *BlockAssembler) subtreeConflictingNodes(ctx context.Context, subtreeHash *chainhash.Hash) ([]chainhash.Hash, error) {
	reader, err := b.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	if err != nil {
		if reader, err = b.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck); err != nil {
			return nil, err
		}
	}

	defer func() {
		_ = reader.Close()
	}()

	return subtree.DeserializeSubtreeConflictingFromReader(reader)
}

// restampStaleBranch marks every transaction of the given stale blocks that no block of the best
// chain contains as not on the longest chain, then reloads the pool on block assembly's tip so
// those transactions are in it. A transaction the UTXO store has pruned is left out: it has no
// record to mark. Invalid blocks are left to block validation, which unmarks their transactions
// when it processes the invalidation.
//
// It first waits for block validation to finish marking every pending block mined. A branch block
// that was briefly the best tip has its transactions marked mined with onLongestChain=true, in
// chunks. A chunk written after the mark below would clear it again, and the reload would then
// miss those transactions while no block of the best chain has them.
func (b *BlockAssembler) restampStaleBranch(ctx context.Context, branch []blockHeaderWithMeta, best *model.BlockHeader) error {
	if err := b.subtreeProcessor.WaitForPendingBlocks(ctx); err != nil {
		return errors.NewProcessingError("[restampStaleBranch] error waiting for pending blocks", err)
	}

	staleIDs := make(map[uint32]struct{}, len(branch))
	txCount := uint64(0)
	for _, hm := range branch {
		txCount += hm.meta.TxCount
	}

	hashes := make([]chainhash.Hash, 0, min(txCount, uint64(len(branch))*maxRestampPresizePerBlock))

	for _, hm := range branch {
		if hm.meta.Invalid {
			continue
		}

		b.heartbeat.BeatIfStarted()

		staleIDs[hm.meta.ID] = struct{}{}

		var err error
		if hashes, err = b.blockTxHashes(ctx, hm.header, hashes); err != nil {
			return errors.NewProcessingError("[restampStaleBranch] error reading block %s", hm.header.Hash(), err)
		}
	}

	branchOnly, err := b.branchOnlyTxs(ctx, hashes, staleIDs, best)
	if err != nil {
		return err
	}

	if len(branchOnly) > 0 {
		b.heartbeat.BeatIfStarted()

		if _, err = b.markOffLongestChain(ctx, branchOnly); err != nil {
			return errors.NewProcessingError("[restampStaleBranch] error marking %d stale branch transactions as not on the longest chain", len(branchOnly), err)
		}
	}

	b.logger.Infof("[BlockAssembler] restamped %d of %d stale branch transactions, reloading the pool", len(branchOnly), len(hashes))

	tip, height := b.CurrentBlock()

	return b.resetToBlockAndRecord(ctx, tip, height, true)
}

// branchOnlyTxs returns the hashes, in order, whose transaction no block of the best chain
// contains. A hash the UTXO store has no record of (pruned) is left out.
func (b *BlockAssembler) branchOnlyTxs(ctx context.Context, hashes []chainhash.Hash, staleIDs map[uint32]struct{}, best *model.BlockHeader) ([]chainhash.Hash, error) {
	kept, _, err := b.branchOnlyMeta(ctx, hashes, staleIDs, best)
	if err != nil {
		return nil, err
	}

	branchOnly := make([]chainhash.Hash, len(kept))
	for i, u := range kept {
		branchOnly[i] = u.Hash
	}

	return branchOnly, nil
}

// branchOnlyMeta is branchOnlyTxs returning each transaction's record, read with its block ids
// and the extra fields asked for, and also the hashes whose transaction a block of the best chain
// contains.
func (b *BlockAssembler) branchOnlyMeta(ctx context.Context, hashes []chainhash.Hash, staleIDs map[uint32]struct{}, best *model.BlockHeader, extra ...fields.FieldName) ([]*utxo.UnresolvedMetaData, []chainhash.Hash, error) {
	kept := make([]*utxo.UnresolvedMetaData, 0, len(hashes))
	var mined []chainhash.Hash
	requested := append([]fields.FieldName{fields.BlockIDs}, extra...)
	onBest := make(map[uint32]bool)

	for start := 0; start < len(hashes); start += resetExistenceChunkSize {
		b.heartbeat.BeatIfStarted()

		chunk := hashes[start:min(start+resetExistenceChunkSize, len(hashes))]

		unresolved := make([]*utxo.UnresolvedMetaData, len(chunk))
		for i := range chunk {
			unresolved[i] = &utxo.UnresolvedMetaData{Hash: chunk[i], Idx: i}
		}

		if err := b.utxoStore.BatchDecorate(ctx, unresolved, requested...); err != nil {
			return nil, nil, errors.NewProcessingError("[branchOnlyMeta] error reading block ids", err)
		}

	next:
		for i, u := range unresolved {
			switch {
			case errors.Is(u.Err, errors.ErrTxNotFound):
				continue
			case u.Err != nil:
				return nil, nil, errors.NewProcessingError("[branchOnlyMeta] error reading block ids of %s", chunk[i], u.Err)
			}

			if u.Data != nil {
				for _, id := range u.Data.BlockIDs {
					if _, stale := staleIDs[id]; stale {
						continue
					}

					on, known := onBest[id]
					if !known {
						var err error
						if on, err = b.blockchainClient.CheckBlockIsAncestorOfBlock(ctx, []uint32{id}, best.Hash()); err != nil {
							return nil, nil, errors.NewProcessingError("[branchOnlyMeta] error checking whether block %d is on the best chain", id, err)
						}

						onBest[id] = on
					}

					if on {
						mined = append(mined, chunk[i])
						continue next
					}
				}
			}

			kept = append(kept, u)
		}
	}

	return kept, mined, nil
}

// bestBlockKey returns the best block and its place in the best-tip order. The place is read from
// the block's own header metadata: over gRPC, GetBestBlockHeader's metadata carries no block id or
// peer id, and a key without them sorts better than the best block itself, so the replay would
// miss a block of equal work that becomes the best tip on a lower peer id.
func (b *BlockAssembler) bestBlockKey(ctx context.Context) (*model.BlockHeader, model.BestTipKey, error) {
	best, _, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, model.BestTipKey{}, err
	}

	header, meta, err := b.blockchainClient.GetBlockHeader(ctx, best.Hash())
	if err != nil {
		return nil, model.BestTipKey{}, err
	}

	return header, model.NewBestTipKey(header, meta), nil
}

// startingBestTipPosition is the replay position without a persisted one. On the best tip, block
// assembly starts there: replaying older blocks would move it onto every stale branch within the
// floor and back again. Off it, it starts at its own tip, the last block it processed, and the
// replay moves it to the best tip through the blocks that sort better.
func (b *BlockAssembler) startingBestTipPosition(ctx context.Context) (model.BestTipKey, error) {
	best, bestKey, err := b.bestBlockKey(ctx)
	if err != nil {
		return model.BestTipKey{}, err
	}

	tip, _ := b.CurrentBlock()
	if tip == nil || best.Hash().IsEqual(tip.Hash()) {
		b.logger.Infof("[BlockAssembler] no persisted best tip position, block assembly is on the best tip, starting there")
		return bestKey, nil
	}

	header, meta, err := b.blockchainClient.GetBlockHeader(ctx, tip.Hash())
	if err != nil {
		return model.BestTipKey{}, err
	}

	b.logger.Infof("[BlockAssembler] no persisted best tip position, replaying from block assembly's tip %s", tip.Hash())

	return model.NewBestTipKey(header, meta), nil
}

// idleReconcileNeeded reports whether the idle tick should start a replay: when a low-water mark
// is set, when a block sorts better than the replay position, or when block assembly is not on
// the best tip. It runs outside StateReconciling, so a mining candidate asked for during the
// check is built as usual. It stamps lastBestTipReplay, so the next check waits a full interval.
func (b *BlockAssembler) idleReconcileNeeded(ctx context.Context) bool {
	b.lastBestTipReplay.Store(time.Now().UnixNano())
	if b.bestTipResetReplayPending.Load() || b.skippedForkMarkDue(time.Now()) {
		return true
	}

	if b.bestTipPosKnown.Load() {
		if _, err := b.blockchainClient.GetState(ctx, model.BestTipLowWaterStateKey); err == nil {
			return true
		}

		if pos := b.bestTipPos.Load(); pos != nil {
			_, tipHeight := b.CurrentBlock()

			if headers, _, _, err := b.blockchainClient.BlocksBetterThan(ctx, *pos, nil, b.bestTipFloor(tipHeight), 1); err == nil && len(headers) > 0 {
				return true
			}
		}
	}

	best, _, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		// The replay reports the error.
		return true
	}

	tip, _ := b.CurrentBlock()

	return tip == nil || !best.Hash().IsEqual(tip.Hash())
}

// bestTipFloor is the lowest height of a block the replay reads, the tip minus the UTXO
// retention. With retention 0 (pruning off) nothing is skipped.
func (b *BlockAssembler) bestTipFloor(tipHeight uint32) uint32 {
	retention := b.settings.GetUtxoStoreBlockHeightRetention()
	if retention == 0 || tipHeight <= retention {
		return 0
	}

	return tipHeight - retention
}

// setBestTipPos moves the replay position to key. The replay reads blocks in the store's order
// and moves the position along it, so the position never needs comparing here.
func (b *BlockAssembler) setBestTipPos(key model.BestTipKey) {
	b.bestTipPos.Store(&key)
}

// returnToRunning ends a block-processing state and then persists the replay position, at most
// once per bestTipPositionSaveInterval. The save is one write to the blockchain service, and a
// mining candidate asked for during a processing state is an empty one, so the save waits until
// block assembly is running again.
func (b *BlockAssembler) returnToRunning(ctx context.Context) {
	b.setCurrentRunningState(StateRunning)

	if time.Since(time.Unix(0, b.lastBestTipPosSave.Load())) >= bestTipPositionSaveInterval {
		b.saveBestTipPosition(ctx)
	}
}

// saveBestTipPosition persists the replay position when it moved since the last save. A failed,
// missed or deferred save only costs a restart replaying blocks block assembly already handled,
// each of which is then a no-op or a move that ends where it started, so it is logged and not
// returned. During initial sync that is a few seconds of blocks, against a write per block saved.
func (b *BlockAssembler) saveBestTipPosition(ctx context.Context) {
	b.lastBestTipPosSave.Store(time.Now().UnixNano())

	if !b.bestTipPosKnown.Load() {
		return
	}

	pos := b.bestTipPos.Load()
	if pos == nil {
		return
	}

	if saved := b.bestTipPosSaved.Load(); saved != nil && bytes.Equal(saved.Bytes(), pos.Bytes()) {
		return
	}

	if err := b.blockchainClient.SetState(ctx, BestTipPositionStateKey, pos.Bytes()); err != nil {
		if !errors.Is(err, context.Canceled) {
			b.logger.Errorf("[BlockAssembler] error saving best tip position %s: %v", pos.Hash, err)
		}

		return
	}

	b.bestTipPosSaved.Store(pos)
}

// reconcileDue reports whether the idle tick should wake a replay: none has run for
// blockassembly_reconcileInterval. It covers lost notifications, because the blockchain
// service drops a subscriber whose buffer fills and nothing arrives until it reconnects.
func (b *BlockAssembler) reconcileDue(now time.Time) bool {
	// Recovery of a reset that already discarded its pool, and an unfinished mark of skipped
	// fork transactions once its back-off is over, are mandatory even when ordinary
	// lost-notification reconciliation is disabled.
	if b.bestTipResetReplayPending.Load() || b.skippedForkMarkDue(now) {
		return true
	}

	interval := b.settings.BlockAssembly.ReconcileInterval
	if interval <= 0 {
		return false
	}

	return now.Sub(time.Unix(0, b.lastBestTipReplay.Load())) >= interval
}

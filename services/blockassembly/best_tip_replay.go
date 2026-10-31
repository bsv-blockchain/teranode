package blockassembly

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// BestTipPositionStateKey is the key under which block assembly persists its replay position, the
// place in the blockchain store's best-tip order of the last block it handled (a serialised
// model.BestTipKey). It is kept apart from StateKey so an older binary, which decodes that blob
// strictly, can still start after a rollback. It replaces BlockAssemblerBestTipSeq, an 8-byte
// sequence number an unreleased build wrote, which is never read.
const BestTipPositionStateKey = "BlockAssemblerBestTipPos"

const (
	// bestTipPageSize is how many blocks one BlocksBetterThan call returns.
	bestTipPageSize = 1000

	// maxBestTipReplayFailures is how many passes in a row the replay may fail before block
	// assembly follows the best header once instead, so a block it cannot apply does not hold
	// it off the tip for ever.
	maxBestTipReplayFailures = 3

	// bestTipProbeInterval is how often block assembly asks a blockchain service that did not
	// serve BlocksBetterThan whether it does now, as it will once it is upgraded.
	bestTipProbeInterval = time.Minute
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
	if _, _, err := b.blockchainClient.BlocksBetterThan(ctx, bestTipProbeKey, nil, 0, 1); err != nil {
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
// low-water mark. The pass reads the mark and also replays the blocks at or above it, so the
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

	mark, markData, err := b.readBestTipLowWater(ctx)
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
		best, _, err := b.blockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			return read, errors.NewProcessingError("[replayBestTipChanges] error getting best block header", err)
		}

		pos := b.bestTipPos.Load()
		if pos == nil {
			return read, errors.NewProcessingError("[replayBestTipChanges] no replay position")
		}

		_, tipHeight := b.CurrentBlock()

		headers, metas, err := b.blockchainClient.BlocksBetterThan(ctx, *pos, from, b.bestTipFloor(tipHeight), bestTipPageSize)
		if err != nil {
			return read, errors.NewProcessingError("[replayBestTipChanges] error getting blocks better than the replay position", err)
		}

		if len(metas) != len(headers) {
			return read, errors.NewProcessingError("[replayBestTipChanges] %d headers with %d metas", len(headers), len(metas))
		}

		read += len(headers)

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

			last := hash.IsEqual(best.Hash())

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

		if len(headers) == 0 {
			// Nothing at or above the mark is left within the floor, so it is spent too.
			from = nil
		}

		settle()

		if len(headers) == 0 || reachedBest {
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
		restamped, err := b.restampDeepStaleBranch(ctx, header, meta)
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

// restampDeepStaleBranch handles a past change whose move would take block assembly back over
// blocks of the current best chain whose coinbases may already be spent. It reports whether it
// handled the change, in which case block assembly has not moved.
//
// Moving back a block deletes every child spend of its coinbase (removeCoinbaseUtxos). On a real
// reorg those spends are on the losing side. Here the blocks moved back are on the chain that
// won, and moving forward again recreates only the coinbases, so confirmed transactions would be
// lost and the coinbases would read as unspent. A coinbase can be spent from coinbase maturity
// blocks above it, so a move back is safe while the best tip is less than maturity blocks above
// the lowest block it moves back. That always holds for a change replayed from the change before
// it, which is the move the chain itself made. It can fail when block assembly is ahead of the
// change: after a reset straight to the best tip, after a crash late in a long pass, or on a
// start without a persisted position.
//
// Such a change is applied at its own depth instead. Block assembly never applied the stale
// branch, so it never swapped the branch's conflicts or created its coinbases, and what is left to
// undo is the mined mark block validation gave the branch's transactions. restampStaleBranch
// clears that mark and reloads the pool on block assembly's current tip.
func (b *BlockAssembler) restampDeepStaleBranch(ctx context.Context, header *model.BlockHeader, meta *model.BlockHeaderMeta) (bool, error) {
	moveBack, moveForward, err := b.getReorgBlockHeaders(ctx, header, meta.Height)
	if err != nil {
		return false, errors.NewProcessingError("[restampDeepStaleBranch] error getting the path to %s", header.Hash(), err)
	}

	if len(moveBack) == 0 {
		return false, nil
	}

	best, bestMeta, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return false, errors.NewProcessingError("[restampDeepStaleBranch] error getting best block header", err)
	}

	lowest := moveBack[len(moveBack)-1].meta
	maturity := uint64(b.settings.ChainCfgParams.CoinbaseMaturity) //nolint:gosec // maturity is a small positive chain parameter

	// Spendable from height lowest+maturity, and the next block is bestHeight+1.
	if uint64(bestMeta.Height)+1 < uint64(lowest.Height)+maturity {
		return false, nil
	}

	onBest, err := b.blockchainClient.CheckBlockIsAncestorOfBlock(ctx, []uint32{lowest.ID}, best.Hash())
	if err != nil {
		return false, errors.NewProcessingError("[restampDeepStaleBranch] error checking whether %s is on the best chain", moveBack[len(moveBack)-1].header.Hash(), err)
	}

	if !onBest {
		// Every block moved back is on a branch that lost, so its coinbase spends lost too.
		return false, nil
	}

	b.logger.Warnf("[BlockAssembler] best tip change %d: %s is %d blocks below the best tip on a stale branch, restamping its %d blocks instead of moving back over the best chain", meta.Height, header.Hash(), bestMeta.Height-lowest.Height+1, len(moveForward))

	return true, b.restampStaleBranch(ctx, moveForward, best)
}

// restampStaleBranch marks every transaction of the given stale blocks that no block of the best
// chain contains as not on the longest chain, then reloads the pool on block assembly's tip so
// those transactions are in it. A transaction the UTXO store has pruned is left out: it has no
// record to mark. Invalid blocks are left to block validation, which unmarks their transactions
// when it processes the invalidation.
func (b *BlockAssembler) restampStaleBranch(ctx context.Context, branch []blockHeaderWithMeta, best *model.BlockHeader) error {
	staleIDs := make(map[uint32]struct{}, len(branch))
	hashes := make([]chainhash.Hash, 0, len(branch)*100)

	for _, hm := range branch {
		if hm.meta.Invalid {
			continue
		}

		b.heartbeat.BeatIfStarted()

		staleIDs[hm.meta.ID] = struct{}{}

		block, err := b.blockchainClient.GetBlock(ctx, hm.header.Hash())
		if err != nil {
			return errors.NewProcessingError("[restampStaleBranch] error getting block %s", hm.header.Hash(), err)
		}

		subtrees, err := block.GetSubtrees(ctx, b.logger, b.subtreeStore, b.settings.Block.GetAndValidateSubtreesConcurrency)
		if err != nil {
			return errors.NewProcessingError("[restampStaleBranch] error getting subtrees of block %s", hm.header.Hash(), err)
		}

		for _, st := range subtrees {
			for _, node := range st.Nodes {
				if !node.Hash.IsEqual(subtree.CoinbasePlaceholderHash) {
					hashes = append(hashes, node.Hash)
				}
			}
		}
	}

	branchOnly, err := b.branchOnlyTxs(ctx, hashes, staleIDs, best)
	if err != nil {
		return err
	}

	if len(branchOnly) > 0 {
		b.heartbeat.BeatIfStarted()

		if err = b.utxoStore.MarkTransactionsOnLongestChain(ctx, branchOnly, false); err != nil {
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
	kept := make([]chainhash.Hash, 0, len(hashes))
	onBest := make(map[uint32]bool)

	for start := 0; start < len(hashes); start += resetExistenceChunkSize {
		b.heartbeat.BeatIfStarted()

		chunk := hashes[start:min(start+resetExistenceChunkSize, len(hashes))]

		unresolved := make([]*utxo.UnresolvedMetaData, len(chunk))
		for i := range chunk {
			unresolved[i] = &utxo.UnresolvedMetaData{Hash: chunk[i], Idx: i}
		}

		if err := b.utxoStore.BatchDecorate(ctx, unresolved, fields.BlockIDs); err != nil {
			return nil, errors.NewProcessingError("[branchOnlyTxs] error reading block ids", err)
		}

	next:
		for i, u := range unresolved {
			switch {
			case errors.Is(u.Err, errors.ErrTxNotFound):
				continue
			case u.Err != nil:
				return nil, errors.NewProcessingError("[branchOnlyTxs] error reading block ids of %s", chunk[i], u.Err)
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
							return nil, errors.NewProcessingError("[branchOnlyTxs] error checking whether block %d is on the best chain", id, err)
						}

						onBest[id] = on
					}

					if on {
						continue next
					}
				}
			}

			kept = append(kept, chunk[i])
		}
	}

	return kept, nil
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
	if b.bestTipResetReplayPending.Load() {
		return true
	}

	if b.bestTipPosKnown.Load() {
		if _, err := b.blockchainClient.GetState(ctx, model.BestTipLowWaterStateKey); err == nil {
			return true
		}

		if pos := b.bestTipPos.Load(); pos != nil {
			_, tipHeight := b.CurrentBlock()

			if headers, _, err := b.blockchainClient.BlocksBetterThan(ctx, *pos, nil, b.bestTipFloor(tipHeight), 1); err == nil && len(headers) > 0 {
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

// returnToRunning ends a block-processing state and then persists the replay position. The save
// is one write to the blockchain service, and a mining candidate asked for during a processing
// state is an empty one, so the save waits until block assembly is running again.
func (b *BlockAssembler) returnToRunning(ctx context.Context) {
	b.setCurrentRunningState(StateRunning)
	b.saveBestTipPosition(ctx)
}

// saveBestTipPosition persists the replay position when it moved since the last save. A failed
// or missed save only costs a restart replaying blocks block assembly already handled, each of
// which is then a no-op or a move that ends where it started, so it is logged and not returned.
func (b *BlockAssembler) saveBestTipPosition(ctx context.Context) {
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
	// Recovery of a reset that already discarded its pool is mandatory even
	// when ordinary lost-notification reconciliation is disabled.
	if b.bestTipResetReplayPending.Load() {
		return true
	}

	interval := b.settings.BlockAssembly.ReconcileInterval
	if interval <= 0 {
		return false
	}

	return now.Sub(time.Unix(0, b.lastBestTipReplay.Load())) >= interval
}

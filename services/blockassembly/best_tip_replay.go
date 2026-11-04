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
		if err = b.saveSkippedForkMark(ctx, true); err != nil {
			b.logger.Errorf("[BlockAssembler] replay of best tip changes failed %d times in a row, keeping its history: the mark of skipped fork transactions giving it up owes cannot be recorded: %v", failures, err)
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
// stamped unmined is left as it is. Unless it runs at start-up, before the unmined transactions
// are loaded, it reloads the pool when one it marked is not in it and is not conflicting, which
// the pool never loads. Outside start-up it runs only with block assembly on the best tip, since
// otherwise block assembly's own blocks read as fork blocks.
//
// The fork blocks are walked down from every chain tip but the active one, to the fork point or
// the floor (forkBlocksBelow, skippedForkMarkFloor). A tip may be an invalid block whose valid
// parent is the one that was briefly best, and a fork block block assembly never processed has
// no processed_at, so its tip reads valid-headers rather than valid-fork. Of the blocks walked,
// the valid ones with subtrees are read: block validation sets subtrees_set before it marks a
// block's transactions mined. One whose mined_set is not set yet is left for a later run, so a
// setTxMined still writing it cannot clear unmined_since again after the mark. A block is
// recorded once its transactions are marked and the pool reloaded, and is not read again.
//
// It returns an error when the run did not finish, for example on a fork block it cannot read.
// The run then stays pending, in memory and under SkippedForkMarkStateKey: every later pass runs
// it again, the idle reconcile wakes one after a back-off (noteSkippedForkMarkFailure), and the
// next start finishes it before it loads the unmined transactions, or fails.
func (b *BlockAssembler) markSkippedForkTxs(ctx context.Context, atStart bool) (err error) {
	b.skippedForkMarkPending = true

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

		// A lost subtree stops block assembly (runSkippedForkMark), so it is not retried.
		if _, gone := err.(*skippedForkSubtreeGoneError); !atStart && !gone {
			b.noteSkippedForkMarkFailure(time.Now())
			b.logger.Errorf("[BlockAssembler] mark of skipped fork transactions unfinished, retrying at %s: %v", b.skippedForkMarkRetryAt.Format(time.RFC3339), err)
		}
	}()

	best, bestMeta, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return errors.NewProcessingError("[markSkippedForkTxs] error getting the best block header", err)
	}

	// GetChainTips reads its rows before it computes branch lengths, so under SQLite's shared
	// cache it cannot deadlock against a concurrent writer.
	tips, err := b.blockchainClient.GetChainTips(ctx)
	if err != nil {
		return errors.NewProcessingError("[markSkippedForkTxs] error getting chain tips", err)
	}

	// Block assembly's tip must be the best block, and the active chain tip when one reads
	// active: the best tip may have moved between the two reads. A best block with a child,
	// such as an invalid block, is no chain tip, and then none reads active.
	if !atStart {
		tip, _ := b.CurrentBlock()
		if tip == nil || !tip.Hash().IsEqual(best.Hash()) {
			return errors.NewProcessingError("[markSkippedForkTxs] block assembly is not on the best tip %s", best.Hash())
		}

		for _, chainTip := range tips {
			if chainTip.Status == "active" && chainTip.Hash != tip.Hash().String() {
				return errors.NewProcessingError("[markSkippedForkTxs] block assembly is not on the best tip %s", chainTip.Hash)
			}
		}
	}

	floor := b.skippedForkMarkFloor(bestMeta.Height)
	walked := make(map[chainhash.Hash]struct{})

	var forkBlocks []blockHeaderWithMeta

	for _, chainTip := range tips {
		if chainTip.Status == "active" || chainTip.Height < floor {
			continue
		}

		tipHash, err := chainhash.NewHashFromStr(chainTip.Hash)
		if err != nil {
			return errors.NewProcessingError("[markSkippedForkTxs] error parsing chain tip hash %s", chainTip.Hash, err)
		}

		headers, metas, err := b.forkBlocksBelow(ctx, tipHash, chainTip.Height, chainTip.Branchlen, floor, best, walked)
		if err != nil {
			return errors.NewProcessingError("[markSkippedForkTxs] error getting the blocks of fork %s", tipHash, err)
		}

		for i, header := range headers {
			// A block without subtrees has not been through block validation, which marked
			// nothing mined in it; it is looked at again on a later run. An invalid block's
			// transactions are block validation's to unmark.
			if metas[i].Invalid || !metas[i].SubtreesSet {
				continue
			}

			if _, done := b.skippedForkBlocksMarked[*header.Hash()]; !done {
				forkBlocks = append(forkBlocks, blockHeaderWithMeta{header: header, meta: metas[i]})
			}
		}
	}

	if len(forkBlocks) == 0 {
		return b.reloadAfterSkippedForkMark(ctx, atStart)
	}

	if b.skippedForkBlocksMarked == nil {
		b.skippedForkBlocksMarked = make(map[chainhash.Hash]uint32)
	}

	for hash, height := range b.skippedForkBlocksMarked {
		if height < floor {
			delete(b.skippedForkBlocksMarked, hash)
		}
	}

	txMap := b.subtreeProcessor.GetCurrentTxMap()
	unfinished := 0
	marked := make(map[chainhash.Hash]uint32, len(forkBlocks))

	// Block by block, so a block that cannot be read does not hold up the others; it is not
	// recorded, and the next run reads it again.
	for _, fb := range forkBlocks {
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
				unfinished++

				continue
			}
		}

		hashes, err := b.blockTxHashes(ctx, fb.header)
		if err != nil {
			// Block validation finished the block, and the floor keeps its subtree files
			// retained, so a missing one is lost data, not something a later run can read.
			if errors.Is(err, errors.ErrNotFound) {
				return &skippedForkSubtreeGoneError{block: hash, err: err}
			}

			b.logger.Errorf("[BlockAssembler] error reading fork block %s to mark skipped fork transactions: %v", hash, err)
			unfinished++

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

			if !atStart && !u.Data.Conflicting && !txMap.Exists(u.Hash) {
				poolable[u.Hash] = struct{}{}
			}
		}

		if len(toMark) > 0 {
			onBest, err := b.markOffLongestChain(ctx, toMark)
			if err != nil {
				return errors.NewProcessingError("[markSkippedForkTxs] error marking %d transactions of fork block %s as not on the longest chain", len(toMark), hash, err)
			}

			for _, txHash := range onBest {
				delete(poolable, txHash)
			}

			if len(poolable) > 0 {
				b.skippedForkReloadPending = true
			}

			b.logger.Warnf("[BlockAssembler] followed the best header without the replay: marked %d transactions of fork block %s as not on the longest chain", len(toMark)-len(onBest), hash)

			// The block may have joined the best chain during the run and could leave it again
			// without block assembly moving onto it, so it is read again on a later run.
			if len(onBest) > 0 {
				continue
			}
		}

		marked[hash] = fb.meta.Height
	}

	if err = b.reloadAfterSkippedForkMark(ctx, atStart); err != nil {
		return err
	}

	for hash, height := range marked {
		b.skippedForkBlocksMarked[hash] = height
	}

	if unfinished > 0 {
		return errors.NewProcessingError("[markSkippedForkTxs] %d of %d fork blocks could not be marked yet", unfinished, len(forkBlocks))
	}

	return nil
}

// skippedForkSubtreeGoneError reports a fork block, owed a mark of skipped fork transactions,
// whose subtree the subtree store reports not found. Block validation finished the block and its
// subtree files are within their retention, so they should be there: nothing can mark the block's
// transactions, and block assembly stops rather than carry on without them.
type skippedForkSubtreeGoneError struct {
	block chainhash.Hash
	err   error
}

func (e *skippedForkSubtreeGoneError) Error() string {
	return fmt.Sprintf("fork block %s, owed a mark of skipped fork transactions, has lost its subtree: %v", e.block, e.err)
}

func (e *skippedForkSubtreeGoneError) Unwrap() error {
	return e.err
}

// runSkippedForkMark runs markSkippedForkTxs outside start-up. A run that does not finish stays
// pending and is retried, except when a fork block lost its subtree: that stops block assembly.
func (b *BlockAssembler) runSkippedForkMark(ctx context.Context) {
	// markSkippedForkTxs returns it unwrapped. errors.As stops at the first *errors.Error in a
	// chain, which this error wraps, so it would not find it.
	if err, gone := b.markSkippedForkTxs(ctx, false).(*skippedForkSubtreeGoneError); gone {
		b.logger.Errorf("[BlockAssembler] stopping block assembly: %v", err)

		select {
		case b.fatalCh <- err:
		default:
		}
	}
}

// Fatal returns a channel that receives an error block assembly cannot continue after. The
// service stops when one arrives.
func (b *BlockAssembler) Fatal() <-chan error {
	return b.fatalCh
}

// markOffLongestChain marks the transactions as not on the longest chain, then marks again as on
// it each one a block of the best chain contains by then, which it returns. The write is decided
// on a read of the best chain: a block that became the best tip since, and whose setTxMined
// cleared unmined_since before the write, would otherwise be left with it set while it is mined
// on the best chain, and moving forward onto that block does not mark it again.
func (b *BlockAssembler) markOffLongestChain(ctx context.Context, hashes []chainhash.Hash) ([]chainhash.Hash, error) {
	if err := b.utxoStore.MarkTransactionsOnLongestChain(ctx, hashes, false); err != nil {
		return nil, err
	}

	best, _, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, errors.NewProcessingError("[markOffLongestChain] error getting the best block header", err)
	}

	_, onBest, err := b.branchOnlyMeta(ctx, hashes, nil, best)
	if err != nil {
		return nil, err
	}

	if len(onBest) > 0 {
		if err = b.utxoStore.MarkTransactionsOnLongestChain(ctx, onBest, true); err != nil {
			return nil, err
		}
	}

	return onBest, nil
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

// reloadAfterSkippedForkMark reloads the pool on block assembly's tip, as the replay's restamp
// does, once markSkippedForkTxs marked a transaction the pool could hold and does not, such as
// one only a skipped fork block had. It is a full reset, so it runs only then. The need is kept
// until a reload succeeds, since a later run finds those transactions unmined already. At
// start-up the load of the unmined transactions that follows does it.
func (b *BlockAssembler) reloadAfterSkippedForkMark(ctx context.Context, atStart bool) error {
	if atStart || !b.skippedForkReloadPending {
		return nil
	}

	tip, height := b.CurrentBlock()

	// The reset marks tip history pending when the replay position is known, which keeps mining
	// candidates empty until a replay passes it. It stays on block assembly's own tip, so it
	// leaves no history behind.
	wasPending := b.bestTipResetReplayPending.Load()

	if err := b.resetToBlockAndRecord(ctx, tip, height, true); err != nil {
		return errors.NewProcessingError("[reloadAfterSkippedForkMark] error reloading the pool after marking skipped fork transactions", err)
	}

	b.bestTipResetReplayPending.Store(wasPending)
	b.skippedForkReloadPending = false

	return nil
}

// forkBlocksBelow returns the blocks of the fork ending at tipHash, from the tip down to the
// fork point, the first block that is an ancestor of best, or to the floor, with their meta.
// It reads the headers in pages of at most bestTipPageSize and blockchain_maxBlockHeadersPerRequest,
// so a fork longer than one read is read whole. branchLen is the branch length GetChainTips
// reported: the blocks within it are taken as off the best chain without asking, and it sizes the
// first read. The store caps the walk that computes it, and reports 0 when it could not compute
// one, so below it every block is checked. A block in walked, read from another tip, ends the
// walk, since the rest is that tip's; every block returned is added to walked. An error leaves
// the fork unread.
func (b *BlockAssembler) forkBlocksBelow(ctx context.Context, tipHash *chainhash.Hash, tipHeight, branchLen, floor uint32, best *model.BlockHeader, walked map[chainhash.Hash]struct{}) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	pageSize := uint32(bestTipPageSize)
	if limit := b.settings.BlockChain.MaxBlockHeadersPerRequest; limit > 0 && uint64(limit) < uint64(pageSize) {
		pageSize = uint32(limit) //nolint:gosec // limit is positive and below bestTipPageSize
	}

	// The fork and the block below it, which is on the best chain.
	count := pageSize
	if branchLen > 0 && branchLen < pageSize {
		count = branchLen + 1
	}

	var (
		headers []*model.BlockHeader
		metas   []*model.BlockHeaderMeta
	)

	next, height := tipHash, tipHeight

	for height >= floor {
		page, pageMetas, err := b.blockchainClient.GetBlockHeaders(ctx, next, uint64(min(count, height-floor+1)))
		if err != nil {
			return nil, nil, err
		}

		if len(page) == 0 || len(pageMetas) != len(page) {
			return nil, nil, errors.NewProcessingError("[forkBlocksBelow] read %d headers with %d metas from %s", len(page), len(pageMetas), next)
		}

		for i := range page {
			if pageMetas[i].Height < floor {
				return headers, metas, nil
			}

			if _, done := walked[*page[i].Hash()]; done {
				return headers, metas, nil
			}

			if uint32(len(headers)) >= branchLen { //nolint:gosec // a fork walk is far below 2^32 blocks
				onBest, err := b.blockchainClient.CheckBlockIsAncestorOfBlock(ctx, []uint32{pageMetas[i].ID}, best.Hash())
				if err != nil {
					return nil, nil, err
				}

				if onBest {
					return headers, metas, nil
				}
			}

			walked[*page[i].Hash()] = struct{}{}
			headers = append(headers, page[i])
			metas = append(metas, pageMetas[i])
		}

		last := pageMetas[len(pageMetas)-1]
		if last.Height == 0 {
			break
		}

		next, height, count = page[len(page)-1].HashPrevBlock, last.Height-1, pageSize
	}

	return headers, metas, nil
}

// saveSkippedForkMark records under SkippedForkMarkStateKey whether a run of markSkippedForkTxs
// is unfinished, so a restart finishes it. It writes only when that changes.
func (b *BlockAssembler) saveSkippedForkMark(ctx context.Context, pending bool) error {
	if b.skippedForkMarkSaved && b.skippedForkMarkSavedPending == pending {
		return nil
	}

	value := byte(0)
	if pending {
		value = 1
	}

	if err := b.blockchainClient.SetState(ctx, SkippedForkMarkStateKey, []byte{value}); err != nil {
		b.skippedForkMarkSaved = false
		return errors.NewStorageError("[saveSkippedForkMark] error saving state[%s]=%d", SkippedForkMarkStateKey, value, err)
	}

	b.skippedForkMarkSaved = true
	b.skippedForkMarkSavedPending = pending

	return nil
}

// finishSkippedForkMarkAtStart runs a mark of skipped fork transactions left pending before a
// restart, before the start loads the unmined transactions, so it finds those the mark makes
// unmined. Without a record there is nothing pending. A run that does not finish fails the start:
// the pool would be loaded without the transactions it owes.
func (b *BlockAssembler) finishSkippedForkMarkAtStart(ctx context.Context) error {
	data, err := b.blockchainClient.GetState(ctx, SkippedForkMarkStateKey)

	switch {
	case err != nil && isStateNotFound(err):
		return nil
	case err != nil:
		return errors.NewStorageError("[BlockAssembler] error reading state[%s]", SkippedForkMarkStateKey, err)
	case len(data) != 1 || data[0] == 0:
		b.skippedForkMarkSaved, b.skippedForkMarkSavedPending = true, false
		return nil
	}

	b.skippedForkMarkSaved, b.skippedForkMarkSavedPending = true, true

	b.logger.Warnf("[BlockAssembler] a mark of skipped fork transactions was left unfinished before the restart, finishing it")

	if err = b.markSkippedForkTxs(ctx, true); err != nil {
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
	wait := skippedForkMarkRetryMax
	if b.skippedForkMarkFailures < 16 {
		wait = min(skippedForkMarkRetryBase<<b.skippedForkMarkFailures, skippedForkMarkRetryMax)
	}

	b.skippedForkMarkFailures++
	b.skippedForkMarkRetryAt = now.Add(wait)
}

// noteSkippedForkMarkSuccess clears the back-off once a run finishes.
func (b *BlockAssembler) noteSkippedForkMarkSuccess() {
	b.skippedForkMarkFailures = 0
	b.skippedForkMarkRetryAt = time.Time{}
}

// skippedForkMarkDue reports whether an unfinished mark of skipped fork transactions is due to
// run again.
func (b *BlockAssembler) skippedForkMarkDue(now time.Time) bool {
	return b.skippedForkMarkPending && !now.Before(b.skippedForkMarkRetryAt)
}

// blockTxHashes returns the hashes of the transactions in a stored block, without its coinbase.
func (b *BlockAssembler) blockTxHashes(ctx context.Context, header *model.BlockHeader) ([]chainhash.Hash, error) {
	block, err := b.blockchainClient.GetBlock(ctx, header.Hash())
	if err != nil {
		return nil, errors.NewProcessingError("error getting block %s", header.Hash(), err)
	}

	subtrees, err := block.GetSubtrees(ctx, b.logger, b.subtreeStore, b.settings.Block.GetAndValidateSubtreesConcurrency)
	if err != nil {
		return nil, errors.NewProcessingError("error getting subtrees of block %s", header.Hash(), err)
	}

	hashes := make([]chainhash.Hash, 0, len(subtrees)*100)

	for _, st := range subtrees {
		for _, node := range st.Nodes {
			if !node.Hash.IsEqual(subtree.CoinbasePlaceholderHash) {
				hashes = append(hashes, node.Hash)
			}
		}
	}

	return hashes, nil
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
			return nil, nil, errors.NewProcessingError("[branchOnlyTxs] error reading block ids", err)
		}

	next:
		for i, u := range unresolved {
			switch {
			case errors.Is(u.Err, errors.ErrTxNotFound):
				continue
			case u.Err != nil:
				return nil, nil, errors.NewProcessingError("[branchOnlyTxs] error reading block ids of %s", chunk[i], u.Err)
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
							return nil, nil, errors.NewProcessingError("[branchOnlyTxs] error checking whether block %d is on the best chain", id, err)
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

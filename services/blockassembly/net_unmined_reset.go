package blockassembly

import (
	"context"
	"os"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"golang.org/x/sync/errgroup"
)

// resetNetUnminedChunkSize is how many net unmined hashes one existence check
// and one mark call handle, so neither ever sees the whole list. A variable so
// tests can force several chunks.
var resetNetUnminedChunkSize = 1_000_000

const (
	// resetNetUnminedMaxRangeEntries bounds the move-forward hashes held in memory
	// at once while Reset finds the net unmined transactions: about 1.5 GiB.
	resetNetUnminedMaxRangeEntries = 32_000_000

	// resetNetUnminedReadConcurrency bounds the move-forward subtrees read at once.
	resetNetUnminedReadConcurrency = 16
)

// markNetUnminedMoveBackTxs sets unmined_since on the transactions that are in a
// moveBack block and in no moveForward block, in moveBack order, one bounded
// chunk at a time.
//
// It is best effort, as the in-memory version it replaces was. When a block
// cannot be read, or a mark fails, it stops, leaves the rest unmarked and logs
// it; a cancelled context is returned. The transactions marked are always a
// prefix of the moveBack order. A moveBack block that cannot be read the first
// time is left out whole, as before. One that reads fine and then fails the
// second read stops the marking there, which the in-memory version, reading each
// block once, could not hit. Marked chunks stay marked, which is safe: marking
// is idempotent and each chunk holds only net unmined transactions.
func (b *BlockAssembler) markNetUnminedMoveBackTxs(ctx context.Context, moveBack, moveForward []blockWithMeta) error {
	var (
		backBlocks    []*model.Block
		forwardBlocks []*model.Block
		forwardTxs    uint64
	)

	// Invalid blocks are skipped: BlockValidation handles them through
	// setTxMinedStatus(unsetMined=true), which Reset waited for.
	for _, m := range moveBack {
		if !m.meta.Invalid {
			backBlocks = append(backBlocks, m.block)
		}
	}

	for _, m := range moveForward {
		if !m.meta.Invalid {
			forwardBlocks = append(forwardBlocks, m.block)
			forwardTxs += m.block.TransactionCount
		}
	}

	if len(backBlocks) == 0 {
		return nil
	}

	// The configured tx map directory is this node's own fast disk, so a scratch
	// directory in it that this run did not make is an earlier run's. The OS temp
	// directory may be shared with other processes, so nothing is swept there.
	scratchDir, sweepStale := os.TempDir(), false
	if dirs := b.settings.BlockAssembly.TxMapDirs; len(dirs) > 0 {
		scratchDir, sweepStale = dirs[0], true
	}

	var kept, pruned int

	res, err := netUnminedInOrder(ctx,
		len(backBlocks), b.netUnminedBlockStream(backBlocks, 1),
		len(forwardBlocks), b.netUnminedBlockStream(forwardBlocks, resetNetUnminedReadConcurrency),
		netUnminedParams{
			Dir:        scratchDir,
			RangeBits:  netUnminedRangeBits(forwardTxs, resetNetUnminedMaxRangeEntries),
			ChunkSize:  resetNetUnminedChunkSize,
			Beat:       b.heartbeat.BeatIfStarted,
			SweepStale: sweepStale,
		},
		func(chunk []chainhash.Hash) error {
			// Leave out moveBack txs the UTXO store has already pruned. A moveBack block can
			// sit deeper than the retention window, so its fully spent txs may be gone, and
			// the store treats marking a missing record as fatal. A pruned tx was mined
			// deeper than the retention window and has no record left to re-add to assembly;
			// its live children stay protected by the pruner's parent preservation.
			live, existsErr := b.dropPrunedTxs(ctx, chunk)

			switch {
			case existsErr == nil:
				pruned += len(chunk) - len(live)
				chunk = live
			case ctx.Err() != nil:
				return existsErr
			default:
				// Cannot tell pruned from live: mark them all, as before the lookup existed.
				// Leaving them out would strand live txs flagged mined with their block off
				// the chain; a pruned tx among them still hits the store's fatal, as before.
				b.logger.Errorf("[BlockAssembler][Reset] error checking moveBack transactions exist, marking all %d: %v", len(chunk), existsErr)
			}

			if len(chunk) == 0 {
				return nil
			}

			// Proves the last moveBack read returned; the mark is one store call.
			b.heartbeat.BeatIfStarted()

			if markErr := b.utxoStore.MarkTransactionsOnLongestChain(ctx, chunk, false); markErr != nil {
				if ctx.Err() != nil {
					return markErr
				}

				b.logger.Errorf("[BlockAssembler][Reset] error marking moveBack transactions as unmined: %v", markErr)

				// Stop here. The chunks already marked are a prefix of the moveBack
				// order; marking later chunks would leave a hole in it that nothing
				// fills in, because a reconcile only runs another reset when the chain moves.
				return markErr
			}

			kept += len(chunk)

			return nil
		})
	if err != nil {
		if ctx.Err() != nil {
			return err
		}

		if res.ForwardIncomplete {
			// Without every moveForward block's txs the filter would mark txs that ARE in
			// the new main chain, and BlockValidation's background job (which clears
			// unmined_since for them) races with that write, so the corruption could
			// persist for a full reconcile cycle. Skip the marker entirely; the next
			// reconcile usually succeeds.
			b.logger.Warnf("[BlockAssembler][Reset] error reading a moveForward block: %v (skipping moveBack unmined_since marker for this reset cycle)", err)

			return nil
		}

		b.logger.Warnf("[BlockAssembler][Reset] stopped marking the net unmined moveBack transactions after %d: %v (the rest stay unmarked until a later reset)", kept, err)

		return nil
	}

	for _, i := range res.SkippedBackBlocks {
		b.logger.Warnf("[BlockAssembler][Reset] error getting subtrees for moveBack block %s (skipped)", backBlocks[i].Hash().String())
	}

	b.logger.Infof("[BlockAssembler][Reset] marked %d net unmined transactions (moveBack minus moveForward): %d moveBack, %d moveForward, %d pruned left out", kept, res.Back, res.Forward, pruned)

	return nil
}

// netUnminedBlockStream streams the transactions of blocks one subtree at a time, so a
// block's subtrees are never all in memory, leaving out the coinbase placeholder.
// With concurrency above 1 the subtrees of a block are read in parallel and
// visit is called from several goroutines, which is only right for a stream
// whose order does not matter.
func (b *BlockAssembler) netUnminedBlockStream(blocks []*model.Block, concurrency int) txHashStream {
	return func(ctx context.Context, i int, visit func([]chainhash.Hash) error) error {
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(max(concurrency, 1))

		for _, subtreeHash := range blocks[i].Subtrees {
			if subtreeHash == nil {
				continue
			}

			if gctx.Err() != nil {
				break
			}

			if concurrency <= 1 {
				if err := b.visitSubtreeTxHashes(gctx, subtreeHash, visit); err != nil {
					return err
				}

				continue
			}

			g.Go(func() error { return b.visitSubtreeTxHashes(gctx, subtreeHash, visit) })
		}

		if err := g.Wait(); err != nil {
			return err
		}

		// A cancelled parent stops the loop above without any visit failing, which
		// would otherwise look like a block with fewer transactions.
		return ctx.Err()
	}
}

func (b *BlockAssembler) visitSubtreeTxHashes(ctx context.Context, subtreeHash *chainhash.Hash, visit func([]chainhash.Hash) error) error {
	reader, err := b.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	if err != nil {
		reader, err = b.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
		if err != nil {
			return errors.NewServiceError("[netUnmined] error getting subtree %s", subtreeHash.String(), err)
		}
	}

	st := &subtreepkg.Subtree{}
	err = st.DeserializeFromReader(reader)
	_ = reader.Close()

	if err != nil {
		return errors.NewProcessingError("[netUnmined] error deserializing subtree %s", subtreeHash.String(), err)
	}

	hashes := make([]chainhash.Hash, 0, len(st.Nodes))

	for _, node := range st.Nodes {
		if !node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			hashes = append(hashes, node.Hash)
		}
	}

	return visit(hashes)
}

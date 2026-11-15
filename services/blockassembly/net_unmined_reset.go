package blockassembly

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/util/retry"
	"golang.org/x/sync/errgroup"
)

var (
	// resetNetUnminedChunkSize is how many net unmined hashes one existence check
	// and one mark call handle, so neither ever sees the whole list. A variable so
	// tests can force several chunks.
	resetNetUnminedChunkSize = 1_000_000

	// resetNetUnminedMaxForwardInMemory bounds the move-forward hashes held in
	// memory at once while Reset finds the net unmined transactions, about 2.5 GiB.
	// A larger move-forward set is split into ranges of at most this many on disk.
	// A variable so tests can force that.
	resetNetUnminedMaxForwardInMemory uint64 = 32_000_000

	// netUnminedSubtreeRetryBackoff is the first wait between tries to open a
	// subtree. A variable so tests need not wait.
	netUnminedSubtreeRetryBackoff = 100 * time.Millisecond
)

const (
	// resetNetUnminedReadConcurrency bounds the subtrees of a block read at once.
	resetNetUnminedReadConcurrency = 16

	// netUnminedSubtreeRetries is how often a failed open of a subtree is tried
	// again.
	netUnminedSubtreeRetries = 3
)

// markNetUnminedMoveBackTxs sets unmined_since on the transactions that are in a
// moveBack block and in no moveForward block, one bounded chunk at a time, in no
// particular order: the store stamps every one of them the same.
//
// A block that cannot be read does not fail the reset, as before: an unreadable
// moveForward block skips the marker, and an unreadable moveBack block is marked
// as far as it was read. Subtree files of a fork expire, so a reset that insisted
// on them could never finish. Unreadable means a subtree file that is gone, or
// that is not the subtree its key names.
//
// Any other failure is returned and fails the reset, before it has rotated the
// tx map or moved the tip: a failed read or write of the UTXO store (the
// existence lookup, the mark, the check against the best chain), a subtree store
// that fails to answer, or a failed scratch file. A reset that carried on would leave moveBack transactions
// flagged mined on a block that is off the chain, and no later reset moves back
// from these blocks again. A reset made for a change of the best tip, or for a
// tx map storage error, is run again over the same blocks; one an operator asked
// for reports the error and is not. Chunks marked before the failure stay
// marked, which is safe: each holds only net unmined transactions and marking is
// idempotent.
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

	var kept, pruned, minedMeanwhile int

	res, err := netUnmined(ctx,
		len(backBlocks), b.netUnminedBlockStream(backBlocks),
		len(forwardBlocks), b.netUnminedBlockStream(forwardBlocks),
		netUnminedParams{
			Dir: scratchDir,
			// Whether the forward set fits in memory, and into how many ranges it is
			// split when it does not: this is what bounds the reset's memory.
			ForwardCount:       forwardTxs,
			MaxForwardInMemory: resetNetUnminedMaxForwardInMemory,
			ChunkSize:          resetNetUnminedChunkSize,
			Beat:               b.heartbeat.BeatIfStarted,
			SweepStale:         sweepStale,
		},
		func(chunk []chainhash.Hash) error {
			// Leave out moveBack txs the UTXO store has already pruned. A moveBack block can
			// sit deeper than the retention window, so its fully spent txs may be gone, and
			// the store treats marking a missing record as fatal. A pruned tx was mined
			// deeper than the retention window and has no record left to re-add to assembly;
			// its live children stay protected by the pruner's parent preservation.
			live, existsErr := b.dropPrunedTxs(ctx, chunk)
			if existsErr != nil {
				// Cannot tell pruned from live. Marking them all would hit the store's
				// fatal for a pruned one; leaving them out would strand live ones.
				return errors.NewProcessingError("[Reset] error checking %d moveBack transactions exist", len(chunk), existsErr)
			}

			pruned += len(chunk) - len(live)
			chunk = live

			if len(chunk) == 0 {
				return nil
			}

			// Proves the last read returned; the mark is one store call.
			b.heartbeat.BeatIfStarted()

			// The mark was decided on the moveForward blocks as they were when the
			// reset began. A block that extended the target since may have mined one
			// of these, and its setTxMined may have cleared unmined_since before this
			// write: neither store looks at the block ids when it stamps. So each
			// chunk is checked against the best chain after its mark, and a chunk
			// whose check failed is kept for a later one.
			onBest, markErr := b.markOffLongestChain(ctx, chunk)
			if markErr != nil {
				return errors.NewProcessingError("[Reset] error marking %d moveBack transactions as unmined", len(chunk), markErr)
			}

			kept += len(chunk) - len(onBest)
			minedMeanwhile += len(onBest)

			return nil
		})

	for _, skipped := range res.SkippedBackBlocks {
		b.logger.Warnf("[BlockAssembler][Reset] error reading moveBack block %s, marking only what was read of it: %v", backBlocks[skipped.Block].Hash().String(), skipped.Err)
	}

	if res.ScratchLeft != nil {
		b.logger.Warnf("[BlockAssembler][Reset] %v", res.ScratchLeft)
	}

	if err != nil {
		if ctx.Err() != nil {
			return err
		}

		if res.ForwardIncomplete {
			// Without every moveForward block's txs the filter would mark txs that ARE in
			// the new main chain. Skip the marker: the net unmined moveBack transactions
			// stay flagged mined, and this reset does not repair that.
			b.logger.Warnf("[BlockAssembler][Reset] error reading a moveForward block, skipping the moveBack unmined_since marker: %v", err)

			return nil
		}

		return errors.NewProcessingError("[Reset] error marking the net unmined moveBack transactions, %d marked before it", kept, err)
	}

	b.logger.Infof("[BlockAssembler][Reset] marked %d net unmined transactions (moveBack minus moveForward): %d moveBack, %d moveForward, %d pruned left out, %d mined on the best chain meanwhile", kept, res.Back, res.Forward, pruned, minedMeanwhile)

	return nil
}

// netUnminedBlockStream streams the transactions of blocks one subtree at a time, so a
// block's subtrees are never all in memory, leaving out the coinbase placeholder.
// The subtrees of a block are read in parallel and visit is called from several
// goroutines, in no particular order.
//
// A subtree whose file is gone, or is not the subtree its key names, makes the
// block unreadable; the block's other subtrees are still read. A subtree store
// that fails to answer is an error, as a failing UTXO store is.
func (b *BlockAssembler) netUnminedBlockStream(blocks []*model.Block) txHashStream {
	return func(ctx context.Context, i int, visit func([]chainhash.Hash) error) (error, error) {
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(resetNetUnminedReadConcurrency)

		var (
			mu         sync.Mutex
			unreadable error
		)

		for _, subtreeHash := range blocks[i].Subtrees {
			if subtreeHash == nil {
				continue
			}

			if gctx.Err() != nil {
				break
			}

			g.Go(func() error {
				hashes, bad, err := b.subtreeTxHashes(gctx, subtreeHash)
				if err != nil {
					return err
				}

				if bad != nil {
					mu.Lock()
					if unreadable == nil {
						unreadable = bad
					}
					mu.Unlock()

					return nil
				}

				return visit(hashes)
			})
		}

		if err := g.Wait(); err != nil {
			return nil, err
		}

		// A cancelled parent stops the loop above without any read failing, which
		// would otherwise look like a block with fewer transactions.
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		return unreadable, nil
	}
}

// openSubtree opens a subtree file, or the file still to be checked when there is
// no subtree file: that one exists when the subtree was downloaded and could not
// be validated in full, and toCheck reports it. The store can fail an open while
// a subtree moves to permanent storage, so a failed open is tried again, as
// Block.GetSubtrees does. A file that is not there is not: the files of a fork
// expire, and waiting for each would hold a reset over many such blocks.
func (b *BlockAssembler) openSubtree(ctx context.Context, subtreeHash *chainhash.Hash) (reader io.ReadCloser, toCheck bool, err error) {
	var notFound error

	reader, err = retry.Retry(ctx, b.logger, func() (io.ReadCloser, error) {
		r, openErr := b.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
		if openErr == nil || !errors.Is(openErr, errors.ErrNotFound) {
			return r, openErr
		}

		if r, toCheckErr := b.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck); toCheckErr == nil {
			toCheck = true

			return r, nil
		}

		// Not there either: report the subtree file as not found, without a retry.
		notFound = openErr

		return nil, nil
	},
		retry.WithMessage("[netUnmined] failed to get subtree "+subtreeHash.String()),
		retry.WithRetryCount(netUnminedSubtreeRetries),
		retry.WithBackoffDurationType(netUnminedSubtreeRetryBackoff),
	)
	if err == nil && notFound != nil {
		err = notFound
	}

	return reader, toCheck, err
}

// subtreeTxHashes reads the transaction hashes of a subtree. unreadable says the
// subtree cannot be had from this store however often it is asked: its file is
// not there, cannot be parsed, or is another subtree's.
func (b *BlockAssembler) subtreeTxHashes(ctx context.Context, subtreeHash *chainhash.Hash) (hashes []chainhash.Hash, unreadable, err error) {
	reader, toCheck, err := b.openSubtree(ctx, subtreeHash)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			return nil, errors.NewProcessingError("[netUnmined] subtree %s is not in the subtree store", subtreeHash.String(), err), nil
		}

		return nil, nil, errors.NewServiceError("[netUnmined] error getting subtree %s", subtreeHash.String(), err)
	}

	st := &subtreepkg.Subtree{}
	err = st.DeserializeFromReader(reader)
	_ = reader.Close()

	if err != nil {
		// A read cut short by a cancelled context says nothing about the file.
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}

		return nil, errors.NewProcessingError("[netUnmined] error deserializing subtree %s", subtreeHash.String(), err), nil
	}

	// The file under this key must be this subtree: another one's transactions
	// are not this block's. A subtree file was validated when it was written, so
	// its own claim is compared. The file still to be checked never was, so its
	// root is computed from its nodes.
	matchesKey := model.ValidateSubtreeMatchesKey
	if toCheck {
		matchesKey = model.ValidateSubtreeNodesMatchKey
	}

	if err = matchesKey(st, subtreeHash); err != nil {
		return nil, errors.NewProcessingError("[netUnmined] subtree %s", subtreeHash.String(), err), nil
	}

	hashes = make([]chainhash.Hash, 0, len(st.Nodes))

	for _, node := range st.Nodes {
		if !node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			hashes = append(hashes, node.Hash)
		}
	}

	return hashes, nil, nil
}

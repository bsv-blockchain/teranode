package blockvalidation

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/util/lane"
	"golang.org/x/sync/errgroup"
)

// The block gate for a peer's block, at utxostore_laneMigration shadow (lanes Phase 1). Before the
// block is stored, block validation reads the lane bitmaps subtree validation saved beside each of
// the block's subtrees, merges them lane by lane into the block's map rows, and hands the rows to
// AddBlock, which inserts them in the block's own database transaction. Nothing reads the rows
// yet, so nothing here can stop a block: a subtree without usable bitmaps means the block is
// stored without rows, and a count that does not match is counted and logged while the rows still
// go with the block.

// laneBitmapReadConcurrency bounds the subtree bitmap reads of one block.
const laneBitmapReadConcurrency = 16

// laneRows builds block's lane map rows from its subtrees' lane bitmaps, or returns nil when lanes
// are off or the rows cannot be built, which is counted and logged.
func (u *BlockValidation) laneRows(ctx context.Context, block *model.Block) []lane.MapRow {
	if u.settings == nil || !u.settings.UtxoStore.LaneMigration.Stamps() || u.subtreeStore == nil || len(block.Subtrees) == 0 {
		return nil
	}

	start := time.Now()

	bitmaps, missing, unreadable := u.readSubtreeLaneBitmaps(ctx, block)

	if missing > 0 || unreadable > 0 {
		lane.CountMissingBitmaps(lane.MissingBitmapsAbsent, missing)
		lane.CountMissingBitmaps(lane.MissingBitmapsUnreadable, unreadable)
		u.logger.Warnf("[laneRows][%s] storing the block without lane rows: %d of its %d subtrees have no lane bitmaps and %d have unreadable ones", block.Hash().String(), missing, len(block.Subtrees), unreadable)

		return nil
	}

	rows, err := lane.BuildBlockRows(bitmaps)
	if err != nil {
		lane.CountRowInsertFailure()
		u.logger.Warnf("[laneRows][%s] storing the block without lane rows, which could not be built: %v", block.Hash().String(), err)

		return nil
	}

	lane.ObserveGate(lane.GateStageBuild, time.Since(start))
	lane.ObserveBlockRows(rows)
	u.checkLaneRowCount(block, rows.Members)

	return rows.Rows
}

// checkLaneRowCount compares the members of a block's rows with its transactions less the
// coinbase, which takes no position from a lane, and counts and logs a difference.
func (u *BlockValidation) checkLaneRowCount(block *model.Block, members uint64) {
	var want uint64
	if block.TransactionCount > 0 {
		want = block.TransactionCount - 1
	}

	if members == want {
		return
	}

	direction := lane.MismatchFewer
	if members > want {
		direction = lane.MismatchMore
	}

	lane.CountRowMismatch(direction)
	u.logger.Warnf("[laneRows][%s] lane rows hold %d members, the block has %d transactions besides its coinbase", block.Hash().String(), members, want)
}

// readSubtreeLaneBitmaps reads and decodes the lane bitmaps of every subtree of block, in subtree
// order, and counts the subtrees whose file is absent and those whose file could not be read or
// decoded, or belongs to another subtree.
func (u *BlockValidation) readSubtreeLaneBitmaps(ctx context.Context, block *model.Block) ([]lane.LaneBitmaps, int, int) {
	bitmaps := make([]lane.LaneBitmaps, len(block.Subtrees))
	outcome := make([]int, len(block.Subtrees)) // 0 read, 1 absent, 2 unreadable

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(laneBitmapReadConcurrency)

	for i, hash := range block.Subtrees {
		g.Go(func() error {
			data, err := u.subtreeStore.Get(gCtx, hash[:], fileformat.FileTypeSubtreeLaneBitmaps)
			if err != nil {
				outcome[i] = 2
				if errors.Is(err, errors.ErrNotFound) {
					outcome[i] = 1
				}

				return nil
			}

			s, err := lane.DecodeSubtreeBitmaps(data)
			if err != nil || s.SubtreeHash != *hash {
				outcome[i] = 2
				return nil
			}

			bitmaps[i] = s.Lanes

			return nil
		})
	}

	_ = g.Wait()

	var missing, unreadable int

	for _, o := range outcome {
		switch o {
		case 1:
			missing++
		case 2:
			unreadable++
		}
	}

	return bitmaps, missing, unreadable
}

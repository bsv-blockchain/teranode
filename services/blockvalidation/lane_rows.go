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

// laneBitmapReadTimeout bounds how long the reads of one block's lane bitmaps files may hold up
// storing the block. The reads sit on the block-store path, and at shadow a slow or stalled
// subtree store must not delay a block: past the deadline the block is stored without rows. A
// variable so a test can shorten it.
var laneBitmapReadTimeout = 5 * time.Second

// laneRows builds block's lane map rows from its subtrees' lane bitmaps, or returns nil when lanes
// are off or the rows cannot be built, which is counted and logged.
func (u *BlockValidation) laneRows(ctx context.Context, block *model.Block) []lane.MapRow {
	if u.settings == nil || !u.settings.UtxoStore.LaneMigration.Stamps() || u.subtreeStore == nil || len(block.Subtrees) == 0 {
		return nil
	}

	start := time.Now()

	bitmaps, missing, unreadable, timedOut := u.readSubtreeLaneBitmaps(ctx, block)

	if missing > 0 || unreadable > 0 || timedOut > 0 {
		lane.CountMissingBitmaps(lane.MissingBitmapsAbsent, missing)
		lane.CountMissingBitmaps(lane.MissingBitmapsUnreadable, unreadable)
		lane.CountMissingBitmaps(lane.MissingBitmapsTimedOut, timedOut)
		u.logger.Warnf("[laneRows][%s] storing the block without lane rows: of its %d subtrees, %d have no lane bitmaps, %d have unreadable ones and %d were not read in time", block.Hash().String(), len(block.Subtrees), missing, unreadable, timedOut)

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
// order, within laneBitmapReadTimeout. It counts the subtrees whose file is absent, those whose
// file could not be read or decoded or belongs to another subtree, and those not read because the
// deadline passed or ctx was cancelled.
func (u *BlockValidation) readSubtreeLaneBitmaps(ctx context.Context, block *model.Block) ([]lane.LaneBitmaps, int, int, int) {
	bitmaps := make([]lane.LaneBitmaps, len(block.Subtrees))
	outcome := make([]int, len(block.Subtrees)) // 0 read, 1 absent, 2 unreadable, 3 timed out

	readCtx, cancel := context.WithTimeout(ctx, laneBitmapReadTimeout)
	defer cancel()

	g, gCtx := errgroup.WithContext(readCtx)
	g.SetLimit(laneBitmapReadConcurrency)

	for i, hash := range block.Subtrees {
		g.Go(func() error {
			if gCtx.Err() != nil {
				outcome[i] = 3
				return nil
			}

			data, err := u.subtreeStore.Get(gCtx, hash[:], fileformat.FileTypeSubtreeLaneBitmaps)
			if err != nil {
				switch {
				case gCtx.Err() != nil:
					outcome[i] = 3
				case errors.Is(err, errors.ErrNotFound):
					outcome[i] = 1
				default:
					outcome[i] = 2
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

	var missing, unreadable, timedOut int

	for _, o := range outcome {
		switch o {
		case 1:
			missing++
		case 2:
			unreadable++
		case 3:
			timedOut++
		}
	}

	return bitmaps, missing, unreadable, timedOut
}

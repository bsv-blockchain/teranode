package blockvalidation

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// The block gate for a peer's block, at utxostore_laneMigration shadow (lanes Phase 1). Before the
// block is stored, block validation reads the lane bitmaps subtree validation saved beside each of
// the block's subtrees, merges them lane by lane into the block's map rows, and hands the rows to
// AddBlock, which inserts them in the block's own database transaction. Readers load the rows at
// shadow and do not act on them, so nothing here can stop a block: a subtree without usable
// bitmaps means the block is stored without rows, and a count that does not match is counted and
// logged while whatever rows were built still go with the block. A block whose bitmaps files were
// all read but hold no position has no rows to go, and counts as fewer.

// laneBitmapReadConcurrency bounds the subtree bitmap reads in flight across every block, not per
// block, and a read a block stopped waiting for keeps its slot until it returns. The file subtree
// store's reads share one process-wide pool of read permits with the block store
// (filestore_read_concurrency), and a read stuck on a hung disk keeps its permit until the disk
// answers. So however many blocks give up on stuck reads, those reads hold at most this many
// permits: a later block finds no free slot, times out and is stored without rows.
const laneBitmapReadConcurrency = 16

// laneBitmapReadTimeout bounds how long the reads of one block's lane bitmaps files may hold up
// storing the block. The reads sit on the block-store path, and at shadow a slow or stalled
// subtree store must not delay a block: past the deadline the block is stored without rows, even
// if a read is still stuck in the store (readSubtreeLaneBitmaps). A variable so a test can shorten
// it.
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
		lane.CountMissingBitmaps(lane.BlockPeer, lane.MissingBitmapsAbsent, missing)
		lane.CountMissingBitmaps(lane.BlockPeer, lane.MissingBitmapsUnreadable, unreadable)
		lane.CountMissingBitmaps(lane.BlockPeer, lane.MissingBitmapsTimedOut, timedOut)
		u.logger.Warnf("[laneRows][%s] storing the block without lane rows: of its %d subtrees, %d have no lane bitmaps, %d have unreadable ones and %d were not read in time", block.Hash().String(), len(block.Subtrees), missing, unreadable, timedOut)

		return nil
	}

	rows, err := lane.BuildBlockRows(bitmaps)
	if err != nil {
		lane.CountRowInsertFailure(lane.BlockPeer)
		u.logger.Warnf("[laneRows][%s] storing the block without lane rows, which could not be built: %v", block.Hash().String(), err)

		return nil
	}

	lane.ObserveGate(lane.BlockPeer, lane.GateStageBuild, time.Since(start))
	lane.ObserveBlockRows(lane.BlockPeer, rows)
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

	lane.CountRowMismatch(lane.BlockPeer, direction)
	u.logger.Warnf("[laneRows][%s] lane rows hold %d members, the block has %d transactions besides its coinbase", block.Hash().String(), members, want)
}

// Outcomes of one subtree's lane bitmaps read.
const (
	bitmapsRead = iota
	bitmapsAbsent
	bitmapsUnreadable
	bitmapsTimedOut
)

// subtreeBitmapsRead is one subtree's read: its index in the block, its outcome and, when read,
// its lanes.
type subtreeBitmapsRead struct {
	index   int
	outcome int
	lanes   lane.LaneBitmaps
}

// readSubtreeLaneBitmaps reads and decodes the lane bitmaps of every subtree of block, in subtree
// order, and returns within laneBitmapReadTimeout, or when ctx ends, whether or not every read has.
// It counts the subtrees whose file is absent, those whose file could not be read or decoded or
// belongs to another subtree, and those not read by the deadline.
//
// It does not wait for a read past the deadline, because the file subtree store does not stop a
// read for its context once the read holds its read permit: a hung disk would otherwise hold the
// block for as long as the disk does. Each read reports on a channel with room for every subtree,
// so a read that finishes late never blocks, and one that never finishes keeps only its own
// goroutine, its store's read permit and its slot of the shared pool until the disk answers.
func (u *BlockValidation) readSubtreeLaneBitmaps(ctx context.Context, block *model.Block) ([]lane.LaneBitmaps, int, int, int) {
	n := len(block.Subtrees)

	readCtx, cancel := context.WithTimeout(ctx, laneBitmapReadTimeout)
	defer cancel()

	results := make(chan subtreeBitmapsRead, n)

	go u.dispatchSubtreeLaneBitmapReads(readCtx, block, results)

	bitmaps := make([]lane.LaneBitmaps, n)
	outcome := make([]int, n)
	reported := make([]bool, n)

	for got := 0; got < n; {
		select {
		case r := <-results:
			reported[r.index] = true
			outcome[r.index] = r.outcome
			bitmaps[r.index] = r.lanes
			got++
		case <-readCtx.Done():
			for i := range reported {
				if !reported[i] {
					outcome[i] = bitmapsTimedOut
				}
			}

			got = n
		}
	}

	var missing, unreadable, timedOut int

	for i, o := range outcome {
		switch o {
		case bitmapsAbsent:
			missing++
		case bitmapsUnreadable:
			unreadable++
		case bitmapsTimedOut:
			timedOut++
			bitmaps[i] = nil
		}
	}

	return bitmaps, missing, unreadable, timedOut
}

// laneBitmapReadSlots is the slot pool every block's bitmap reads share; see
// laneBitmapReadConcurrency.
func (u *BlockValidation) laneBitmapReadSlots() chan struct{} {
	u.laneBitmapSlotsOnce.Do(func() { u.laneBitmapSlots = make(chan struct{}, laneBitmapReadConcurrency) })

	return u.laneBitmapSlots
}

// dispatchSubtreeLaneBitmapReads starts one read per subtree of block, each once it holds a slot
// of the pool every block shares, and each read reports once on results and frees its slot when
// it returns. It stops when readCtx ends, and no read starts after that: readSubtreeLaneBitmaps,
// which stops waiting at the same moment, counts every subtree it has no report for as timed out.
func (u *BlockValidation) dispatchSubtreeLaneBitmapReads(readCtx context.Context, block *model.Block, results chan<- subtreeBitmapsRead) {
	slots := u.laneBitmapReadSlots()

	for i, hash := range block.Subtrees {
		select {
		case slots <- struct{}{}:
		case <-readCtx.Done():
			return
		}

		// A slot that came free as the deadline passed can win the select above, which picks at
		// random between ready cases. Check again, so no read starts after the deadline, and hand
		// the slot back: only the read it was taken for frees it, and every block shares the pool.
		if readCtx.Err() != nil {
			<-slots

			return
		}

		go func() {
			defer func() { <-slots }()

			results <- u.readSubtreeLaneBitmapsFile(readCtx, i, hash)
		}()
	}
}

// readSubtreeLaneBitmapsFile reads and decodes the lane bitmaps file of the subtree at index i.
func (u *BlockValidation) readSubtreeLaneBitmapsFile(readCtx context.Context, i int, hash *chainhash.Hash) subtreeBitmapsRead {
	data, err := u.subtreeStore.Get(readCtx, hash[:], fileformat.FileTypeSubtreeLaneBitmaps)
	if err != nil {
		switch {
		case readCtx.Err() != nil:
			return subtreeBitmapsRead{index: i, outcome: bitmapsTimedOut}
		case errors.Is(err, errors.ErrNotFound):
			return subtreeBitmapsRead{index: i, outcome: bitmapsAbsent}
		default:
			return subtreeBitmapsRead{index: i, outcome: bitmapsUnreadable}
		}
	}

	s, err := lane.DecodeSubtreeBitmaps(data)
	if err != nil || s.SubtreeHash != *hash {
		return subtreeBitmapsRead{index: i, outcome: bitmapsUnreadable}
	}

	return subtreeBitmapsRead{index: i, outcome: bitmapsRead, lanes: s.Lanes}
}

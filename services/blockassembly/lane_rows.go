package blockassembly

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// The lane gate for the blocks this node mines, at utxostore_laneMigration shadow (lanes Phase 1).
// The validator hands block assembly each transaction's own lane position, which the pool keeps
// with its inpoints. When block assembly stores a subtree it builds the subtree's lane bitmaps from
// those positions, in the pass that builds the subtree meta, writes them as the subtree's lane
// bitmaps file before the subtree file, as subtree validation does for a peer's subtree, and keeps
// them in memory by subtree hash. When a block it mined is stored, its rows are merged from the
// kept bitmaps of the block's own subtrees, the candidate that was cut, never from the pool, which
// by then also holds transactions that arrived after the cut. Nothing reads the rows yet, so
// nothing here can stop a block or a transaction: a transaction without a position is counted and
// mined, and a subtree without bitmaps means the block is stored without rows.

// laneBitmapsService labels the transactions block assembly leaves out of a subtree's lane
// bitmaps (teranode_lane_txs_without_position_total).
const laneBitmapsService = "blockassembly"

// ownLaneBitmapsKeepBlocks is how many blocks the kept bitmaps of a subtree outlive the height
// they were last stored at. A candidate is cut from subtrees sealed since the last block, some of
// them possibly still being stored (see ownLaneBitmapsWait): when a
// block moves the pool on, every transaction left is built into new subtrees, which are stored and
// kept again, and a candidate cut before that block is stale. So an entry older than this serves
// nothing, and it is dropped when the next one is kept. This bounds the memory to the subtrees
// stored at the last ownLaneBitmapsKeepBlocks+1 heights, three generations of the pool's
// subtrees: about 0.5 KB per million-transaction subtree whose lanes are contiguous runs, which is
// how block assembly fills its subtrees from the queue.
const ownLaneBitmapsKeepBlocks = 2

// ownLaneBitmapsWait bounds how long storing our block waits for the kept bitmaps of a subtree
// whose storage is still running. A sealed subtree joins the candidate at once and its storage
// worker keeps its bitmaps tens of milliseconds later, so a block mined right after a seal, or
// right after a block rebuilds the pool's subtrees, would otherwise find them missing. A variable
// so a test can shorten it.
var ownLaneBitmapsWait = time.Second

// ownLaneBitmaps keeps the lane bitmaps of the subtrees block assembly stored, by subtree root
// hash, for the rows of the blocks it mines. The zero value is ready to use.
type ownLaneBitmaps struct {
	mu        sync.Mutex
	bySubtree map[chainhash.Hash]keptLaneBitmaps
	// pending holds the subtrees handed to the storage workers whose storage has not finished.
	pending map[chainhash.Hash]struct{}
	// changed is closed, and replaced, whenever a subtree's bitmaps are kept or its storage ends.
	changed chan struct{}
}

// keptLaneBitmaps is one subtree's bitmaps and the block height it was last stored at.
type keptLaneBitmaps struct {
	lanes  lane.LaneBitmaps
	height uint32
}

// put keeps the bitmaps of subtree root, stored at height, and drops entries last stored more
// than ownLaneBitmapsKeepBlocks blocks below it.
func (o *ownLaneBitmaps) put(root chainhash.Hash, lanes lane.LaneBitmaps, height uint32) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.bySubtree == nil {
		o.bySubtree = make(map[chainhash.Hash]keptLaneBitmaps)
	}

	o.bySubtree[root] = keptLaneBitmaps{lanes: lanes, height: height}
	delete(o.pending, root)
	o.broadcastLocked()

	if height <= ownLaneBitmapsKeepBlocks {
		return
	}

	for hash, kept := range o.bySubtree {
		if kept.height < height-ownLaneBitmapsKeepBlocks {
			delete(o.bySubtree, hash)
		}
	}
}

// touch marks the kept bitmaps of subtree root as stored again at height, and reports whether
// any are kept.
func (o *ownLaneBitmaps) touch(root chainhash.Hash, height uint32) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	kept, ok := o.bySubtree[root]
	if ok && kept.height < height {
		kept.height = height
		o.bySubtree[root] = kept
	}

	if ok {
		delete(o.pending, root)
		o.broadcastLocked()
	}

	return ok
}

// expect marks subtree root as handed to a storage worker, so a block that lists it before its
// bitmaps are kept waits for them.
func (o *ownLaneBitmaps) expect(root chainhash.Hash) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, kept := o.bySubtree[root]; kept {
		return
	}

	if o.pending == nil {
		o.pending = make(map[chainhash.Hash]struct{})
	}

	o.pending[root] = struct{}{}

	// a waiter that counted this subtree before its storage began counts it again
	o.broadcastLocked()
}

// settle marks the storage of subtree root as finished, kept or not, so nothing waits for it.
func (o *ownLaneBitmaps) settle(root chainhash.Hash) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, ok := o.pending[root]; ok {
		delete(o.pending, root)
		o.broadcastLocked()
	}
}

// broadcastLocked wakes every waiter; the caller holds o.mu.
func (o *ownLaneBitmaps) broadcastLocked() {
	if o.changed != nil {
		close(o.changed)
		o.changed = nil
	}
}

// changedLocked returns the channel the next change closes; the caller holds o.mu.
func (o *ownLaneBitmaps) changedLocked() chan struct{} {
	if o.changed == nil {
		o.changed = make(chan struct{})
	}

	return o.changed
}

// waitFor returns the kept bitmaps of each subtree in roots, in order, waiting until ctx ends or
// wait has passed for subtrees whose bitmaps are not kept yet. It reports how many have none by
// then: timedOut for a subtree still being stored, absent for one no storage is running for.
func (o *ownLaneBitmaps) waitFor(ctx context.Context, roots []*chainhash.Hash, wait time.Duration) (bitmaps []lane.LaneBitmaps, absent, timedOut int) {
	timer := time.NewTimer(wait)
	defer timer.Stop()

	for {
		o.mu.Lock()
		bitmaps, absent, timedOut = o.lookupLocked(roots)

		if absent+timedOut == 0 {
			o.mu.Unlock()
			return bitmaps, 0, 0
		}

		changed := o.changedLocked()
		o.mu.Unlock()

		select {
		case <-changed:
			continue
		case <-timer.C:
		case <-ctx.Done():
		}

		// Count once more as the wait ends: bitmaps kept, or a storage begun, as the timer fired
		// count as they now stand.
		o.mu.Lock()
		bitmaps, absent, timedOut = o.lookupLocked(roots)
		o.mu.Unlock()

		return bitmaps, absent, timedOut
	}
}

// lookupLocked returns the kept bitmaps of each subtree in roots, in order, and how many have none:
// timedOut for one still being stored, absent for one no storage is running for. The caller holds
// o.mu.
func (o *ownLaneBitmaps) lookupLocked(roots []*chainhash.Hash) (bitmaps []lane.LaneBitmaps, absent, timedOut int) {
	bitmaps = make([]lane.LaneBitmaps, len(roots))

	for i, root := range roots {
		if kept, ok := o.bySubtree[*root]; ok {
			bitmaps[i] = kept.lanes
			continue
		}

		if _, storing := o.pending[*root]; storing {
			timedOut++
		} else {
			absent++
		}
	}

	return bitmaps, absent, timedOut
}

// get returns the kept bitmaps of each subtree in roots, in order, and how many of them have
// none.
func (o *ownLaneBitmaps) get(roots []*chainhash.Hash) ([]lane.LaneBitmaps, int) {
	o.mu.Lock()
	defer o.mu.Unlock()

	bitmaps := make([]lane.LaneBitmaps, len(roots))
	missing := 0

	for i, root := range roots {
		kept, ok := o.bySubtree[*root]
		if !ok {
			missing++
			continue
		}

		bitmaps[i] = kept.lanes
	}

	return bitmaps, missing
}

// len is how many subtrees have kept bitmaps.
func (o *ownLaneBitmaps) len() int {
	o.mu.Lock()
	defer o.mu.Unlock()

	return len(o.bySubtree)
}

// subtreeLanePositions builds the lane bitmaps of subtree from the positions parentTxMap keeps with
// its transactions, for a subtree whose meta pass did not collect them (no meta is written, or
// building it failed). A transaction the map no longer holds has no position here: it is counted as
// without one, as a transaction that arrived without one is. The coinbase placeholder is skipped.
//
// It runs on a subtree storage worker, beside the processor, so the disk map is read with
// PositionWithErr: a read error that Position recorded on the map would be taken by the
// processor's next check for its own, and fail that block movement into a reset. A position that
// cannot be read is none, and counted.
//
// A tx removed from the pool while the subtree was being stored takes its position from
// deletedPositions, the backup kept beside its inpoints.
func subtreeLanePositions(subtree *subtreepkg.Subtree, parentTxMap subtreeprocessor.TxInpointsMap, deletedPositions *txmap.SyncedMap[chainhash.Hash, uint64]) *lane.BitmapsBuilder {
	laneBits := lane.NewBitmapsBuilder()

	withErr, readsWithErr := parentTxMap.(interface {
		PositionWithErr(hash chainhash.Hash) (uint64, error)
	})

	for _, node := range subtree.Nodes {
		if node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			continue
		}

		var position uint64

		switch {
		case readsWithErr:
			position, _ = withErr.PositionWithErr(node.Hash)
		case parentTxMap != nil:
			position = parentTxMap.Position(node.Hash)
		}

		if position == 0 {
			position = deletedLanePosition(deletedPositions, node.Hash)
		}

		laneBits.Add(position)
	}

	return laneBits
}

// storeSubtreeLaneBitmaps keeps the lane bitmaps laneBits collected for subtree in memory, for the
// rows of a block mined from it, and writes them as the subtree's lane bitmaps file with the
// subtree's delete-at height, so a peer's block that lists this subtree finds them too. A write that
// fails is logged; at shadow it never stops the subtree.
func (ba *BlockAssembly) storeSubtreeLaneBitmaps(ctx context.Context, subtree *subtreepkg.Subtree, laneBits *lane.BitmapsBuilder, height, dah uint32) {
	root := *subtree.RootHash()

	lane.CountTxsWithoutPosition(laneBitmapsService, laneBits.Unpositioned())

	bitmaps := laneBits.Subtree(root)
	ba.ownLaneBitmaps.put(root, bitmaps.Lanes, height)

	data, err := lane.EncodeSubtreeBitmaps(bitmaps)
	if err != nil {
		ba.logger.Warnf("[BlockAssembly:storeSubtreeData][%s] storing the subtree without its lane bitmaps file, which could not be encoded: %v", root.String(), err)
		return
	}

	err = ba.subtreeStore.Set(ctx, root[:], fileformat.FileTypeSubtreeLaneBitmaps, data, options.WithDeleteAt(dah))

	switch {
	case errors.Is(err, errors.ErrBlobAlreadyExists):
		// a file from an earlier store, after a restart or by subtree validation, lives as long as
		// the subtree now does
		if dahErr := ba.subtreeStore.SetDAH(ctx, root[:], fileformat.FileTypeSubtreeLaneBitmaps, dah); dahErr != nil {
			ba.logger.Warnf("[BlockAssembly:storeSubtreeData][%s] the lane bitmaps file may now expire before its subtree, whose DAH update failed: %v", root.String(), dahErr)
		}
	case err != nil:
		ba.logger.Warnf("[BlockAssembly:storeSubtreeData][%s] storing the subtree without its lane bitmaps file, whose write failed: %v", root.String(), err)
	}
}

// keepExistingSubtreeLaneBitmaps is the lane half of storing a subtree the store already has: the
// subtree's bitmaps stay kept for as long as the subtree is, and their file gets its new delete-at
// height. Bitmaps not kept in memory, after a restart or for a subtree first stored by subtree
// validation, are built from the pool and stored.
func (ba *BlockAssembly) keepExistingSubtreeLaneBitmaps(ctx context.Context, subtreeRequest subtreeprocessor.NewSubtreeRequest, height, dah uint32) {
	subtree := subtreeRequest.Subtree
	root := *subtree.RootHash()

	if !ba.ownLaneBitmaps.touch(root, height) {
		ba.storeSubtreeLaneBitmaps(ctx, subtree, subtreeLanePositions(subtree, subtreeRequest.ParentTxMap, subtreeRequest.DeletedPositions), height, dah)
		return
	}

	err := ba.subtreeStore.SetDAH(ctx, root[:], fileformat.FileTypeSubtreeLaneBitmaps, dah)

	switch {
	case errors.Is(err, errors.ErrNotFound):
		// an earlier write of the file failed while its bitmaps were kept: write it now
		ba.storeSubtreeLaneBitmaps(ctx, subtree, subtreeLanePositions(subtree, subtreeRequest.ParentTxMap, subtreeRequest.DeletedPositions), height, dah)
	case err != nil:
		ba.logger.Warnf("[BlockAssembly:storeSubtreeData][%s] the lane bitmaps file may now expire before its subtree, whose DAH update failed: %v", root.String(), err)
	}
}

// ownLaneRows builds the lane map rows of block, a block this node mined, from the kept lane
// bitmaps of its subtrees, or returns nil when lanes are off or the rows cannot be built, which is
// counted and logged.
//
// A subtree of the block whose bitmaps are not kept yet is waited for, up to ownLaneBitmapsWait:
// its storage may still be running. When the wait ends with every subtree's bitmaps kept, it is
// part of the build stage's time (teranode_lane_gate_seconds{block="own",stage="build"}). When any
// is still missing, no build time is recorded: one still being stored counts as timed out and one
// with no storage running as absent, in teranode_lane_missing_bitmaps_total.
func (ba *BlockAssembly) ownLaneRows(ctx context.Context, block *model.Block) []lane.MapRow {
	if !ba.keepsLanePositions() || len(block.Subtrees) == 0 {
		return nil
	}

	start := time.Now()

	bitmaps, absent, timedOut := ba.ownLaneBitmaps.waitFor(ctx, block.Subtrees, ownLaneBitmapsWait)
	if absent+timedOut > 0 {
		lane.CountMissingBitmaps(lane.BlockOwn, lane.MissingBitmapsAbsent, absent)
		lane.CountMissingBitmaps(lane.BlockOwn, lane.MissingBitmapsTimedOut, timedOut)
		ba.logger.Warnf("[ownLaneRows][%s] storing the block without lane rows: of its %d subtrees, %d have no lane bitmaps kept and %d were still being stored after %s", block.Hash().String(), len(block.Subtrees), absent, timedOut, ownLaneBitmapsWait)

		return nil
	}

	rows, err := lane.BuildBlockRows(bitmaps)
	if err != nil {
		lane.CountRowInsertFailure(lane.BlockOwn)
		ba.logger.Warnf("[ownLaneRows][%s] storing the block without lane rows, which could not be built: %v", block.Hash().String(), err)

		return nil
	}

	lane.ObserveGate(lane.BlockOwn, lane.GateStageBuild, time.Since(start))
	lane.ObserveBlockRows(lane.BlockOwn, rows)
	ba.checkOwnLaneRowCount(block, rows.Members)

	return rows.Rows
}

// checkOwnLaneRowCount compares the members of a block's rows with its transactions less the
// coinbase, which takes no position from a lane, and counts and logs a difference. A transaction
// that reached block assembly without a position is one fewer member.
func (ba *BlockAssembly) checkOwnLaneRowCount(block *model.Block, members uint64) {
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

	lane.CountRowMismatch(lane.BlockOwn, direction)
	ba.logger.Warnf("[ownLaneRows][%s] lane rows hold %d members, the block has %d transactions besides its coinbase", block.Hash().String(), members, want)
}

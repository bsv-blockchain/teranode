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
// they were last stored at, or last listed in a mining candidate. A block that moves the pool on
// usually rebuilds every transaction left into new subtrees, which are stored and kept again; a
// coinbase-only block does not, so the pool's subtrees stay as they were. The subtrees the latest
// mining candidate lists are therefore never dropped, whatever their height
// (keepCandidateLaneBitmaps): several coinbase-only blocks with no candidate served between them
// would otherwise age out subtrees still in the pool. Any other entry older than this is dropped
// when the next one is kept. This bounds the memory to the subtrees stored or listed at the last
// ownLaneBitmapsKeepBlocks+1 heights, plus the latest candidate's: about 0.5 KB per
// million-transaction subtree whose lanes are contiguous runs, which is how block assembly fills
// its subtrees from the queue.
const ownLaneBitmapsKeepBlocks = 2

// ownLaneBitmapsWait bounds how long storing our block waits for the kept bitmaps of a subtree
// whose storage is still running. A sealed subtree joins the candidate at once, and its storage
// worker keeps its bitmaps once the subtree's meta pass is done: measured at about 120 ms (in-memory
// map) to 300 ms (disk map) per million transactions after a worker takes it. So a block mined
// right after a seal, or right after a block rebuilds the pool's subtrees, would otherwise find
// them missing. A variable
// so a test can shorten it.
var ownLaneBitmapsWait = time.Second

// ownLaneBitmaps keeps the lane bitmaps of the subtrees block assembly stored, by subtree root
// hash, for the rows of the blocks it mines. The zero value is ready to use.
type ownLaneBitmaps struct {
	mu        sync.Mutex
	bySubtree map[chainhash.Hash]keptLaneBitmaps
	// pending holds the subtrees a storage worker has taken whose storage has not finished. A
	// subtree still queued for a worker, behind a storage backlog, is not in it yet, so a block
	// listing it counts it absent rather than timed out.
	// It counts the storage requests in flight per subtree, so one that ends does not hide another
	// of the same subtree still running.
	pending map[chainhash.Hash]int
	// changed is closed, and replaced, whenever a subtree's bitmaps are kept or its storage ends.
	changed chan struct{}
	// listed holds the subtrees the latest mining candidate lists, whose entries are not dropped.
	listed map[chainhash.Hash]struct{}
	// fileDAHLocks serialize the writes and delete-at updates of one subtree's lane bitmaps file,
	// so two storage requests of it cannot leave the file with the older one's delete-at. They are
	// striped by root, so a slow write of one subtree's file does not hold up another's.
	fileDAHLocks [64]sync.Mutex
}

// fileLock is the lock that serializes subtree root's lane bitmaps file writes and refreshes.
func (o *ownLaneBitmaps) fileLock(root chainhash.Hash) *sync.Mutex {
	return &o.fileDAHLocks[root[0]%uint8(len(o.fileDAHLocks))]
}

// fileDAHLater reports whether subtree root's lane bitmaps file already has a later delete-at
// height than dah from this process. An equal one is not skipped, so a store of the subtree again
// still finds out from the file store whether the file was lost.
func (o *ownLaneBitmaps) fileDAHLater(root chainhash.Hash, dah uint32) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	kept, ok := o.bySubtree[root]

	return ok && kept.fileDAH > dah
}

// recordFileDAH notes dah as the delete-at height subtree root's lane bitmaps file now has, if
// its bitmaps are kept and it is later than the one noted.
func (o *ownLaneBitmaps) recordFileDAH(root chainhash.Hash, dah uint32) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if kept, ok := o.bySubtree[root]; ok && kept.fileDAH < dah {
		kept.fileDAH = dah
		o.bySubtree[root] = kept
	}
}

// keptLaneBitmaps is one subtree's bitmaps and the block height it was last stored at.
type keptLaneBitmaps struct {
	lanes  lane.LaneBitmaps
	height uint32
	// fileDAH is the highest delete-at height this process gave the subtree's lane bitmaps file,
	// 0 when none; a refresh never lowers it (setLaneBitmapsFileDAH).
	fileDAH uint32
}

// put keeps the bitmaps of subtree root, stored at height, and drops entries last stored more
// than ownLaneBitmapsKeepBlocks blocks below it. It reports whether root was not kept before, so
// two storage requests of one subtree count its members once.
func (o *ownLaneBitmaps) put(root chainhash.Hash, lanes lane.LaneBitmaps, height uint32) (first bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.bySubtree == nil {
		o.bySubtree = make(map[chainhash.Hash]keptLaneBitmaps)
	}

	// a worker that captured an older height, for a root a newer store or candidate has since
	// kept, keeps the newer height, so the entry is not dropped while candidates list it
	keptHeight := height

	kept, already := o.bySubtree[root]
	if already && kept.height > keptHeight {
		keptHeight = kept.height
	}

	first = !already

	o.bySubtree[root] = keptLaneBitmaps{lanes: lanes, height: keptHeight, fileDAH: kept.fileDAH}
	o.broadcastLocked()

	if height <= ownLaneBitmapsKeepBlocks {
		return first
	}

	for hash, kept := range o.bySubtree {
		if _, inCandidate := o.listed[hash]; inCandidate {
			continue
		}

		// a storage request of it still running would re-create the entry without its file's
		// delete-at height
		if o.pending[hash] > 0 {
			continue
		}

		if kept.height < height-ownLaneBitmapsKeepBlocks {
			delete(o.bySubtree, hash)
		}
	}

	return first
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
		o.broadcastLocked()
	}

	return ok
}

// lookup returns the kept bitmaps of subtree root, and whether any are kept.
func (o *ownLaneBitmaps) lookup(root chainhash.Hash) (lane.LaneBitmaps, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	kept, ok := o.bySubtree[root]

	return kept.lanes, ok
}

// list records roots as the subtrees the latest mining candidate lists, which put does not drop,
// and marks their kept bitmaps as current at height.
func (o *ownLaneBitmaps) list(roots []chainhash.Hash, height uint32) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.listed = make(map[chainhash.Hash]struct{}, len(roots))

	for _, root := range roots {
		o.listed[root] = struct{}{}

		if kept, ok := o.bySubtree[root]; ok && kept.height < height {
			kept.height = height
			o.bySubtree[root] = kept
		}
	}
}

// expect marks subtree root as being stored by a storage worker, so a block that lists it before
// its bitmaps are kept counts it as timed out, not absent.
func (o *ownLaneBitmaps) expect(root chainhash.Hash) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.pending == nil {
		o.pending = make(map[chainhash.Hash]int)
	}

	o.pending[root]++

	// a waiter that counted this subtree before its storage began counts it again
	o.broadcastLocked()
}

// settle marks one storage request of subtree root, the one expect counted, as finished, kept or
// not; once none is left, nothing waits for it.
func (o *ownLaneBitmaps) settle(root chainhash.Hash) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.pending[root] <= 1 {
		delete(o.pending, root)
	} else {
		o.pending[root]--
	}

	o.broadcastLocked()
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

// subtreeLanePositions builds the lane bitmaps of subtree from the positions parentTxMap keeps with
// its transactions, for a subtree whose meta pass did not collect them (no meta is written, or
// building it failed). A tx removed from the pool while the subtree was being stored is in
// deletedTxs and takes its position from deletedPositions, the backup kept beside it. lost counts
// the members found in neither: the half of the double buffer the subtree was sealed from has
// been cleared since, after a block, so the bitmaps would be missing those members and must not be
// kept. The coinbase placeholder is skipped.
//
// It runs on a subtree storage worker, beside the processor, so the disk map is read with
// PositionWithErr (see poolLanePosition). A position that cannot be read, and is in neither the
// removal backup nor a second pool read, is unknown: the member is lost, so the subtree gets no
// bitmaps, and it is not counted as a member without a position.
func subtreeLanePositions(subtree *subtreepkg.Subtree, parentTxMap subtreeprocessor.TxInpointsMap,
	deletedTxs *txmap.SyncedMap[chainhash.Hash, subtreepkg.TxInpoints], deletedPositions *txmap.SyncedMap[chainhash.Hash, uint64]) (laneBits *lane.BitmapsBuilder, lost int) {
	laneBits = lane.NewBitmapsBuilder()

	for _, node := range subtree.Nodes {
		if node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			continue
		}

		// the processor may remove or re-add the member between reads, so a 0 is checked against
		// the removal backup and the map again; a member whose position cannot be told from none
		// is lost, as is one in neither the pool nor the removal backups
		position := poolLanePosition(parentTxMap, node.Hash)
		if position == 0 {
			var known bool

			position, known = removedLanePosition(parentTxMap, deletedPositions, node.Hash)

			inPool := parentTxMap != nil && parentTxMap.Exists(node.Hash)
			inRemoved := deletedTxs != nil && deletedTxs.Exists(node.Hash)

			if !known || (!inPool && !inRemoved) {
				lost++
				continue
			}
		}

		laneBits.Add(position)
	}

	return laneBits, lost
}

// storeLanePositionsFromPool builds subtree's lane bitmaps with subtreeLanePositions and stores
// them, unless the pool no longer holds some of its members, when it stores none: the subtree then
// counts as absent for a block that lists it, rather than holding bitmaps that miss members.
func (ba *BlockAssembly) storeLanePositionsFromPool(ctx context.Context, subtreeRequest subtreeprocessor.NewSubtreeRequest, height, dah uint32) {
	subtree := subtreeRequest.Subtree

	laneBits, lost := subtreeLanePositions(subtree, subtreeRequest.ParentTxMap, subtreeRequest.DeletedTxs, subtreeRequest.DeletedPositions)
	if lost > 0 {
		ba.logger.Warnf("[BlockAssembly:storeSubtreeData][%s] storing the subtree without lane bitmaps: %d of its members are no longer in the pool it was sealed from", subtree.RootHash().String(), lost)
		return
	}

	ba.storeSubtreeLaneBitmaps(ctx, subtree, laneBits, height, dah)
}

// storeSubtreeLaneBitmaps keeps the lane bitmaps laneBits collected for subtree in memory, for the
// rows of a block mined from it, and writes them as the subtree's lane bitmaps file with the
// subtree's delete-at height, so a peer's block that lists this subtree finds them too. A write that
// fails is logged; at shadow it never stops the subtree.
func (ba *BlockAssembly) storeSubtreeLaneBitmaps(ctx context.Context, subtree *subtreepkg.Subtree, laneBits *lane.BitmapsBuilder, height, dah uint32) {
	root := *subtree.RootHash()

	// a second storage request of the same subtree, overlapping the first, does not count its
	// members again
	bitmaps := laneBits.Subtree(root)
	if ba.ownLaneBitmaps.put(root, bitmaps.Lanes, height) {
		lane.CountTxsWithoutPosition(laneBitmapsService, laneBits.Unpositioned())
	}

	ba.writeLaneBitmapsFile(ctx, bitmaps, dah)
}

// writeLaneBitmapsFile writes a subtree's lane bitmaps file, to live until dah. A file already
// there gets dah; a file that cannot be encoded or written is counted under block="own".
func (ba *BlockAssembly) writeLaneBitmapsFile(ctx context.Context, bitmaps lane.SubtreeBitmaps, dah uint32) {
	root := bitmaps.SubtreeHash

	data, err := lane.EncodeSubtreeBitmaps(bitmaps)
	if err != nil {
		lane.CountMissingBitmaps(lane.BlockOwn, lane.MissingBitmapsWriteFailed, 1)
		ba.logger.Warnf("[BlockAssembly:storeSubtreeData][%s] storing the subtree without its lane bitmaps file, which could not be encoded: %v", root.String(), err)

		return
	}

	// An exclusive publish bounds the file store's delete-at scheduling, which the subtree file
	// waits behind, and never replaces the delete-at of a file another writer published first.
	fileLock := ba.ownLaneBitmaps.fileLock(root)
	fileLock.Lock()
	err = ba.subtreeStore.Set(ctx, root[:], fileformat.FileTypeSubtreeLaneBitmaps, data, options.WithDeleteAt(dah), options.WithExclusivePublish())
	if err == nil {
		ba.ownLaneBitmaps.recordFileDAH(root, dah)
	}
	fileLock.Unlock()

	switch {
	case errors.Is(err, errors.ErrBlobAlreadyExists):
		// a file from an earlier store, after a restart or by subtree validation, lives as long as
		// the subtree now does
		if dahErr := ba.setLaneBitmapsFileDAH(ctx, root, dah); dahErr != nil {
			ba.logger.Warnf("[BlockAssembly:storeSubtreeData][%s] the lane bitmaps file may now expire before its subtree, whose DAH update failed: %v", root.String(), dahErr)
		}
	case err != nil:
		// a peer's block that lists this subtree will count it absent; our own uses the kept bitmaps
		lane.CountMissingBitmaps(lane.BlockOwn, lane.MissingBitmapsWriteFailed, 1)
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
		ba.storeLanePositionsFromPool(ctx, subtreeRequest, height, dah)
		return
	}

	err := ba.setLaneBitmapsFileDAH(ctx, root, dah)

	switch {
	case errors.Is(err, errors.ErrNotFound):
		// an earlier write of the file failed while its bitmaps were kept: write it now from the
		// kept bitmaps, whose members were counted when they were collected
		if lanes, kept := ba.ownLaneBitmaps.lookup(root); kept {
			ba.writeLaneBitmapsFile(ctx, lane.SubtreeBitmaps{SubtreeHash: root, Lanes: lanes}, dah)
		} else {
			ba.storeLanePositionsFromPool(ctx, subtreeRequest, height, dah)
		}
	case err != nil:
		ba.logger.Warnf("[BlockAssembly:storeSubtreeData][%s] the lane bitmaps file may now expire before its subtree, whose DAH update failed: %v", root.String(), err)
	}
}

// setLaneBitmapsFileDAH gives subtree root's lane bitmaps file delete-at height dah, unless this
// process already gave it a later one. The updates of one subtree's file are serialized (fileLock): the file store
// replaces a blob's delete-at rather than keeping the later one, so two storage requests of one
// subtree racing would otherwise leave the file with whichever height came last, possibly the
// older, and the file would expire before its subtree.
func (ba *BlockAssembly) setLaneBitmapsFileDAH(ctx context.Context, root chainhash.Hash, dah uint32) error {
	fileLock := ba.ownLaneBitmaps.fileLock(root)
	fileLock.Lock()

	defer fileLock.Unlock()

	if ba.ownLaneBitmaps.fileDAHLater(root, dah) {
		return nil
	}

	if err := ba.subtreeStore.SetDAH(ctx, root[:], fileformat.FileTypeSubtreeLaneBitmaps, dah); err != nil {
		return err
	}

	ba.ownLaneBitmaps.recordFileDAH(root, dah)

	return nil
}

// keepCandidateLaneBitmaps keeps the bitmaps of a mining candidate's subtrees until a later
// candidate no longer lists them, so they outlive blocks that do not rebuild the pool's subtrees.
func (ba *BlockAssembly) keepCandidateLaneBitmaps(subtrees []*subtreepkg.Subtree) {
	if ba.blockAssembler == nil || ba.blockAssembler.utxoStore == nil {
		return
	}

	// an empty candidate, served while a block moves the pool, lists none of the pool's subtrees
	// and keeps the pins of the last candidate that did
	if len(subtrees) == 0 {
		return
	}

	roots := make([]chainhash.Hash, len(subtrees))
	for i, st := range subtrees {
		roots[i] = *st.RootHash()
	}

	ba.ownLaneBitmaps.list(roots, ba.blockAssembler.utxoStore.GetBlockHeight())
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
// coinbase, and counts and logs a difference. The coinbase does take a position when its outputs
// are created, but it is not in the bitmaps: a stored subtree holds its placeholder. A transaction
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

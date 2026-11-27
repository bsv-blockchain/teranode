package blockassembly

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// The lane gate for the blocks this node mines, at utxostore_laneMigration shadow (lanes Phase 1).
// The validator hands block assembly each transaction's own lane position. The subtree processor
// records each position as the transaction joins the open subtree, builds the subtree's lane
// bitmaps when it seals the subtree, before a mining candidate can list it, and hands them to
// ownLaneBitmaps, kept in memory by subtree hash (subtreeprocessor/lane_bitmaps.go). The storage
// worker writes them as the subtree's lane bitmaps file before the subtree file, as subtree
// validation does for a peer's subtree. When a block it mined is stored, its rows are merged from
// the kept bitmaps of the block's own subtrees, the candidate that was cut, never from the pool,
// which by then also holds transactions that arrived after the cut. Nothing reads the rows yet, so
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

// ownLaneBitmaps keeps the lane bitmaps of the subtrees the subtree processor chained, by subtree
// root hash, for the rows of the blocks block assembly mines. The zero value is ready to use.
type ownLaneBitmaps struct {
	mu        sync.Mutex
	bySubtree map[chainhash.Hash]keptLaneBitmaps
	// height returns the block height a subtree's bitmaps are kept at; nil keeps them at 0.
	height func() uint32
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

// keptLaneBitmaps is one subtree's bitmaps and the block height it was last kept, stored or
// listed at.
type keptLaneBitmaps struct {
	lanes  lane.LaneBitmaps
	height uint32
	// fileDAH is the highest delete-at height this process gave the subtree's lane bitmaps file,
	// 0 when none; a refresh never lowers it (setLaneBitmapsFileDAH).
	fileDAH uint32
}

// put keeps the bitmaps of subtree root at height, and drops entries last kept more than
// ownLaneBitmapsKeepBlocks blocks below it. It reports whether root was not kept before, so a
// subtree chained twice counts its members once.
func (o *ownLaneBitmaps) put(root chainhash.Hash, lanes lane.LaneBitmaps, height uint32) (first bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.bySubtree == nil {
		o.bySubtree = make(map[chainhash.Hash]keptLaneBitmaps)
	}

	// a root a newer store or candidate has since kept keeps the newer height, so the entry is not
	// dropped while candidates list it
	keptHeight := height

	kept, already := o.bySubtree[root]
	if already && kept.height > keptHeight {
		keptHeight = kept.height
	}

	first = !already

	o.bySubtree[root] = keptLaneBitmaps{lanes: lanes, height: keptHeight, fileDAH: kept.fileDAH}

	if height <= ownLaneBitmapsKeepBlocks {
		return first
	}

	for hash, kept := range o.bySubtree {
		if _, inCandidate := o.listed[hash]; inCandidate {
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

	return ok
}

// KeepLaneBitmaps keeps the lane bitmaps the subtree processor built for a subtree it chained
// (subtreeprocessor.LaneBitmapsKeeper), and counts its members without a position the first time
// the subtree is kept.
func (o *ownLaneBitmaps) KeepLaneBitmaps(bitmaps lane.SubtreeBitmaps, unpositioned int) {
	var height uint32
	if o.height != nil {
		height = o.height()
	}

	if o.put(bitmaps.SubtreeHash, bitmaps.Lanes, height) {
		lane.CountTxsWithoutPosition(laneBitmapsService, unpositioned)
	}
}

// HasLaneBitmaps reports whether the bitmaps of subtree root are kept.
func (o *ownLaneBitmaps) HasLaneBitmaps(root chainhash.Hash) bool {
	_, ok := o.lookup(root)

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

// writeLaneBitmapsFile writes a subtree's lane bitmaps file, to live until dah. A file already
// there is given dah, and a failure to give it is logged without failing the write. A write the
// store refuses returns a storage error; storing a new subtree then writes no subtree file on
// that attempt. Bitmaps that cannot be encoded are a lane fault, not a store one, return nil, and
// do not stop the subtree. Either failure is counted under block="own".
func (ba *BlockAssembly) writeLaneBitmapsFile(ctx context.Context, bitmaps lane.SubtreeBitmaps, dah uint32) error {
	root := bitmaps.SubtreeHash

	data, err := lane.EncodeSubtreeBitmaps(bitmaps)
	if err != nil {
		lane.CountMissingBitmaps(lane.BlockOwn, lane.MissingBitmapsWriteFailed, 1)
		ba.logger.Warnf("[BlockAssembly:storeSubtreeData][%s] storing the subtree without its lane bitmaps file, which could not be encoded: %v", root.String(), err)

		return nil
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
		lane.CountMissingBitmaps(lane.BlockOwn, lane.MissingBitmapsWriteFailed, 1)

		return errors.NewStorageError("[BlockAssembly:storeSubtreeData][%s] failed to store the subtree's lane bitmaps file", root.String(), err)
	}

	return nil
}

// writeKeptLaneBitmapsFile writes the kept lane bitmaps of subtree root as its lane bitmaps file,
// to live until dah, and marks them as stored at height. A subtree with no bitmaps kept gets no
// file, which is counted as write_failed under block="own" and logged, and does not stop the
// subtree. It returns the error of a write the store refused (writeLaneBitmapsFile).
func (ba *BlockAssembly) writeKeptLaneBitmapsFile(ctx context.Context, root chainhash.Hash, height, dah uint32) error {
	lanes, kept := ba.ownLaneBitmaps.lookup(root)

	return ba.writeCapturedLaneBitmapsFile(ctx, root, lanes, kept, height, dah)
}

// writeCapturedLaneBitmapsFile is writeKeptLaneBitmapsFile for bitmaps the caller already read:
// lanes, when kept is true.
func (ba *BlockAssembly) writeCapturedLaneBitmapsFile(ctx context.Context, root chainhash.Hash, lanes lane.LaneBitmaps, kept bool, height, dah uint32) error {
	if !kept {
		lane.CountMissingBitmaps(lane.BlockOwn, lane.MissingBitmapsWriteFailed, 1)
		ba.logger.Warnf("[BlockAssembly:storeSubtreeData][%s] storing the subtree without its lane bitmaps file: it has no lane bitmaps kept", root.String())

		return nil
	}

	ba.ownLaneBitmaps.touch(root, height)

	return ba.writeLaneBitmapsFile(ctx, lane.SubtreeBitmaps{SubtreeHash: root, Lanes: lanes}, dah)
}

// laneRetryBitmaps returns the kept lane bitmaps of subtree root for a storage retry to carry, and
// whether lanes are on and any are kept.
func (ba *BlockAssembly) laneRetryBitmaps(root chainhash.Hash) (lane.LaneBitmaps, bool) {
	if !ba.keepsLanePositions() {
		return nil, false
	}

	return ba.ownLaneBitmaps.lookup(root)
}

// keepExistingSubtreeLaneBitmaps is the lane half of storing a subtree the store already has: the
// subtree's kept bitmaps stay kept for as long as the subtree is, and their file gets its new
// delete-at height, or is written from the kept bitmaps when the store has none.
func (ba *BlockAssembly) keepExistingSubtreeLaneBitmaps(ctx context.Context, root chainhash.Hash, height, dah uint32) {
	if !ba.ownLaneBitmaps.touch(root, height) {
		ba.logger.Warnf("[BlockAssembly:storeSubtreeData][%s] the stored subtree has no lane bitmaps kept", root.String())
		return
	}

	err := ba.setLaneBitmapsFileDAH(ctx, root, dah)

	switch {
	case errors.Is(err, errors.ErrNotFound):
		// the subtree was stored without the file, before its bitmaps were kept or while they
		// could not be encoded: write it now from the kept bitmaps, whose members were counted
		// when they were kept
		if writeErr := ba.writeKeptLaneBitmapsFile(ctx, root, height, dah); writeErr != nil {
			ba.logger.Warnf("[BlockAssembly:storeSubtreeData][%s] the stored subtree is still without its lane bitmaps file: %v", root.String(), writeErr)
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
// The subtree processor keeps a subtree's bitmaps before a mining candidate can list it, so nothing
// waits here: a subtree of the block with none kept counts as absent in
// teranode_lane_missing_bitmaps_total, and the block is stored without rows.
func (ba *BlockAssembly) ownLaneRows(ctx context.Context, block *model.Block) []lane.MapRow {
	if !ba.keepsLanePositions() || len(block.Subtrees) == 0 {
		return nil
	}

	start := time.Now()

	bitmaps := make([]lane.LaneBitmaps, len(block.Subtrees))
	absent := 0

	for i, root := range block.Subtrees {
		lanes, kept := ba.ownLaneBitmaps.lookup(*root)
		if !kept {
			absent++
			continue
		}

		bitmaps[i] = lanes
	}

	if absent > 0 {
		lane.CountMissingBitmaps(lane.BlockOwn, lane.MissingBitmapsAbsent, absent)
		ba.logger.Warnf("[ownLaneRows][%s] storing the block without lane rows: of its %d subtrees, %d have no lane bitmaps kept", block.Hash().String(), len(block.Subtrees), absent)

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

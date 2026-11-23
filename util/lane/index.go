package lane

import (
	"bytes"
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// The reader's index of the map, Phase 1 (utxostore_laneMigration shadow): the rows of the recent
// blocks a reader has loaded, by lane, and a floor per lane, so a reader can say which block took a
// position without a database read. Nothing acts on its answers yet; they are compared with
// today's mined lists and counted.
//
// Take lane 7 at a tip of height 1,200. Block 1,150 took positions 1 to 1,000 with a hole at 977,
// and block 1,160 took 977 and 1,001 to 1,400. Both rows are in lane 7's interval tree, keyed by
// their ranges, so a lookup of 977 finds both rows and their bitmaps decide: block 1,150 says
// hole, block 1,160 says taken. Once block 1,160 is 100 blocks deep (settled, the coinbase
// maturity depth), its rows and every older block's are folded into lane 7's floor: the floor
// moves to 1,401 and every position below it that no folded row took stays listed. A lookup below
// the floor that is not listed is answered with one comparison and one bitmap test: mined, in a
// settled main-chain block, which block not said. The rows themselves leave memory.
//
// Rows are folded by block, never by position. A transaction mined a day late, at position 600
// below the floor, keeps its row in the tree until its own block settles; only then does 600 leave
// the floor's list.
//
// The floor is built only from the rows this index loaded. Below the first row it folded for a
// lane, every position is listed, so the index never calls a position settled that it did not see
// settle. Floors from history, written by the pruner and loaded at start, are Phase 2.
//
// Two writers add blocks and publish: the follower, which also moves the tip, and block
// validation loading a block's own ancestry. They take turns, one load at a time, and every change
// is made under the Index's mutex and published as an immutable IndexSnapshot, so a lookup takes
// no lock.

// DefaultSettleDepth is how deep a main-chain block must be before its rows fold into the floor:
// the coinbase maturity, the same depth the fork-depth check accepts.
const DefaultSettleDepth = 100

// keepFoldedBlocks is how many folded blocks below the tip an index keeps the metadata of, so a
// reader can still say a block an old mined list names was loaded and folded. About four weeks.
const keepFoldedBlocks = 4_032

// Via says what answered a lookup.
type Via uint8

const (
	// ViaNone is no answer: no row on the judged chain takes the position, and the floor does not
	// cover it.
	ViaNone Via = iota
	// ViaRow is a row of a block on the judged chain that takes the position.
	ViaRow
	// ViaFloor is the floor: the position is below its lane's floor and not listed, so a settled
	// main-chain block took it.
	ViaFloor
)

// String is the metric label for v.
func (v Via) String() string {
	switch v {
	case ViaRow:
		return "row"
	case ViaFloor:
		return "floor"
	default:
		return "none"
	}
}

// Answer is the index's answer for one position on one chain.
type Answer struct {
	Mined bool
	Via   Via
	// BlockID and Height name the block whose row takes the position, with ViaRow; zero otherwise.
	BlockID uint32
	Height  uint32
}

// IndexBlock is one block as the index knows it: from its header, not from the map.
type IndexBlock struct {
	ID     uint32
	Height uint32
	Hash   chainhash.Hash
	Parent chainhash.Hash
}

// indexRow is one loaded row. A range row, whose block took first to last, has neither bits nor
// taken. A row with holes has one of them: bits, a plain bitset of its span from first, when that
// costs little more than its Roaring bitmap, or taken.
//
// The plain bitset is for mainnet's backlog shape, where a block takes a thin scatter of a day of
// positions and many rows overlap: Roaring stores such a row as sorted arrays, and a lookup tests
// every overlapping row, each test a binary search that misses the cache. A bitset test is one
// read.
type indexRow struct {
	blockID   uint32
	first     uint64
	last      uint64
	firstHole uint64
	bits      []uint64
	taken     *roaring64.Bitmap
}

// Plain bitsets: a row gets one when it costs at most plainBitsetSlack times its Roaring bitmap
// plus plainBitsetAllowance, and its block took at most plainBitsetMaxTaken positions, which bounds
// what building it costs.
const (
	plainBitsetSlack     = 2
	plainBitsetAllowance = 64 << 10
	plainBitsetMaxTaken  = 1 << 22
)

// takes reports whether the row's block took p, which lies within first to last.
func (r *indexRow) takes(p uint64) bool {
	if p < r.firstHole || (r.bits == nil && r.taken == nil) {
		return true
	}

	if r.bits != nil {
		off := p - r.first

		return r.bits[off>>6]&(1<<(off&63)) != 0
	}

	return r.taken.Contains(p)
}

// bitmap is the positions the row took, built for a range row or a plain bitset.
func (r *indexRow) bitmap() *roaring64.Bitmap {
	if r.taken != nil {
		return r.taken
	}

	bm := roaring64.New()

	if r.bits == nil {
		bm.AddRange(r.first, r.last+1)

		return bm
	}

	for w, word := range r.bits {
		for word != 0 {
			bit := uint64(bits.TrailingZeros64(word))
			bm.Add(r.first + uint64(w)<<6 + bit) //nolint:gosec // w is a slice index
			word &= word - 1
		}
	}

	return bm
}

// bytes is what the row holds in memory: the entry and its bitset or loaded bitmap.
func (r *indexRow) bytes() uint64 {
	const entry = 72 + 8 // the struct and its pointer in the lane's slice

	switch {
	case r.bits != nil:
		return entry + uint64(len(r.bits))*8
	case r.taken != nil:
		return entry + r.taken.GetSizeInBytes()
	default:
		return entry
	}
}

// plainBits returns bm as a bitset of first to last, or nil when that would cost too much.
func plainBits(bm *roaring64.Bitmap, first, last uint64) []uint64 {
	words := (last - first + 64) / 64
	if bm.GetCardinality() > plainBitsetMaxTaken || words*8 > plainBitsetSlack*bm.GetSizeInBytes()+plainBitsetAllowance {
		return nil
	}

	out := make([]uint64, words)
	it := bm.ManyIterator()
	buf := make([]uint64, 4096)

	for n := it.NextMany(buf); n > 0; n = it.NextMany(buf) {
		for _, p := range buf[:n] {
			off := p - first
			out[off>>6] |= 1 << (off & 63)
		}
	}

	return out
}

// laneState is one lane: its rows sorted by first position in an implicit interval tree, and its
// floor. A laneState is immutable once published.
type laneState struct {
	rows []*indexRow
	// maxLast[i] is the highest last position in the subtree rooted at rows[i], where the root of
	// rows[lo:hi] is rows[(lo+hi)/2].
	maxLast []uint64
	// floor: every position below it is settled unless listed. Zero means no floor yet.
	floor  uint64
	listed *roaring64.Bitmap
}

func newLaneState(rows []*indexRow, floor uint64, listed *roaring64.Bitmap) *laneState {
	slices.SortFunc(rows, func(a, b *indexRow) int {
		switch {
		case a.first != b.first:
			if a.first < b.first {
				return -1
			}

			return 1
		case a.blockID != b.blockID:
			if a.blockID < b.blockID {
				return -1
			}

			return 1
		default:
			return 0
		}
	})

	ls := &laneState{rows: rows, maxLast: make([]uint64, len(rows)), floor: floor, listed: listed}
	ls.build(0, len(rows))

	return ls
}

func (ls *laneState) build(lo, hi int) uint64 {
	if lo >= hi {
		return 0
	}

	mid := lo + (hi-lo)/2
	m := ls.rows[mid].last
	m = max(m, ls.build(lo, mid), ls.build(mid+1, hi))
	ls.maxLast[mid] = m

	return m
}

// covering calls fn for every row whose range holds p, until fn returns false.
func (ls *laneState) covering(p uint64, fn func(*indexRow) bool) bool {
	return ls.visit(0, len(ls.rows), p, fn)
}

func (ls *laneState) visit(lo, hi int, p uint64, fn func(*indexRow) bool) bool {
	for lo < hi {
		mid := lo + (hi-lo)/2
		if ls.maxLast[mid] < p {
			return true
		}

		if !ls.visit(lo, mid, p, fn) {
			return false
		}

		r := ls.rows[mid]
		if r.first > p {
			// Every row to the right starts above p too.
			return true
		}

		if r.last >= p && !fn(r) {
			return false
		}

		lo = mid + 1
	}

	return true
}

// belowFloor reports whether p is below the lane's floor and not listed: settled.
func (ls *laneState) belowFloor(p uint64) bool {
	return p < ls.floor && (ls.listed == nil || !ls.listed.Contains(p))
}

// snapshotBlock is a block as a snapshot holds it.
type snapshotBlock struct {
	id     uint32
	height uint32
	parent uint32
	// parentLoaded is whether the parent is loaded; parent is meaningful only then.
	parentLoaded bool
	hasRows      bool
	folded       bool
	rows         []*indexRow
	lanes        []uint8 // rows[i] is in lanes[i]
}

// IndexSnapshot is the index at one moment. It is never changed, so lookups need no lock.
type IndexSnapshot struct {
	lanes  [MaxLane + 1]*laneState
	blocks map[uint32]*snapshotBlock
	byHash map[chainhash.Hash]uint32
	// main maps each loaded block on the main chain, folded ones included, to its height, and
	// mainByHeight the other way.
	main         map[uint32]uint32
	mainByHeight map[uint32]uint32
	tipID        uint32
	tipHeight    uint32
	// anchorHeight is the height of the newest folded block, the block every floor's claims rest
	// on; 0 before any fold. anchorID is that block, valid when anchored.
	anchorHeight uint32
	anchored     bool
	anchorID     uint32
	// startHeight is the lowest height on the main chain the index loaded.
	startHeight uint32
	// forkHeights are the heights of off-main blocks the index cannot judge: forgotten ones, whose
	// rows are gone, and loaded ones with no rows.
	forkHeights map[uint32]struct{}
	stats       IndexStats
	mainView    *ChainView
}

// IndexStats is the index's size, for the metrics.
type IndexStats struct {
	// Bytes is the memory the rows and floors hold, as Roaring reports its bitmaps; the Go heap
	// behind them is larger, and the benchmarks measure that.
	Bytes uint64
	// Rows is how many rows the tree holds; Blocks how many blocks the index knows, folded ones
	// included.
	Rows   int
	Blocks int
	// Listed is how many positions below the floors are listed, over every lane.
	Listed uint64
}

// Stats is the snapshot's size.
func (s *IndexSnapshot) Stats() IndexStats { return s.stats }

// TipHeight is the height of the main chain's tip, and AnchorHeight the height of the newest
// folded block (0 before any fold).
func (s *IndexSnapshot) TipHeight() uint32 { return s.tipHeight }

// AnchorHeight is the height of the newest folded block, 0 before any fold.
func (s *IndexSnapshot) AnchorHeight() uint32 { return s.anchorHeight }

// StartHeight is the lowest main-chain height the index loaded.
func (s *IndexSnapshot) StartHeight() uint32 { return s.startHeight }

// BlockState says what the index knows of a block.
type BlockState uint8

const (
	// BlockNotLoaded is a block the index never loaded, or has forgotten.
	BlockNotLoaded BlockState = iota
	// BlockNoRows is a loaded block the map holds no row for: one taken by quick validation, one
	// of our own before block assembly writes rows, or a block of only its coinbase.
	BlockNoRows
	// BlockHasRows is a loaded block with rows.
	BlockHasRows
)

// Block says what the index knows of block id, and its height if loaded.
func (s *IndexSnapshot) Block(id uint32) (BlockState, uint32) {
	b, ok := s.blocks[id]
	if !ok {
		return BlockNotLoaded, 0
	}

	if !b.hasRows {
		return BlockNoRows, b.height
	}

	return BlockHasRows, b.height
}

// MainBlockAt is the loaded main-chain block at height, if any.
func (s *IndexSnapshot) MainBlockAt(height uint32) (uint32, bool) {
	id, ok := s.mainByHeight[height]

	return id, ok
}

// OnMain reports whether block id is a loaded main-chain block.
func (s *IndexSnapshot) OnMain(id uint32) bool {
	_, ok := s.main[id]

	return ok
}

// ChainView is one chain as a reader judges it: the main chain, or the chain behind a block under
// validation, which may leave the main chain at a fork.
type ChainView struct {
	snap *IndexSnapshot
	// fork holds the blocks of the chain that are not on the main chain, by ID to height; nil on
	// the main chain.
	fork map[uint32]uint32
	// joinHeight is the height of the newest main-chain block on the chain.
	joinHeight uint32
	// floorValid is whether the floors hold on this chain: the chain holds the anchor.
	floorValid bool
	// ids, when set, is the chain as a set of block IDs, given by the caller, in place of fork and
	// joinHeight.
	ids map[uint32]bool
	// disconnected is a fork whose ancestry leaves what the index holds before it meets the main
	// chain: the chain's older blocks are unknown to the view.
	disconnected bool
}

// Disconnected reports whether the view is a fork whose older ancestry the index no longer holds,
// so a block missing from it may still be on the chain.
func (v *ChainView) Disconnected() bool { return v.disconnected }

// IDSet reports whether the view is a chain given as a set of block IDs (ViewOfIDs), which a
// caller may have cut to a recent window.
func (v *ChainView) IDSet() bool { return v.ids != nil }

// Snapshot is the view's snapshot.
func (v *ChainView) Snapshot() *IndexSnapshot { return v.snap }

// FloorValid reports whether the floors hold on the chain.
func (v *ChainView) FloorValid() bool { return v.floorValid }

// Height returns block id's height and whether it is on the chain.
func (v *ChainView) Height(id uint32) (uint32, bool) {
	if v.ids != nil {
		b, ok := v.snap.blocks[id]
		if !ok || !v.ids[id] {
			return 0, false
		}

		return b.height, true
	}

	if h, ok := v.fork[id]; ok {
		return h, true
	}

	h, ok := v.snap.main[id]
	if !ok || h > v.joinHeight {
		return 0, false
	}

	return h, true
}

// Main is the view of the main chain.
func (s *IndexSnapshot) Main() *ChainView { return s.mainView }

// ViewFrom is the view of the chain ending at the loaded block hash, or false if hash, or one of
// its ancestors back to the main chain, is not loaded.
func (s *IndexSnapshot) ViewFrom(hash chainhash.Hash) (*ChainView, bool) {
	id, ok := s.byHash[hash]
	if !ok {
		return nil, false
	}

	if h, onMain := s.main[id]; onMain {
		if h == s.tipHeight {
			return s.mainView, true
		}

		return &ChainView{snap: s, joinHeight: h, floorValid: h >= s.anchorHeight}, true
	}

	fork := map[uint32]uint32{}

	for {
		b, loaded := s.blocks[id]
		if !loaded {
			return nil, false
		}

		if h, onMain := s.main[id]; onMain {
			return &ChainView{snap: s, fork: fork, joinHeight: h, floorValid: h >= s.anchorHeight}, true
		}

		fork[id] = b.height

		if !b.parentLoaded {
			// The chain leaves what the index loaded before it meets the main chain.
			return &ChainView{snap: s, fork: fork, floorValid: false, disconnected: true}, true
		}

		id = b.parent
	}
}

// ViewOfIDs is the view of the chain whose blocks are ids, as a caller that already holds its
// chain's block IDs, such as subtree validation's already-mined check, judges it. The floors hold
// only if ids holds the anchor.
func (s *IndexSnapshot) ViewOfIDs(ids map[uint32]bool) *ChainView {
	return &ChainView{snap: s, ids: ids, floorValid: s.anchored && ids[s.anchorID]}
}

// Lookup answers whether the chain holds a block that took the packed lane position pos, and
// which. A pos that is not a valid lane position is not mined.
func (v *ChainView) Lookup(pos uint64) Answer {
	if !Valid(pos) {
		return Answer{}
	}

	laneNum, p := Unpack(pos)

	ls := v.snap.lanes[laneNum]
	if ls == nil {
		return Answer{}
	}

	if v.floorValid && ls.belowFloor(p) {
		return Answer{Mined: true, Via: ViaFloor}
	}

	var a Answer

	ls.covering(p, func(r *indexRow) bool {
		// The bitmap first: of the rows covering p usually only one took it, so the chain test
		// runs about once.
		if !r.takes(p) {
			return true
		}

		h, onChain := v.Height(r.blockID)
		if !onChain {
			return true
		}

		a = Answer{Mined: true, Via: ViaRow, BlockID: r.blockID, Height: h}

		return false
	})

	return a
}

// TakenBefore counts the positions block id took that one of its ancestors on the chain also
// took: a block re-including a mined transaction. byRow counts those an ancestor's row took and
// byFloor those below the floor. hasRows is false when the index holds no rows of the block.
func (v *ChainView) TakenBefore(id uint32) (byRow, byFloor uint64, hasRows bool) {
	b, ok := v.snap.blocks[id]
	if !ok || !b.hasRows || b.folded {
		return 0, 0, false
	}

	for i, own := range b.rows {
		ls := v.snap.lanes[b.lanes[i]]

		if ls == nil {
			continue
		}

		ownBits := own.bitmap()

		if v.floorValid && ls.floor > own.first {
			below := ownBits.Clone()
			below.RemoveRange(ls.floor, MaxPosition+1)

			if ls.listed != nil {
				below.AndNot(ls.listed)
			}

			byFloor += below.GetCardinality()
		}

		for _, r := range ls.rows {
			if r.blockID == id || r.last < own.first || r.first > own.last {
				continue
			}

			// Only the block's ancestors: blocks on the chain below it.
			if h, onChain := v.Height(r.blockID); !onChain || h >= b.height {
				continue
			}

			byRow += ownBits.AndCardinality(r.bitmap())
		}
	}

	return byRow, byFloor, true
}

// TakenOffMain reports whether a loaded block at height that is not on the main chain took the
// packed lane position pos: a fork block, such as one a reorg replaced.
func (s *IndexSnapshot) TakenOffMain(pos uint64, height uint32) bool {
	laneNum, p := Unpack(pos)

	ls := s.lanes[laneNum]
	if ls == nil {
		return false
	}

	found := false

	ls.covering(p, func(r *indexRow) bool {
		b, ok := s.blocks[r.blockID]
		if !ok || b.height != height || !r.takes(p) {
			return true
		}

		if _, onMain := s.main[r.blockID]; onMain {
			return true
		}

		found = true

		return false
	})

	return found
}

// ForkAt reports whether the index has, at height, a block off the main chain it cannot judge: one
// it forgot, such as a reorg's loser the floors passed, or one with no rows. A store that answers
// with a height alone may mean that block. A loaded fork block with rows is judged by TakenOffMain.
func (s *IndexSnapshot) ForkAt(height uint32) bool {
	_, ok := s.forkHeights[height]

	return ok
}

// Folded reports whether block id's rows were folded into the floors and left memory.
func (s *IndexSnapshot) Folded(id uint32) bool {
	b, ok := s.blocks[id]

	return ok && b.folded
}

// ErrDeepReorg is SetTip's answer when the new main chain no longer holds the anchor: a reorg
// deeper than the settle depth, past rows already folded. The index must be rebuilt.
var ErrDeepReorg = errors.NewProcessingError("lane index: the main chain no longer holds the floor's anchor block")

// Index is the writer's side: it loads blocks and moves the tip, and publishes snapshots.
type Index struct {
	mu          sync.Mutex
	settleDepth uint32
	cur         atomic.Pointer[IndexSnapshot]

	blocks    map[uint32]*indexBlockState
	byHash    map[chainhash.Hash]uint32
	lanes     [MaxLane + 1]*laneState
	main      map[uint32]uint32
	tipID     uint32
	tipHeight uint32
	// forkHeights are the heights of off-main blocks forget dropped, kept as long as folded
	// blocks' metadata.
	forkHeights map[uint32]struct{}
	// anchored is whether any block has folded; anchorID and anchorH are the newest folded block.
	anchored bool
	anchorID uint32
	anchorH  uint32
}

type indexBlockState struct {
	IndexBlock
	hasRows bool
	folded  bool
	rows    []*indexRow
	lanes   []uint8 // rows[i] is in lanes[i]
}

// NewIndex returns an empty index whose blocks fold settleDepth blocks below the tip.
func NewIndex(settleDepth uint32) *Index {
	if settleDepth == 0 {
		settleDepth = DefaultSettleDepth
	}

	return &Index{
		settleDepth: settleDepth,
		blocks:      map[uint32]*indexBlockState{},
		byHash:      map[chainhash.Hash]uint32{},
		main:        map[uint32]uint32{},
		forkHeights: map[uint32]struct{}{},
	}
}

// Snapshot is the newest published snapshot, nil before the first SetTip.
func (x *Index) Snapshot() *IndexSnapshot { return x.cur.Load() }

// Has reports whether block hash is loaded.
func (x *Index) Has(hash chainhash.Hash) bool {
	x.mu.Lock()
	defer x.mu.Unlock()

	_, ok := x.byHash[hash]

	return ok
}

// AddBlock loads one block and its rows, which must be the block's every row. Adding a block
// already loaded does nothing. It is published with the next SetTip or Publish.
func (x *Index) AddBlock(b IndexBlock, rows []MapRow) error {
	loaded := make([]*indexRow, len(rows))
	lanes := make([]uint8, len(rows))

	for i, r := range rows {
		ir, err := loadRow(b.ID, r)
		if err != nil {
			return err
		}

		loaded[i], lanes[i] = ir, r.Lane
	}

	x.mu.Lock()
	defer x.mu.Unlock()

	if _, ok := x.blocks[b.ID]; ok {
		return nil
	}

	st := &indexBlockState{IndexBlock: b, hasRows: len(rows) > 0, rows: loaded, lanes: lanes}
	x.blocks[b.ID] = st
	x.byHash[b.Hash] = b.ID

	byLane := map[uint8][]*indexRow{}
	for i, r := range loaded {
		byLane[lanes[i]] = append(byLane[lanes[i]], r)
	}

	for l, add := range byLane {
		x.replaceLane(l, add, nil)
	}

	return nil
}

// loadRow parses one row. Rows come from the blockchain store, which checked them, so this parses
// without checking the encoding again.
func loadRow(blockID uint32, r MapRow) (*indexRow, error) {
	if r.First == 0 || r.First > r.Last || r.Last > MaxPosition {
		return nil, errors.NewProcessingError("lane index: block %d lane %d row %d to %d is not a range", blockID, r.Lane, r.First, r.Last)
	}

	ir := &indexRow{blockID: blockID, first: r.First, last: r.Last, firstHole: r.FirstHole}

	if r.Tag == RowTagRange {
		return ir, nil
	}

	if r.Tag != RowTagRoaring64 {
		return nil, errors.NewProcessingError("lane index: block %d lane %d row has tag %d", blockID, r.Lane, r.Tag)
	}

	bm := roaring64.New()
	if _, err := bm.ReadPortableFrom(bytes.NewReader(r.HoleData)); err != nil {
		return nil, errors.NewProcessingError("lane index: block %d lane %d row's hole data does not parse", blockID, r.Lane, err)
	}

	if bm.IsEmpty() || bm.Minimum() != r.First || bm.Maximum() != r.Last {
		return nil, errors.NewProcessingError("lane index: block %d lane %d row's hole data does not match its range", blockID, r.Lane)
	}

	if ir.bits = plainBits(bm, r.First, r.Last); ir.bits == nil {
		ir.taken = bm
	}

	return ir, nil
}

// replaceLane gives lane l a new state with add's rows added and drop's removed, keeping its floor.
func (x *Index) replaceLane(l uint8, add []*indexRow, drop map[*indexRow]bool) {
	old := x.lanes[l]

	var (
		rows   []*indexRow
		floor  uint64
		listed *roaring64.Bitmap
	)

	if old != nil {
		rows = make([]*indexRow, 0, len(old.rows)+len(add))

		for _, r := range old.rows {
			if !drop[r] {
				rows = append(rows, r)
			}
		}

		floor, listed = old.floor, old.listed
	}

	rows = append(rows, add...)
	x.lanes[l] = newLaneState(rows, floor, listed)
}

// SetTip makes the loaded block hash the main chain's tip, folds every main-chain block at least
// the settle depth below it into the floors, forgets the rows of fork blocks the floors pass, and
// publishes a snapshot. The tip's ancestry must be loaded back to the anchor, or to the first block
// loaded. It returns ErrDeepReorg if the new chain does not hold the anchor; the index is then
// unchanged and must be rebuilt.
func (x *Index) SetTip(hash chainhash.Hash) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	tipID, ok := x.byHash[hash]
	if !ok {
		return errors.NewProcessingError("lane index: tip %s is not loaded", hash.String())
	}

	main := map[uint32]uint32{}

	for id := tipID; ; {
		b, loaded := x.blocks[id]
		if !loaded {
			break
		}

		main[id] = b.Height

		parent, ok := x.byHash[b.Parent]
		if !ok || parent == id {
			// The walk leaves what the index loaded; a block naming itself as parent ends it too.
			break
		}

		id = parent
	}

	if x.anchored {
		if _, ok := main[x.anchorID]; !ok {
			return ErrDeepReorg
		}
	}

	x.main, x.tipID, x.tipHeight = main, tipID, x.blocks[tipID].Height

	x.fold()
	x.forget()
	x.publish()

	return nil
}

// fold folds every unfolded main-chain block at least the settle depth below the tip into the
// floors, lowest first.
func (x *Index) fold() {
	if x.tipHeight < x.settleDepth {
		return
	}

	limit := x.tipHeight - x.settleDepth

	var due []*indexBlockState

	for id, h := range x.main {
		if b := x.blocks[id]; h <= limit && !b.folded {
			due = append(due, b)
		}
	}

	if len(due) == 0 {
		return
	}

	slices.SortFunc(due, func(a, b *indexBlockState) int { return int(a.Height) - int(b.Height) })

	// Fold every due block into fresh floors, then rebuild each lane it touched once.
	type laneFold struct {
		floor  uint64
		listed *roaring64.Bitmap
		drop   map[*indexRow]bool
	}

	folds := map[uint8]*laneFold{}

	for _, b := range due {
		for i, r := range b.rows {
			l := b.lanes[i]

			f, ok := folds[l]
			if !ok {
				f = &laneFold{drop: map[*indexRow]bool{}}

				if old := x.lanes[l]; old != nil {
					f.floor = old.floor

					if old.listed != nil {
						f.listed = old.listed.Clone()
					}
				}

				if f.listed == nil {
					f.listed = roaring64.New()
				}

				folds[l] = f
			}

			if f.floor == 0 {
				// Nothing below this row was seen to settle: list all of it.
				f.floor = FirstPosition
			}

			if r.last+1 > f.floor {
				f.listed.AddRange(f.floor, r.last+1)
				f.floor = r.last + 1
			}

			if r.bits == nil && r.taken == nil {
				f.listed.RemoveRange(r.first, r.last+1)
			} else {
				f.listed.AndNot(r.bitmap())
			}

			f.drop[r] = true
		}

		b.folded = true
		b.rows, b.lanes = nil, nil

		if !x.anchored || b.Height >= x.anchorH {
			x.anchored, x.anchorID, x.anchorH = true, b.ID, b.Height
		}
	}

	for l, f := range folds {
		f.listed.RunOptimize()

		old := x.lanes[l]
		rows := make([]*indexRow, 0, len(old.rows))

		for _, r := range old.rows {
			if !f.drop[r] {
				rows = append(rows, r)
			}
		}

		x.lanes[l] = newLaneState(rows, f.floor, f.listed)
	}
}

// forget drops the rows of fork blocks at or below the anchor, which no chain holding the anchor
// reaches, and the metadata of blocks far below the tip.
func (x *Index) forget() {
	drops := map[uint8]map[*indexRow]bool{}

	for id, b := range x.blocks {
		_, onMain := x.main[id]

		if !onMain && x.anchored && b.Height <= x.anchorH {
			x.forkHeights[b.Height] = struct{}{}

			for i, r := range b.rows {
				if drops[b.lanes[i]] == nil {
					drops[b.lanes[i]] = map[*indexRow]bool{}
				}

				drops[b.lanes[i]][r] = true
			}

			delete(x.blocks, id)
			delete(x.byHash, b.Hash)

			continue
		}

		if b.folded && x.tipHeight > keepFoldedBlocks && b.Height < x.tipHeight-keepFoldedBlocks {
			delete(x.blocks, id)
			delete(x.byHash, b.Hash)
			delete(x.main, id)
		}
	}

	for l, d := range drops {
		x.replaceLane(l, nil, d)
	}

	for h := range x.forkHeights {
		if x.tipHeight > keepFoldedBlocks && h < x.tipHeight-keepFoldedBlocks {
			delete(x.forkHeights, h)
		}
	}
}

// Publish publishes a snapshot of what the index holds now, for blocks added since the last
// SetTip, such as a fork block's ancestry loaded for its validation.
func (x *Index) Publish() {
	x.mu.Lock()
	defer x.mu.Unlock()

	// Genesis has block ID 0, so a published snapshot, not the tip's ID, says the index has a tip.
	if x.cur.Load() != nil {
		x.publish()
	}
}

func (x *Index) publish() {
	s := &IndexSnapshot{
		lanes:        x.lanes,
		blocks:       make(map[uint32]*snapshotBlock, len(x.blocks)),
		byHash:       make(map[chainhash.Hash]uint32, len(x.byHash)),
		main:         make(map[uint32]uint32, len(x.main)),
		mainByHeight: make(map[uint32]uint32, len(x.main)),
		tipID:        x.tipID,
		tipHeight:    x.tipHeight,
		anchorHeight: x.anchorH,
		anchored:     x.anchored,
		anchorID:     x.anchorID,
		startHeight:  x.tipHeight,
	}

	for h, id := range x.byHash {
		s.byHash[h] = id
	}

	for id, h := range x.main {
		s.main[id] = h
		s.mainByHeight[h] = id
		s.startHeight = min(s.startHeight, h)
	}

	for id, b := range x.blocks {
		sb := &snapshotBlock{id: id, height: b.Height, hasRows: b.hasRows, folded: b.folded, rows: b.rows, lanes: b.lanes}

		if p, ok := x.byHash[b.Parent]; ok && p != id {
			sb.parent, sb.parentLoaded = p, true
		}

		s.blocks[id] = sb
	}

	s.stats.Blocks = len(x.blocks)

	s.forkHeights = make(map[uint32]struct{}, len(x.forkHeights))
	for h := range x.forkHeights {
		s.forkHeights[h] = struct{}{}
	}

	// A loaded fork block with no rows cannot be judged either.
	for id, b := range x.blocks {
		if _, onMain := x.main[id]; !onMain && !b.hasRows {
			s.forkHeights[b.Height] = struct{}{}
		}
	}

	for _, ls := range x.lanes {
		if ls == nil {
			continue
		}

		s.stats.Rows += len(ls.rows)
		s.stats.Bytes += uint64(len(ls.rows)) * 8 // maxLast

		for _, r := range ls.rows {
			s.stats.Bytes += r.bytes()
		}

		if ls.listed != nil {
			s.stats.Bytes += ls.listed.GetSizeInBytes()
			s.stats.Listed += ls.listed.GetCardinality()
		}
	}

	s.stats.Bytes += uint64(len(x.blocks)) * 128 // block metadata and the snapshot's maps

	s.mainView = &ChainView{snap: s, joinHeight: s.tipHeight, floorValid: true}
	x.cur.Store(s)
}

package lane

import (
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// testChain builds blocks for index tests: block hashes are derived from IDs, so a test names
// blocks by ID alone.
type testChain struct {
	t   *testing.T
	idx *Index
}

func blockHash(id uint32) chainhash.Hash {
	var h chainhash.Hash
	h[0], h[1], h[2], h[3] = byte(id), byte(id>>8), byte(id>>16), byte(id>>24)
	h[31] = 1

	return h
}

func newTestChain(t *testing.T, settleDepth uint32) *testChain {
	return &testChain{t: t, idx: NewIndex(settleDepth)}
}

// rowsOf builds the rows of one lane taking positions.
func rowsOf(t *testing.T, laneNum uint8, positions ...uint64) []MapRow {
	bm := roaring64.New()
	bm.AddMany(positions)

	rows, err := BuildMapRows(laneNum, bm)
	require.NoError(t, err)

	return rows
}

func rangeRows(t *testing.T, laneNum uint8, first, last uint64) []MapRow {
	bm := roaring64.New()
	bm.AddRange(first, last+1)

	rows, err := BuildMapRows(laneNum, bm)
	require.NoError(t, err)

	return rows
}

// add loads block id at height on parent with rows.
func (c *testChain) add(id, parent, height uint32, rows ...[]MapRow) {
	var all []MapRow
	for _, r := range rows {
		all = append(all, r...)
	}

	require.NoError(c.t, c.idx.AddBlock(IndexBlock{ID: id, Height: height, Hash: blockHash(id), Parent: blockHash(parent)}, all))
}

// line loads blocks from..to as a line, each block's ID its height, with no rows unless given.
func (c *testChain) line(from, to uint32) {
	for h := from; h <= to; h++ {
		c.add(h, h-1, h)
	}
}

func (c *testChain) tip(id uint32) {
	require.NoError(c.t, c.idx.SetTip(blockHash(id)))
}

func (c *testChain) main() *ChainView { return c.idx.Snapshot().Main() }

func TestIndexLookupOverlappingRows(t *testing.T) {
	c := newTestChain(t, 100)

	// Block 10 took 1 to 1,000 except 977; block 11 took 977 and 1,001 to 1,400.
	took10 := roaring64.New()
	took10.AddRange(1, 1001)
	took10.Remove(977)
	rows10, err := BuildMapRows(7, took10)
	require.NoError(t, err)

	took11 := roaring64.New()
	took11.Add(977)
	took11.AddRange(1001, 1401)
	rows11, err := BuildMapRows(7, took11)
	require.NoError(t, err)

	c.add(10, 9, 10, rows10)
	c.add(11, 10, 11, rows11)
	c.tip(11)

	v := c.main()

	require.Equal(t, Answer{Mined: true, Via: ViaRow, BlockID: 10, Height: 10}, v.Lookup(Pack(7, 150)))
	require.Equal(t, Answer{Mined: true, Via: ViaRow, BlockID: 11, Height: 11}, v.Lookup(Pack(7, 977)))
	require.Equal(t, Answer{Mined: true, Via: ViaRow, BlockID: 11, Height: 11}, v.Lookup(Pack(7, 1400)))
	require.Equal(t, Answer{}, v.Lookup(Pack(7, 1401)))
	require.Equal(t, Answer{}, v.Lookup(Pack(8, 150)), "another lane")
}

func TestIndexFoldsSettledBlocksIntoTheFloor(t *testing.T) {
	c := newTestChain(t, 3)

	// Block 1 takes 10 to 20 but not 15; positions 1 to 9 were never seen.
	c.add(1, 0, 1, rowsOf(t, 2, 10, 11, 12, 13, 14, 16, 17, 18, 19, 20))
	c.line(2, 3)
	c.tip(3)

	require.Equal(t, ViaRow, c.main().Lookup(Pack(2, 12)).Via)
	require.Equal(t, uint32(0), c.idx.Snapshot().AnchorHeight())

	c.line(4, 4)
	c.tip(4) // block 1 is now 3 deep: folded

	snap := c.idx.Snapshot()
	require.Equal(t, uint32(1), snap.AnchorHeight())
	require.Equal(t, 0, snap.Stats().Rows, "the folded row left the tree")

	v := c.main()
	require.Equal(t, Answer{Mined: true, Via: ViaFloor}, v.Lookup(Pack(2, 12)))
	require.Equal(t, Answer{Mined: true, Via: ViaFloor}, v.Lookup(Pack(2, 20)))
	require.Equal(t, Answer{}, v.Lookup(Pack(2, 15)), "a hole stays listed")
	require.Equal(t, Answer{}, v.Lookup(Pack(2, 5)), "below the first row seen is listed")
	require.Equal(t, Answer{}, v.Lookup(Pack(2, 21)), "above the floor")
	require.Equal(t, uint64(9+1), snap.Stats().Listed)

	state, height := snap.Block(1)
	require.Equal(t, BlockHasRows, state)
	require.Equal(t, uint32(1), height)
}

// TestIndexLateMinedPositionKeepsItsRow is the design's day-5 example: a position listed below the
// floor is taken by a late block, whose row answers until that block settles.
func TestIndexLateMinedPositionKeepsItsRow(t *testing.T) {
	c := newTestChain(t, 2)

	// Block 1 takes 1 to 1,000 except 600 (stuck transaction T).
	took := roaring64.New()
	took.AddRange(1, 1001)
	took.Remove(600)
	rows1, err := BuildMapRows(7, took)
	require.NoError(t, err)

	c.add(1, 0, 1, rows1)
	c.line(2, 3)
	c.tip(3)

	require.Equal(t, Answer{}, c.main().Lookup(Pack(7, 600)))
	require.Equal(t, ViaFloor, c.main().Lookup(Pack(7, 599)).Via)

	// Block 4 takes T: a row entirely below the floor.
	c.add(4, 3, 4, rowsOf(t, 7, 600))
	c.tip(4)
	require.Equal(t, Answer{Mined: true, Via: ViaRow, BlockID: 4, Height: 4}, c.main().Lookup(Pack(7, 600)))

	c.line(5, 6)
	c.tip(6) // block 4 settles and folds: 600 leaves the list

	require.Equal(t, Answer{Mined: true, Via: ViaFloor}, c.main().Lookup(Pack(7, 600)))
	require.Equal(t, uint64(0), c.idx.Snapshot().Stats().Listed)
}

func TestIndexReorgMovesTheAnswer(t *testing.T) {
	c := newTestChain(t, 100)

	c.line(1, 5)
	// Block 6 and its rival 106 at the same height take different positions of lane 3.
	c.add(6, 5, 6, rowsOf(t, 3, 1, 2, 3))
	c.add(106, 5, 6, rowsOf(t, 3, 2, 3, 4))
	c.tip(6)

	require.Equal(t, Answer{Mined: true, Via: ViaRow, BlockID: 6, Height: 6}, c.main().Lookup(Pack(3, 1)))
	require.Equal(t, Answer{}, c.main().Lookup(Pack(3, 4)), "the rival's row does not count off its chain")

	c.add(107, 106, 7)
	c.tip(107)

	require.Equal(t, Answer{}, c.main().Lookup(Pack(3, 1)))
	require.Equal(t, Answer{Mined: true, Via: ViaRow, BlockID: 106, Height: 6}, c.main().Lookup(Pack(3, 4)))
	require.Equal(t, Answer{Mined: true, Via: ViaRow, BlockID: 106, Height: 6}, c.main().Lookup(Pack(3, 2)))
	require.False(t, c.idx.Snapshot().OnMain(6))
}

func TestIndexViewFromAForkBlock(t *testing.T) {
	c := newTestChain(t, 100)

	c.line(1, 5)
	c.add(6, 5, 6, rowsOf(t, 3, 1))
	c.add(106, 5, 6, rowsOf(t, 3, 2))
	c.add(107, 106, 7)
	c.tip(6)

	// Judging a child of 107: the fork's rows count, the main chain's above the fork do not.
	v, ok := c.idx.Snapshot().ViewFrom(blockHash(107))
	require.True(t, ok)
	require.Equal(t, Answer{Mined: true, Via: ViaRow, BlockID: 106, Height: 6}, v.Lookup(Pack(3, 2)))
	require.Equal(t, Answer{}, v.Lookup(Pack(3, 1)))

	// A main-chain block below the tip sees only its own ancestry.
	v, ok = c.idx.Snapshot().ViewFrom(blockHash(5))
	require.True(t, ok)
	require.Equal(t, Answer{}, v.Lookup(Pack(3, 1)))

	_, ok = c.idx.Snapshot().ViewFrom(blockHash(999))
	require.False(t, ok)
}

func TestIndexDeepReorgPastTheAnchorIsRefused(t *testing.T) {
	c := newTestChain(t, 2)

	c.line(1, 5)
	c.tip(5) // blocks 1 to 3 folded, anchor 3

	c.add(103, 2, 3)
	c.add(104, 103, 4)
	c.add(105, 104, 5)
	c.add(106, 105, 6)

	require.ErrorIs(t, c.idx.SetTip(blockHash(106)), ErrDeepReorg)
	require.True(t, c.idx.Snapshot().OnMain(5), "the index is unchanged")
}

func TestIndexForgetsForkRowsThePassesFloor(t *testing.T) {
	c := newTestChain(t, 2)

	c.line(1, 2)
	c.add(102, 1, 2, rowsOf(t, 1, 5))
	c.add(103, 102, 3, rowsOf(t, 1, 6))
	c.add(104, 103, 4, rowsOf(t, 1, 7))
	c.line(3, 5)
	c.tip(5) // anchor 3

	s := c.idx.Snapshot()

	state, _ := s.Block(102)
	require.Equal(t, BlockNotLoaded, state, "a fork block below the anchor is forgotten")

	state, _ = s.Block(103)
	require.Equal(t, BlockNotLoaded, state, "a fork block at the anchor's height is forgotten")

	state, _ = s.Block(104)
	require.Equal(t, BlockHasRows, state, "a fork block above the anchor stays")
	require.Equal(t, 1, s.Stats().Rows)
}

// A chain that leaves the main chain below the anchor cannot use the floors: the floor's claims
// rest on the anchor, which that chain does not hold.
func TestIndexFloorDoesNotHoldOnAForkBelowTheAnchor(t *testing.T) {
	c := newTestChain(t, 2)

	c.add(1, 0, 1, rangeRows(t, 4, 1, 100))
	c.line(2, 5)
	c.tip(5) // anchor 3: block 1 folded, 1 to 100 below the floor

	require.Equal(t, ViaFloor, c.main().Lookup(Pack(4, 50)).Via)

	// Block 102 forks from block 1, below the anchor, and re-takes 50.
	c.add(102, 1, 2, rowsOf(t, 4, 50))
	c.idx.Publish()

	v, ok := c.idx.Snapshot().ViewFrom(blockHash(102))
	require.True(t, ok)
	require.False(t, v.FloorValid())
	require.Equal(t, Answer{Mined: true, Via: ViaRow, BlockID: 102, Height: 2}, v.Lookup(Pack(4, 50)), "only its own row answers")
	require.Equal(t, Answer{}, v.Lookup(Pack(4, 60)), "the floor does not")
	require.False(t, v.Disconnected())
	require.Equal(t, Answer{}, c.main().Lookup(0), "position 0 is no position, even below a floor")

	byRow, byFloor, hasRows := v.TakenBefore(102)
	require.True(t, hasRows)
	require.Equal(t, uint64(0), byRow)
	require.Equal(t, uint64(0), byFloor, "the floor is not counted on this chain")
	require.True(t, c.idx.Snapshot().Folded(1))
	require.False(t, c.idx.Snapshot().Folded(102))

	// After the next tip, the fork's ancestry below the anchor is gone: the chain leaves what the
	// index holds, and the floor still does not hold.
	c.add(103, 102, 3, rowsOf(t, 4, 60))
	c.add(104, 103, 4)
	c.tip(5)

	v, ok = c.idx.Snapshot().ViewFrom(blockHash(104))
	require.True(t, ok)
	require.False(t, v.FloorValid())
	require.True(t, v.Disconnected(), "its ancestry below the anchor is gone")
	require.Equal(t, Answer{}, v.Lookup(Pack(4, 50)))
}

func TestIndexBlockWithoutRows(t *testing.T) {
	c := newTestChain(t, 100)

	c.line(1, 2)
	c.tip(2)

	state, height := c.idx.Snapshot().Block(2)
	require.Equal(t, BlockNoRows, state)
	require.Equal(t, uint32(2), height)

	state, _ = c.idx.Snapshot().Block(77)
	require.Equal(t, BlockNotLoaded, state)
}

func TestIndexTakenBefore(t *testing.T) {
	c := newTestChain(t, 2)

	c.add(1, 0, 1, rangeRows(t, 4, 1, 100))
	c.line(2, 3)
	c.add(4, 3, 4, rowsOf(t, 4, 101, 102))
	c.tip(4) // block 1 folded: 1 to 100 below the floor

	// Block 5 takes 50 (below the floor), 102 (block 4's) and 103 (new).
	c.add(5, 4, 5, rowsOf(t, 4, 50, 102, 103))
	c.tip(5)

	byRow, byFloor, hasRows := c.main().TakenBefore(5)
	require.True(t, hasRows)
	require.Equal(t, uint64(1), byRow, "102, block 4's")
	require.Equal(t, uint64(1), byFloor, "50, below the floor")

	byRow, byFloor, hasRows = c.main().TakenBefore(4)
	require.True(t, hasRows)
	require.Equal(t, uint64(0), byRow+byFloor)

	c.add(6, 5, 6)
	c.tip(6)

	_, _, hasRows = c.main().TakenBefore(6)
	require.False(t, hasRows)
}

func TestIndexTakenOffMain(t *testing.T) {
	c := newTestChain(t, 100)

	c.line(1, 1)
	c.add(2, 1, 2, rowsOf(t, 3, 7))
	c.add(102, 1, 2, rowsOf(t, 3, 8))
	c.tip(2)

	s := c.idx.Snapshot()
	require.True(t, s.TakenOffMain(Pack(3, 8), 2), "the fork block at height 2 took 8")
	require.False(t, s.TakenOffMain(Pack(3, 7), 2), "the main block took 7")
	require.False(t, s.TakenOffMain(Pack(3, 8), 3), "another height")
	require.False(t, s.TakenOffMain(Pack(5, 8), 2), "another lane")
}

func TestIndexIntervalTreeFindsEveryCoveringRow(t *testing.T) {
	// Many overlapping rows in one lane: every lookup must find the row that took the position,
	// whatever the tree's shape.
	c := newTestChain(t, 1_000)

	var parent uint32

	for id := uint32(1); id <= 60; id++ {
		// Row id takes every position p in 1..2,000 with p % 60 == id-1: wide, fully overlapping.
		var positions []uint64

		for p := uint64(1); p <= 2_000; p++ {
			if p%60 == uint64(id-1) {
				positions = append(positions, p)
			}
		}

		c.add(id, parent, id, rowsOf(t, 9, positions...))
		parent = id
	}

	c.tip(60)

	v := c.main()

	for p := uint64(1); p <= 2_000; p++ {
		want := uint32(p%60) + 1
		require.Equal(t, Answer{Mined: true, Via: ViaRow, BlockID: want, Height: want}, v.Lookup(Pack(9, p)), "position %d", p)
	}
}

// A view given as a set of block IDs judges on exactly those blocks, fork blocks included, and
// the floors hold only if the set holds the anchor.
func TestIndexViewOfIDs(t *testing.T) {
	c := newTestChain(t, 2)

	c.add(1, 0, 1, rangeRows(t, 4, 1, 100))
	c.line(2, 5)
	c.add(106, 5, 6, rowsOf(t, 4, 200))
	c.add(6, 5, 6)
	c.tip(6) // anchor 4

	s := c.idx.Snapshot()

	v := s.ViewOfIDs(map[uint32]bool{1: true, 2: true, 3: true, 4: true, 5: true, 106: true})
	require.True(t, v.FloorValid())
	require.Equal(t, Answer{Mined: true, Via: ViaRow, BlockID: 106, Height: 6}, v.Lookup(Pack(4, 200)), "the fork block in the set")
	require.Equal(t, Answer{Mined: true, Via: ViaFloor}, v.Lookup(Pack(4, 50)))

	v = s.ViewOfIDs(map[uint32]bool{5: true, 106: true})
	require.False(t, v.FloorValid(), "the set does not hold the anchor")
	require.Equal(t, Answer{}, v.Lookup(Pack(4, 50)))

	require.Equal(t, Answer{}, s.ViewOfIDs(map[uint32]bool{6: true}).Lookup(Pack(4, 200)), "the fork block is not in the set")
}

// Genesis is block ID 0: an index whose tip is genesis still publishes the blocks added after it.
func TestIndexPublishesWithGenesisAsTheTip(t *testing.T) {
	c := newTestChain(t, 100)

	// Genesis's parent is the all-zero hash, which names no block.
	require.NoError(t, c.idx.AddBlock(IndexBlock{ID: 0, Height: 0, Hash: blockHash(0)}, nil))
	c.tip(0)

	c.add(1, 0, 1, rowsOf(t, 3, 7))
	c.idx.Publish()

	v, ok := c.idx.Snapshot().ViewFrom(blockHash(1))
	require.True(t, ok)
	require.Equal(t, Answer{Mined: true, Via: ViaRow, BlockID: 1, Height: 1}, v.Lookup(Pack(3, 7)))
}

// A block that names itself as its parent ends the walk along the chain instead of looping.
func TestIndexStopsAtABlockNamingItselfAsParent(t *testing.T) {
	c := newTestChain(t, 100)

	c.add(5, 5, 5)
	c.tip(5)

	require.True(t, c.idx.Snapshot().OnMain(5))

	v, ok := c.idx.Snapshot().ViewFrom(blockHash(5))
	require.True(t, ok)
	require.Equal(t, Answer{}, v.Lookup(Pack(1, 1)))
}

// Loading a block twice changes nothing: its rows are not added to the lanes again.
func TestIndexAddBlockTwiceAddsItsRowsOnce(t *testing.T) {
	c := newTestChain(t, 100)

	c.add(1, 0, 1, rowsOf(t, 3, 7))
	c.add(1, 0, 1, rowsOf(t, 3, 8))
	c.tip(1)

	require.Equal(t, 1, c.idx.Snapshot().Stats().Rows)
	require.Equal(t, Answer{}, c.main().Lookup(Pack(3, 8)), "the second load's rows are ignored")
}

// A row whose hole data does not parse is refused.
func TestIndexRefusesHoleDataThatDoesNotParse(t *testing.T) {
	idx := NewIndex(100)

	err := idx.AddBlock(IndexBlock{ID: 1, Height: 1, Hash: blockHash(1)}, []MapRow{{Lane: 3, First: 5, Last: 9, FirstHole: 6, Tag: RowTagRoaring64, HoleData: []byte{1, 2, 3}}})
	require.Error(t, err)
	require.False(t, idx.Has(blockHash(1)), "a refused block is not loaded")
}

// A refused deep reorg leaves the writer's state as it was: the next publish still has the old
// main chain.
func TestIndexRefusedDeepReorgLeavesTheWriterUnchanged(t *testing.T) {
	c := newTestChain(t, 2)

	c.line(1, 5)
	c.tip(5)

	c.add(103, 2, 3)
	c.add(104, 103, 4)
	c.add(105, 104, 5)
	c.add(106, 105, 6)
	require.ErrorIs(t, c.idx.SetTip(blockHash(106)), ErrDeepReorg)

	c.idx.Publish()

	s := c.idx.Snapshot()
	require.True(t, s.OnMain(5))
	require.False(t, s.OnMain(106))
	require.Equal(t, uint32(5), s.TipHeight())
}

// A main-chain block below the anchor does not hold the anchor, so the floors do not hold on the
// chain ending at it.
func TestIndexFloorDoesNotHoldOnAMainBlockBelowTheAnchor(t *testing.T) {
	c := newTestChain(t, 2)

	c.add(1, 0, 1, rangeRows(t, 4, 1, 100))
	c.line(2, 5)
	c.tip(5) // anchor 3

	v, ok := c.idx.Snapshot().ViewFrom(blockHash(2))
	require.True(t, ok)
	require.False(t, v.FloorValid())
	require.Equal(t, Answer{}, v.Lookup(Pack(4, 50)))

	v, ok = c.idx.Snapshot().ViewFrom(blockHash(3))
	require.True(t, ok)
	require.True(t, v.FloorValid())
}

// Many goroutines loading, moving the tip and publishing at once, with lookups running, share no
// state unsafely (run with -race).
func TestIndexConcurrentWritersAndReaders(t *testing.T) {
	c := newTestChain(t, 5)
	c.add(1, 0, 1, rowsOf(t, 2, 1))
	c.tip(1)

	var wg sync.WaitGroup

	for w := uint32(0); w < 4; w++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := uint32(0); i < 50; i++ {
				id := 1_000*(w+1) + i
				_ = c.idx.AddBlock(IndexBlock{ID: id, Height: 2, Hash: blockHash(id), Parent: blockHash(1)}, rowsOf(t, uint8(w), uint64(i)+1)) //nolint:gosec // w is small
				c.idx.Publish()
				_ = c.idx.Has(blockHash(id))
			}
		}()
	}

	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 0; i < 200; i++ {
			if s := c.idx.Snapshot(); s != nil {
				_ = s.Main().Lookup(Pack(2, 1))
			}
		}
	}()

	wg.Wait()
	c.tip(1)
}

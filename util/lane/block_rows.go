package lane

import (
	"sort"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// The Phase 1 block gate, at utxostore_laneMigration shadow: a subtree's writer collects the lane
// positions of its transactions into one Roaring bitmap per lane (BitmapsBuilder) and saves them
// as the subtree lane bitmaps file; the block's writer merges its subtrees' bitmaps lane by lane
// and turns each lane's union into map rows (BuildBlockRows). Readers load the rows at shadow
// (util/lanereader) and only count agreement, so a transaction without a position is counted,
// never refused.

// BitmapsBuilder collects one subtree's lane positions by lane. The zero value is not usable;
// call NewBitmapsBuilder.
type BitmapsBuilder struct {
	byLane       map[uint8][]uint64
	unpositioned int
}

// NewBitmapsBuilder starts an empty collection.
func NewBitmapsBuilder() *BitmapsBuilder {
	return &BitmapsBuilder{byLane: map[uint8][]uint64{}}
}

// Add records packed lane position v. A position that is not valid, zero for a transaction
// stored before positions were on, is counted as unpositioned and left out.
func (b *BitmapsBuilder) Add(v uint64) {
	if !Valid(v) {
		b.unpositioned++
		return
	}

	l, p := Unpack(v)
	b.byLane[l] = append(b.byLane[l], p)
}

// Unpositioned is how many transactions Add was given without a valid position.
func (b *BitmapsBuilder) Unpositioned() int {
	return b.unpositioned
}

// Subtree returns the collected positions as the subtree lane bitmaps of root. A lane nothing was
// added to is absent, so a subtree of unpositioned transactions has no lanes.
func (b *BitmapsBuilder) Subtree(root chainhash.Hash) SubtreeBitmaps {
	lanes := make(LaneBitmaps, len(b.byLane))

	for l, positions := range b.byLane {
		bm := roaring64.New()
		bm.AddMany(positions)
		bm.RunOptimize()
		lanes[l] = bm
	}

	return SubtreeBitmaps{SubtreeHash: root, Lanes: lanes}
}

// BlockRows is one block's map rows and what was measured building them.
type BlockRows struct {
	Rows []MapRow
	// Members is the number of positions the rows hold, every lane together.
	Members uint64
	// Lanes is, per lane the block took from, the bytes its rows store and the holes in each of
	// its rows, in rising lane order.
	Lanes []LaneRowStats
}

// LaneRowStats is what one lane's rows of one block cost.
type LaneRowStats struct {
	Lane uint8
	// Bytes is the stored size of the lane's rows: RowFixedBytes per row plus its hole data.
	Bytes int
	// Holes is, per row, the positions between its first and last that the block did not take.
	Holes []uint64
}

// RowFixedBytes is what a map row stores besides its hole data: block ID, lane, first, last,
// first hole and tag, at their SQL widths (8, 2, 8, 8, 8 and 2 bytes).
const RowFixedBytes = 36

// BuildBlockRows merges the block's subtrees' bitmaps lane by lane and builds each lane's map
// rows, in rising lane order, splitting a lane past MaxRowHoleDataBytes as BuildMapRows does. The
// subtrees' bitmaps are not modified. A block whose subtrees hold no position has no rows.
func BuildBlockRows(subtrees []LaneBitmaps) (BlockRows, error) {
	perLane := map[uint8][]*roaring64.Bitmap{}

	for _, s := range subtrees {
		for l, bm := range s {
			if bm != nil && !bm.IsEmpty() {
				perLane[l] = append(perLane[l], bm)
			}
		}
	}

	lanes := make([]int, 0, len(perLane))
	for l := range perLane {
		lanes = append(lanes, int(l))
	}

	sort.Ints(lanes)

	var out BlockRows

	for _, k := range lanes {
		l := uint8(k) //nolint:gosec // k came from a uint8

		union := roaring64.FastOr(perLane[l]...)

		rows, err := BuildMapRows(l, union)
		if err != nil {
			return BlockRows{}, errors.NewProcessingError("lane %d rows", l, err)
		}

		stats := LaneRowStats{Lane: l, Holes: make([]uint64, len(rows))}

		for i, r := range rows {
			stats.Bytes += RowFixedBytes + len(r.HoleData)

			stats.Holes[i] = rowHoles(union, r)
		}

		out.Members += union.GetCardinality()
		out.Rows = append(out.Rows, rows...)
		out.Lanes = append(out.Lanes, stats)
	}

	return out, nil
}

// rowHoles is how many positions between r's first and last its block did not take, counted from
// union, the lane's positions that every row of the lane was cut from.
func rowHoles(union *roaring64.Bitmap, r MapRow) uint64 {
	taken := union.Rank(r.Last)
	if r.First > 0 {
		taken -= union.Rank(r.First - 1)
	}

	return r.Last - r.First + 1 - taken
}

package lane

import (
	"bytes"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/teranode/errors"
)

// RowTag says how a map row stores which positions of its range its block took. It is one byte so
// a later codec can be added per row without migrating the rows already stored.
type RowTag uint8

const (
	// RowTagRange is a row with no holes: its block took every position from First to Last. It
	// has no hole data.
	RowTagRange RowTag = 0

	// RowTagRoaring64 is a row with holes. Its hole data is a 64-bit Roaring bitmap, in the
	// portable 64-bit format, of the positions the block took.
	RowTagRoaring64 RowTag = 1
)

// MaxRowHoleDataBytes is the largest hole data one map row holds. A block whose bitmap for a
// lane would be larger is split into several rows for that lane, each over a run of whole
// 65,536-position chunks.
const MaxRowHoleDataBytes = 64 << 20

// chunkBits is the width of a Roaring container: positions are split into chunks of 65,536.
const chunkBits = 16

// MapRow is one row of the map: the positions one block took from one lane, or from a run of
// whole chunks of that lane when the block's bitmap for the lane is split. Positions here are the
// seven-byte position within the lane, not the packed lane position. A row is immutable once
// written.
type MapRow struct {
	Lane uint8
	// First and Last are the lowest and highest positions the block took in this row's part of
	// the lane. Both are taken.
	First uint64
	Last  uint64
	// FirstHole is the lowest position between First and Last the block did not take, or 0 when
	// it took all of them.
	FirstHole uint64
	Tag       RowTag
	// HoleData is the serialized bitmap of the positions taken, for RowTagRoaring64; nil for
	// RowTagRange.
	HoleData []byte
}

// BuildMapRows turns the positions one block took from one lane into its map rows: one row,
// unless the serialized bitmap is larger than MaxRowHoleDataBytes, when it is split into rows
// over runs of whole chunks, in rising order. taken is not modified. It is an error for taken to
// be empty or to hold position 0 or a position wider than seven bytes.
func BuildMapRows(lane uint8, taken *roaring64.Bitmap) ([]MapRow, error) {
	return buildMapRows(lane, taken, MaxRowHoleDataBytes)
}

func buildMapRows(lane uint8, taken *roaring64.Bitmap, limit uint64) ([]MapRow, error) {
	if taken == nil || taken.IsEmpty() {
		return nil, errors.NewProcessingError("lane %d: no positions taken", lane)
	}

	lowest, highest := taken.Minimum(), taken.Maximum()
	if lowest == 0 || highest > MaxPosition {
		return nil, errors.NewProcessingError("lane %d: taken positions %d to %d are outside 1 to %d", lane, lowest, highest, MaxPosition)
	}

	bm := taken.Clone()
	bm.RunOptimize()

	if isFullRange(bm, lowest, highest) {
		return []MapRow{{Lane: lane, First: lowest, Last: highest, Tag: RowTagRange}}, nil
	}

	if bm.GetSerializedSizeInBytes() <= limit {
		row, err := rowFromBitmap(lane, bm)
		if err != nil {
			return nil, err
		}

		return []MapRow{row}, nil
	}

	var rows []MapRow

	for start := lowest; ; {
		part, next, err := takeChunks(bm, start, highest, limit)
		if err != nil {
			return nil, errors.NewProcessingError("lane %d", lane, err)
		}

		row, err := rowFromBitmap(lane, part)
		if err != nil {
			return nil, err
		}

		rows = append(rows, row)

		if next == 0 {
			return rows, nil
		}

		start = next
	}
}

// takeChunks returns the bitmap of the most whole chunks, starting at start's chunk, whose
// serialized size is at most limit, and the first taken position after them, or 0 when none is
// left. Every chunk holds at most 65,536 positions, so one chunk is always below any limit a
// caller uses outside tests.
func takeChunks(bm *roaring64.Bitmap, start, highest, limit uint64) (*roaring64.Bitmap, uint64, error) {
	firstChunk := start >> chunkBits
	lastChunk := highest >> chunkBits

	part := func(endChunk uint64) *roaring64.Bitmap {
		rng := roaring64.New()
		rng.AddRange(firstChunk<<chunkBits, (endChunk+1)<<chunkBits)
		rng.And(bm)

		return rng
	}

	// The largest endChunk in [firstChunk, lastChunk] whose part fits.
	lo, hi := firstChunk, lastChunk
	best := uint64(0)
	found := false

	for lo <= hi {
		mid := lo + (hi-lo)/2

		if part(mid).GetSerializedSizeInBytes() <= limit {
			best, found = mid, true
			lo = mid + 1
		} else {
			if mid == 0 {
				break
			}

			hi = mid - 1
		}
	}

	if !found {
		return nil, 0, errors.NewProcessingError("one chunk at position %d is larger than the %d-byte row limit", start, limit)
	}

	p := part(best)
	p.RunOptimize()

	if best == lastChunk {
		return p, 0, nil
	}

	it := bm.Iterator()
	it.AdvanceIfNeeded((best + 1) << chunkBits)

	if !it.HasNext() {
		return p, 0, nil
	}

	return p, it.Next(), nil
}

// isFullRange reports whether bm holds every position from lowest to highest.
func isFullRange(bm *roaring64.Bitmap, lowest, highest uint64) bool {
	return bm.GetCardinality() == highest-lowest+1
}

func rowFromBitmap(lane uint8, bm *roaring64.Bitmap) (MapRow, error) {
	lowest, highest := bm.Minimum(), bm.Maximum()

	if isFullRange(bm, lowest, highest) {
		return MapRow{Lane: lane, First: lowest, Last: highest, Tag: RowTagRange}, nil
	}

	data, err := bm.ToBytes()
	if err != nil {
		return MapRow{}, errors.NewProcessingError("lane %d: serializing hole data", lane, err)
	}

	return MapRow{
		Lane:      lane,
		First:     lowest,
		Last:      highest,
		FirstHole: firstHole(bm, lowest, highest),
		Tag:       RowTagRoaring64,
		HoleData:  data,
	}, nil
}

// firstHole returns the lowest position between lowest and highest that bm does not hold, or 0
// if it holds them all. It searches by rank, so it costs a few dozen rank queries, not a walk.
func firstHole(bm *roaring64.Bitmap, lowest, highest uint64) uint64 {
	if isFullRange(bm, lowest, highest) {
		return 0
	}

	// rankBelow(x) is how many of bm's positions are below x.
	rankBelow := func(x uint64) uint64 { return bm.Rank(x - 1) }
	base := rankBelow(lowest)

	// The answer is the lowest x in (lowest, highest] with fewer than x-lowest positions held in
	// [lowest, x): every position in [lowest, x) taken means x-lowest of them.
	lo, hi := lowest+1, highest

	for lo < hi {
		mid := lo + (hi-lo)/2

		if rankBelow(mid+1)-base == mid-lowest+1 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo
}

// DecodeHoleData parses a row's hole data and checks it: canonical encoding, which is the
// run-optimized form every writer here produces, exact length, no empty bucket, and every position
// within 1 to MaxPosition.
func DecodeHoleData(data []byte) (*roaring64.Bitmap, error) {
	if len(data) < 8 {
		return nil, errors.NewProcessingError("lane hole data is %d bytes, too short", len(data))
	}

	bm := roaring64.New()
	r := bytes.NewReader(data)

	if _, err := bm.ReadPortableFrom(r); err != nil {
		return nil, errors.NewProcessingError("lane hole data does not parse", err)
	}

	if r.Len() != 0 {
		return nil, errors.NewProcessingError("lane hole data has %d trailing bytes", r.Len())
	}

	if err := bm.Validate(); err != nil {
		return nil, errors.NewProcessingError("lane hole data is inconsistent", err)
	}

	canonical := bm.Clone()
	canonical.RunOptimize()

	again, err := canonical.ToBytes()
	if err != nil || !bytes.Equal(again, data) {
		return nil, errors.NewProcessingError("lane hole data is not in canonical run-optimized form", err)
	}

	if bm.IsEmpty() {
		return nil, errors.NewProcessingError("lane hole data holds no position")
	}

	if bm.Minimum() == 0 || bm.Maximum() > MaxPosition {
		return nil, errors.NewProcessingError("lane hole data holds positions %d to %d, outside 1 to %d", bm.Minimum(), bm.Maximum(), MaxPosition)
	}

	return bm, nil
}

// Check validates a stored row against itself: range bounds, tag, first hole and hole data. It
// returns the parsed bitmap of the positions taken for a RowTagRoaring64 row, nil for a
// RowTagRange row.
func (r MapRow) Check() (*roaring64.Bitmap, error) {
	if r.First == 0 || r.Last > MaxPosition || r.First > r.Last {
		return nil, errors.NewProcessingError("lane %d map row: range %d to %d is not within 1 to %d", r.Lane, r.First, r.Last, MaxPosition)
	}

	switch r.Tag {
	case RowTagRange:
		if r.FirstHole != 0 || r.HoleData != nil {
			return nil, errors.NewProcessingError("lane %d map row %d to %d: a range row has no holes", r.Lane, r.First, r.Last)
		}

		return nil, nil

	case RowTagRoaring64:
		if len(r.HoleData) > MaxRowHoleDataBytes {
			return nil, errors.NewProcessingError("lane %d map row %d to %d: hole data of %d bytes is over the row limit", r.Lane, r.First, r.Last, len(r.HoleData))
		}

		bm, err := DecodeHoleData(r.HoleData)
		if err != nil {
			return nil, errors.NewProcessingError("lane %d map row %d to %d", r.Lane, r.First, r.Last, err)
		}

		if bm.Minimum() != r.First || bm.Maximum() != r.Last {
			return nil, errors.NewProcessingError("lane %d map row %d to %d: hole data spans %d to %d", r.Lane, r.First, r.Last, bm.Minimum(), bm.Maximum())
		}

		hole := firstHole(bm, r.First, r.Last)
		if hole == 0 {
			return nil, errors.NewProcessingError("lane %d map row %d to %d: hole data has no hole, so the row must be a range row", r.Lane, r.First, r.Last)
		}

		if hole != r.FirstHole {
			return nil, errors.NewProcessingError("lane %d map row %d to %d: first hole is %d, row says %d", r.Lane, r.First, r.Last, hole, r.FirstHole)
		}

		return bm, nil

	default:
		return nil, errors.NewProcessingError("lane %d map row %d to %d: unknown tag %d", r.Lane, r.First, r.Last, r.Tag)
	}
}

// CheckMapRows validates the rows one block wrote: each row by itself, and for each lane, rows in
// rising order that never share a chunk, so a split row covers whole chunks. A block has at most
// one row per lane unless the lane's bitmap was split.
func CheckMapRows(rows []MapRow) error {
	lastEnd := map[uint8]uint64{}

	for i, r := range rows {
		if _, err := r.Check(); err != nil {
			return errors.NewProcessingError("map row %d", i, err)
		}

		if prev, ok := lastEnd[r.Lane]; ok && r.First>>chunkBits <= prev>>chunkBits {
			return errors.NewProcessingError("map row %d: lane %d row starting %d shares a chunk with or comes before the row ending %d", i, r.Lane, r.First, prev)
		}

		lastEnd[r.Lane] = r.Last
	}

	return nil
}

// Taken reports whether this row's block took position p. The caller has parsed the hole data
// once with Check and passes the bitmap; it is ignored for a range row.
func (r MapRow) Taken(p uint64, holeData *roaring64.Bitmap) bool {
	if p < r.First || p > r.Last {
		return false
	}

	if r.Tag == RowTagRange || p < r.FirstHole {
		return true
	}

	return holeData != nil && holeData.Contains(p)
}

// SameRow reports whether two rows say the same thing: lane, range, first hole, tag and the same
// set of taken positions. A retried insert compares with it rather than with the bytes.
func SameRow(a, b MapRow) (bool, error) {
	if a.Lane != b.Lane || a.First != b.First || a.Last != b.Last || a.FirstHole != b.FirstHole || a.Tag != b.Tag {
		return false, nil
	}

	if a.Tag == RowTagRange {
		return true, nil
	}

	if bytes.Equal(a.HoleData, b.HoleData) {
		return true, nil
	}

	ab, err := DecodeHoleData(a.HoleData)
	if err != nil {
		return false, err
	}

	bb, err := DecodeHoleData(b.HoleData)
	if err != nil {
		return false, err
	}

	return ab.Equals(bb), nil
}

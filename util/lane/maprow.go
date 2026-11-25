package lane

import (
	"bytes"
	"encoding/binary"
	"math/bits"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/teranode/errors"
)

// splitChunkRanges counts the chunk ranges takeChunks builds, so a test can hold the split's work
// to the chunks it places. Nothing else reads it.
var splitChunkRanges atomic.Int64

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

	chunks := occupiedChunks(bm)

	var rows []MapRow

	for i := 0; i < len(chunks); {
		part, n, err := takeChunks(bm, chunks[i:], limit)
		if err != nil {
			return nil, errors.NewProcessingError("lane %d", lane, err)
		}

		row, err := rowFromBitmap(lane, part)
		if err != nil {
			return nil, err
		}

		rows = append(rows, row)
		i += n
	}

	return rows, nil
}

// occupiedChunks returns the numbers of the chunks bm holds at least one position in, in rising
// order. It visits each occupied chunk once and skips the empty ones between them, so a sparse
// bitmap over a wide span costs what it holds, not what it spans.
func occupiedChunks(bm *roaring64.Bitmap) []uint64 {
	var chunks []uint64

	it := bm.Iterator()

	for it.HasNext() {
		chunk := it.Next() >> chunkBits
		chunks = append(chunks, chunk)
		it.AdvanceIfNeeded((chunk + 1) << chunkBits)
	}

	return chunks
}

// takeChunks returns the bitmap of bm's positions in the longest leading run of chunks whose
// serialized size is at most limit, and how many chunks that run holds. chunks are occupied chunk
// numbers in rising order. Each candidate is built from its own occupied chunks only, and the run
// length is found by doubling and then bisecting, so the work follows the chunks one row takes,
// not the span of positions between them. Every chunk holds at most 65,536 positions, so one chunk
// is always below any limit a caller uses outside tests.
func takeChunks(bm *roaring64.Bitmap, chunks []uint64, limit uint64) (*roaring64.Bitmap, int, error) {
	part := func(n int) *roaring64.Bitmap {
		splitChunkRanges.Add(int64(n))

		p := roaring64.New()
		for _, c := range chunks[:n] {
			p.AddRange(c<<chunkBits, (c+1)<<chunkBits)
		}

		p.And(bm)

		return p
	}

	fits := func(n int) bool { return part(n).GetSerializedSizeInBytes() <= limit }

	if !fits(1) {
		return nil, 0, errors.NewProcessingError("one chunk at position %d is larger than the %d-byte row limit", chunks[0]<<chunkBits, limit)
	}

	// good runs fit; bad is the shortest run known not to fit, or one past the end.
	good, bad := 1, len(chunks)+1

	for n := 2; n <= len(chunks); n *= 2 {
		if !fits(n) {
			bad = n

			break
		}

		good = n
	}

	for bad-good > 1 {
		mid := good + (bad-good)/2

		if fits(mid) {
			good = mid
		} else {
			bad = mid
		}
	}

	p := part(good)
	p.RunOptimize()

	return p, good, nil
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

	// What Roaring's Validate checks, in one linear pass: Validate compares every pair of runs in a
	// run container, which on a block of scattered takes cost most of a second (checkContainers).
	if err := checkContainers(data); err != nil {
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

// Roaring's serialized form, as checkContainers reads it. A 64-bit bitmap is a bucket count, then
// per bucket a 4-byte key and a 32-bit bitmap. A 32-bit bitmap starts with a cookie: with run
// containers, the low 16 bits are serialCookie, the high 16 bits the container count less one,
// then a run flag per container; without, serialCookieNoRunContainer then a 4-byte count. Then a
// key and a cardinality less one per container, 2 bytes each, then 4-byte offsets unless the
// bitmap has run containers and fewer than noOffsetThreshold containers. Then each container: a
// run container is a 2-byte run count and per run a 2-byte start and a 2-byte length less one, a
// bitmap container 8,192 bytes, and an array container 2 bytes per value.
const (
	serialCookie               = 12347
	serialCookieNoRunContainer = 12346
	noOffsetThreshold          = 4
	arrayContainerMax          = 4096
	bitmapContainerBytes       = 8192

	// maxCanonicalRuns is the most runs a run-optimized container holds: a run container takes 2
	// bytes plus 4 per run, and with more than 2,047 runs a bitmap container's 8,192 bytes is
	// smaller, so RunOptimize stores it as a bitmap instead.
	maxCanonicalRuns = (bitmapContainerBytes - 2) / 4
)

// checkContainers walks the serialized form of a 64-bit Roaring bitmap and checks, in one pass
// linear in its size, everything Roaring's Validate checks: bucket and container keys in rising
// order, no empty bucket or container, array values in rising order, a bitmap container's
// cardinality equal to its bits set, and runs in range, in rising order, neither overlapping nor
// touching. It also refuses a run container of more than maxCanonicalRuns runs. Validate compares
// every pair of runs in a run container, so a block whose scattered takes fill its containers with
// close to 2,047 runs cost it most of a second; with the runs in order, each run against the next
// is the same check. A buffer it cannot walk is refused too; ReadPortableFrom has already accepted
// the buffer, so that means it is not the format checkContainers reads.
func checkContainers(data []byte) error {
	refuse := func() error {
		return errors.NewProcessingError("lane hole data: unreadable container headers")
	}

	if len(data) < 8 {
		return refuse()
	}

	buckets := binary.LittleEndian.Uint64(data)
	b := data[8:]

	var prevBucket int64 = -1

	for i := uint64(0); i < buckets; i++ {
		if len(b) < 8 {
			return refuse()
		}

		bucket := int64(binary.LittleEndian.Uint32(b))
		if bucket <= prevBucket {
			return errors.NewProcessingError("lane hole data: bucket %d follows bucket %d", bucket, prevBucket)
		}

		prevBucket = bucket
		b = b[4:]

		cookie := binary.LittleEndian.Uint32(b)

		var (
			size    uint32
			runFlag []byte
		)

		switch {
		case cookie&0xFFFF == serialCookie:
			size = cookie>>16 + 1
			b = b[4:]

			flagBytes := int(size+7) / 8
			if len(b) < flagBytes {
				return refuse()
			}

			runFlag, b = b[:flagBytes], b[flagBytes:]
		case cookie == serialCookieNoRunContainer:
			if len(b) < 8 {
				return refuse()
			}

			size = binary.LittleEndian.Uint32(b[4:])
			b = b[8:]
		default:
			return refuse()
		}

		if size == 0 {
			return errors.NewProcessingError("lane hole data: bucket %d is empty", bucket)
		}

		if uint64(len(b)) < uint64(size)*4 {
			return refuse()
		}

		keyCard, rest := b[:size*4], b[size*4:]
		b = rest

		if runFlag == nil || size >= noOffsetThreshold {
			if uint64(len(b)) < uint64(size)*4 {
				return refuse()
			}

			b = b[size*4:]
		}

		prevKey := -1

		for c := uint32(0); c < size; c++ {
			key := int(binary.LittleEndian.Uint16(keyCard[c*4:]))
			if key <= prevKey {
				return errors.NewProcessingError("lane hole data: container %d follows container %d", key, prevKey)
			}

			prevKey = key
			card := int(binary.LittleEndian.Uint16(keyCard[c*4+2:])) + 1

			var err error

			switch {
			case runFlag != nil && runFlag[c/8]&(1<<(c%8)) != 0:
				b, err = checkRunContainer(b)
			case card > arrayContainerMax:
				b, err = checkBitmapContainer(b, card)
			default:
				b, err = checkArrayContainer(b, card)
			}

			if err != nil {
				return err
			}
		}
	}

	return nil
}

// checkRunContainer checks the run container at the start of b and returns what follows it.
func checkRunContainer(b []byte) ([]byte, error) {
	if len(b) < 2 {
		return nil, errors.NewProcessingError("lane hole data: unreadable container headers")
	}

	runs := int(binary.LittleEndian.Uint16(b))
	if runs > maxCanonicalRuns {
		return nil, errors.NewProcessingError("lane hole data: a run container of %d runs, more than the canonical %d", runs, maxCanonicalRuns)
	}

	if runs == 0 {
		return nil, errors.NewProcessingError("lane hole data: a run container with no runs")
	}

	if len(b) < 2+runs*4 {
		return nil, errors.NewProcessingError("lane hole data: unreadable container headers")
	}

	prevEnd := -2

	for r := 0; r < runs; r++ {
		start := int(binary.LittleEndian.Uint16(b[2+r*4:]))
		end := start + int(binary.LittleEndian.Uint16(b[4+r*4:]))

		if end > 0xFFFF {
			return nil, errors.NewProcessingError("lane hole data: a run from %d passes the end of its container", start)
		}

		if start <= prevEnd+1 {
			return nil, errors.NewProcessingError("lane hole data: a run from %d overlaps or touches the run ending %d", start, prevEnd)
		}

		prevEnd = end
	}

	return b[2+runs*4:], nil
}

// checkBitmapContainer checks the bitmap container at the start of b, whose header says it holds
// card values, and returns what follows it.
func checkBitmapContainer(b []byte, card int) ([]byte, error) {
	if len(b) < bitmapContainerBytes {
		return nil, errors.NewProcessingError("lane hole data: unreadable container headers")
	}

	set := 0
	for w := 0; w < bitmapContainerBytes; w += 8 {
		set += bits.OnesCount64(binary.LittleEndian.Uint64(b[w:]))
	}

	if set != card {
		return nil, errors.NewProcessingError("lane hole data: a bitmap container of %d values says %d", set, card)
	}

	return b[bitmapContainerBytes:], nil
}

// checkArrayContainer checks the array container of card values at the start of b and returns
// what follows it.
func checkArrayContainer(b []byte, card int) ([]byte, error) {
	if len(b) < card*2 {
		return nil, errors.NewProcessingError("lane hole data: unreadable container headers")
	}

	prev := -1

	for v := 0; v < card; v++ {
		value := int(binary.LittleEndian.Uint16(b[v*2:]))
		if value <= prev {
			return nil, errors.NewProcessingError("lane hole data: array value %d follows %d", value, prev)
		}

		prev = value
	}

	return b[card*2:], nil
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
// once with Check and passes the bitmap; it is ignored for a range row and below the first hole.
// It is an error to pass no bitmap when a RowTagRoaring64 row needs one to answer.
func (r MapRow) Taken(p uint64, holeData *roaring64.Bitmap) (bool, error) {
	if p < r.First || p > r.Last {
		return false, nil
	}

	if r.Tag == RowTagRange || p < r.FirstHole {
		return true, nil
	}

	if holeData == nil {
		return false, errors.NewProcessingError("lane %d map row %d to %d: position %d needs the row's hole data", r.Lane, r.First, r.Last, p)
	}

	return holeData.Contains(p), nil
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

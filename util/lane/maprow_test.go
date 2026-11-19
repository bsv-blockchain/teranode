package lane

import (
	"math/rand"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/stretchr/testify/require"
)

func bitmapOf(positions ...uint64) *roaring64.Bitmap {
	bm := roaring64.New()
	bm.AddMany(positions)

	return bm
}

func requireTaken(t *testing.T, r MapRow, p uint64, holeData *roaring64.Bitmap, want bool) {
	t.Helper()

	got, err := r.Taken(p, holeData)
	require.NoError(t, err)
	require.Equal(t, want, got, "position %d", p)
}

func TestBuildMapRowsRange(t *testing.T) {
	bm := roaring64.New()
	bm.AddRange(1, 90_001)

	rows, err := BuildMapRows(7, bm)
	require.NoError(t, err)
	require.Equal(t, []MapRow{{Lane: 7, First: 1, Last: 90_000, Tag: RowTagRange}}, rows)

	hole, err := rows[0].Check()
	require.NoError(t, err)
	require.Nil(t, hole)
	requireTaken(t, rows[0], 41_000, nil, true)
	requireTaken(t, rows[0], 90_001, nil, false)
}

// The design's lane 11 example: block 1,000 takes 5,000 to 7,000 with holes from 5,004.
func TestBuildMapRowsWithHoles(t *testing.T) {
	bm := roaring64.New()
	bm.AddRange(5_000, 7_001)
	bm.Remove(5_004)
	bm.Remove(6_000)
	bm.Remove(6_998)

	rows, err := BuildMapRows(11, bm)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	r := rows[0]
	require.Equal(t, uint8(11), r.Lane)
	require.Equal(t, uint64(5_000), r.First)
	require.Equal(t, uint64(7_000), r.Last)
	require.Equal(t, uint64(5_004), r.FirstHole)
	require.Equal(t, RowTagRoaring64, r.Tag)

	parsed, err := r.Check()
	require.NoError(t, err)
	require.True(t, parsed.Equals(bm))

	requireTaken(t, r, 5_003, parsed, true)
	requireTaken(t, r, 5_003, nil, true) // below the first hole needs no bitmap
	requireTaken(t, r, 5_004, parsed, false)
	requireTaken(t, r, 6_000, parsed, false)
	requireTaken(t, r, 6_999, parsed, true)
	requireTaken(t, r, 4_999, parsed, false)

	// At or above the first hole a holed row cannot answer without its bitmap.
	_, err = r.Taken(6_999, nil)
	require.Error(t, err)
	_, err = r.Taken(5_004, nil)
	require.Error(t, err)

	// The input is not modified.
	require.False(t, bm.Contains(6_000))
}

func TestBuildMapRowsRefusesBadInput(t *testing.T) {
	_, err := BuildMapRows(1, nil)
	require.Error(t, err)

	_, err = BuildMapRows(1, roaring64.New())
	require.Error(t, err)

	_, err = BuildMapRows(1, bitmapOf(0, 5))
	require.Error(t, err, "position 0")

	_, err = BuildMapRows(1, bitmapOf(5, MaxPosition+1))
	require.Error(t, err, "wider than seven bytes")
}

func TestFirstHoleMatchesAWalk(t *testing.T) {
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic test data

	for i := 0; i < 200; i++ {
		bm := roaring64.New()
		start := uint64(rng.Intn(1 << 20))
		n := uint64(rng.Intn(5000) + 2)

		for p := start; p < start+n; p++ {
			if rng.Intn(50) != 0 {
				bm.Add(p + 1)
			}
		}

		if bm.IsEmpty() {
			continue
		}

		lo, hi := bm.Minimum(), bm.Maximum()

		want := uint64(0)

		for p := lo; p <= hi; p++ {
			if !bm.Contains(p) {
				want = p
				break
			}
		}

		require.Equal(t, want, firstHole(bm, lo, hi))
	}
}

// A bitmap over the limit splits into rows over whole chunks, and the rows together hold exactly
// the input. The limit is lowered so the test needs no 64 MB bitmap.
func TestBuildMapRowsSplitsOnWholeChunks(t *testing.T) {
	rng := rand.New(rand.NewSource(2)) //nolint:gosec // deterministic test data
	bm := roaring64.New()

	for c := uint64(0); c < 40; c++ {
		base := c<<16 + 1

		for j := 0; j < 3000; j++ {
			bm.Add(base + uint64(rng.Intn(1<<16-1)))
		}
	}

	const limit = 40 << 10

	require.Greater(t, bm.GetSerializedSizeInBytes(), uint64(limit))

	rows, err := buildMapRows(9, bm, limit)
	require.NoError(t, err)
	require.Greater(t, len(rows), 1)
	require.NoError(t, CheckMapRows(rows))
	requireLongestRuns(t, bm, rows, limit)

	union := roaring64.New()

	for _, r := range rows {
		require.LessOrEqual(t, uint64(len(r.HoleData)), uint64(limit))

		parsed, err := r.Check()
		require.NoError(t, err)
		require.False(t, union.Intersects(parsed))
		union.Or(parsed)
	}

	require.True(t, union.Equals(bm))
}

// requireLongestRuns checks that each row except the last is the longest run of chunks that fits
// the limit: adding the next occupied chunk's positions to it would pass the limit.
func requireLongestRuns(t *testing.T, bm *roaring64.Bitmap, rows []MapRow, limit uint64) {
	t.Helper()

	for i := 0; i+1 < len(rows); i++ {
		part, err := rows[i].Check()
		require.NoError(t, err)

		if part == nil {
			part = roaring64.New()
			part.AddRange(rows[i].First, rows[i].Last+1)
		}

		next := rows[i+1].First >> chunkBits
		chunk := roaring64.New()
		chunk.AddRange(next<<chunkBits, (next+1)<<chunkBits)
		chunk.And(bm)

		part.Or(chunk)
		part.RunOptimize()
		require.Greater(t, part.GetSerializedSizeInBytes(), limit, "row %d could have taken the next chunk", i)
	}
}

// A sparse bitmap over a very wide span splits at a cost that follows the chunks it holds. Each
// position sits in its own chunk 2^32 positions from the next, so the span holds about 10^11
// chunks and only 4,000 of them are occupied. A split that builds every candidate row over its
// whole span of chunks, empty ones included, needs gigabytes here.
func TestBuildMapRowsSplitsASparseWideBitmap(t *testing.T) {
	bm := roaring64.New()
	for i := uint64(0); i < 4000; i++ {
		bm.Add(1 + i<<32)
	}

	const limit = 4 << 10

	require.Greater(t, bm.GetSerializedSizeInBytes(), uint64(limit))

	// The split this replaced took more than 45 s and 12 GB here; this one takes well under a
	// second.
	started := time.Now()
	rows, err := buildMapRows(9, bm, limit)
	require.Less(t, time.Since(started), 10*time.Second, "the split's cost follows the span, not the occupied chunks")

	require.NoError(t, err)
	require.Greater(t, len(rows), 1)
	require.NoError(t, CheckMapRows(rows))
	requireLongestRuns(t, bm, rows, limit)

	union := roaring64.New()

	for _, r := range rows {
		require.LessOrEqual(t, uint64(len(r.HoleData)), uint64(limit))

		parsed, err := r.Check()
		require.NoError(t, err)
		require.False(t, union.Intersects(parsed))
		union.Or(parsed)
	}

	require.True(t, union.Equals(bm))
}

func TestBuildMapRowsSplitFailsWhenOneChunkIsTooBig(t *testing.T) {
	bm := roaring64.New()
	for p := uint64(1); p < 1<<16; p += 2 {
		bm.Add(p)
	}

	_, err := buildMapRows(9, bm, 100)
	require.Error(t, err)
}

func TestMapRowCheckRejects(t *testing.T) {
	good, err := BuildMapRows(3, bitmapOf(10, 11, 13))
	require.NoError(t, err)
	require.NoError(t, CheckMapRows(good))

	r := good[0]

	cases := map[string]MapRow{
		"zero first":           {Lane: 3, First: 0, Last: 5, Tag: RowTagRange},
		"first after last":     {Lane: 3, First: 6, Last: 5, Tag: RowTagRange},
		"last too wide":        {Lane: 3, First: 1, Last: MaxPosition + 1, Tag: RowTagRange},
		"range with hole":      {Lane: 3, First: 1, Last: 5, FirstHole: 2, Tag: RowTagRange},
		"range with data":      {Lane: 3, First: 1, Last: 5, Tag: RowTagRange, HoleData: r.HoleData},
		"unknown tag":          {Lane: 3, First: 10, Last: 13, FirstHole: 12, Tag: 9, HoleData: r.HoleData},
		"wrong first hole":     {Lane: 3, First: 10, Last: 13, FirstHole: 11, Tag: RowTagRoaring64, HoleData: r.HoleData},
		"wrong first":          {Lane: 3, First: 9, Last: 13, FirstHole: 12, Tag: RowTagRoaring64, HoleData: r.HoleData},
		"wrong last":           {Lane: 3, First: 10, Last: 14, FirstHole: 12, Tag: RowTagRoaring64, HoleData: r.HoleData},
		"no data":              {Lane: 3, First: 10, Last: 13, FirstHole: 12, Tag: RowTagRoaring64},
		"trailing data":        {Lane: 3, First: 10, Last: 13, FirstHole: 12, Tag: RowTagRoaring64, HoleData: append(append([]byte{}, r.HoleData...), 0)},
		"truncated data":       {Lane: 3, First: 10, Last: 13, FirstHole: 12, Tag: RowTagRoaring64, HoleData: r.HoleData[:len(r.HoleData)-1]},
		"roaring without hole": {Lane: 3, First: 10, Last: 11, Tag: RowTagRoaring64, HoleData: mustBytes(t, bitmapOf(10, 11))},
	}

	for name, row := range cases {
		_, err := row.Check()
		require.Error(t, err, name)
	}
}

func TestCheckMapRowsRejectsSharedChunks(t *testing.T) {
	a := MapRow{Lane: 1, First: 1, Last: 10, Tag: RowTagRange}
	b := MapRow{Lane: 1, First: 20, Last: 30, Tag: RowTagRange}
	c := MapRow{Lane: 2, First: 1, Last: 10, Tag: RowTagRange}
	d := MapRow{Lane: 1, First: 1 << 16, Last: 1<<16 + 5, Tag: RowTagRange}

	require.NoError(t, CheckMapRows([]MapRow{a, c}))
	require.NoError(t, CheckMapRows([]MapRow{a, d}))
	require.Error(t, CheckMapRows([]MapRow{a, b}), "two rows of one lane in one chunk")
	require.Error(t, CheckMapRows([]MapRow{d, a}), "out of order")
}

func TestSameRow(t *testing.T) {
	built, err := BuildMapRows(4, bitmapOf(1, 2, 4, 5))
	require.NoError(t, err)

	// The same set built another way serializes to the same canonical bytes.
	other := bitmapOf(5, 4, 2, 1)
	other.RunOptimize()
	alt := built[0]
	alt.HoleData = mustBytes(t, other)

	same, err := SameRow(built[0], alt)
	require.NoError(t, err)
	require.True(t, same)

	// A row whose bytes are not the canonical form is refused, not compared.
	sparse := roaring64.New()
	for p := uint64(1); p < 5_000; p++ {
		if p != 3 {
			sparse.Add(p) // one by one: a plain bitmap container, where runs are smaller
		}
	}
	loose := MapRow{Lane: 4, First: 1, Last: 4_999, FirstHole: 3, Tag: RowTagRoaring64, HoleData: mustBytes(t, sparse)}
	_, err = loose.Check()
	require.Error(t, err)
	_, err = SameRow(MapRow{Lane: 4, First: 1, Last: 4_999, FirstHole: 3, Tag: RowTagRoaring64, HoleData: []byte{1}}, loose)
	require.Error(t, err)

	diff := built[0]
	diff.HoleData = mustBytes(t, bitmapOf(1, 2, 3, 5))
	diff.FirstHole = 4
	same, err = SameRow(built[0], diff)
	require.NoError(t, err)
	require.False(t, same)

	same, err = SameRow(MapRow{Lane: 1, First: 1, Last: 3}, MapRow{Lane: 1, First: 1, Last: 3})
	require.NoError(t, err)
	require.True(t, same)

	// The same lane, range, first hole and tag over different sets are different rows.
	a, err := BuildMapRows(1, bitmapOf(1, 2, 4, 6))
	require.NoError(t, err)
	b, err := BuildMapRows(1, bitmapOf(1, 2, 4, 5, 6))
	require.NoError(t, err)
	require.Equal(t, a[0].FirstHole, b[0].FirstHole)

	same, err = SameRow(a[0], b[0])
	require.NoError(t, err)
	require.False(t, same)
}

// A bitmap filled one position at a time is a plain bitmap container, not the canonical run form,
// and BuildMapRows still writes canonical rows from it, whole or split.
func TestBuildMapRowsWritesCanonicalRowsFromAnyBitmap(t *testing.T) {
	bm := roaring64.New()

	for c := uint64(0); c < 8; c++ {
		for p := uint64(1); p < 5_000; p++ {
			if p != 3 {
				bm.Add(c<<16 + p)
			}
		}
	}

	for _, limit := range []uint64{MaxRowHoleDataBytes, 2 << 10} {
		rows, err := buildMapRows(9, bm, limit)
		require.NoError(t, err)
		require.NoError(t, CheckMapRows(rows), "limit %d", limit)
	}
}

// A lane split into many rows costs what its rows hold: each row's search starts small and grows
// with that row, not with every chunk still to place. 100,000 occupied chunks under a 4 KB limit
// make a few hundred rows. The split builds about a dozen chunk ranges per chunk placed; a search
// that probed the whole remainder for every row builds more than ten times as many, and that gap
// widens with the lane.
func TestBuildMapRowsSplitCostFollowsTheRows(t *testing.T) {
	const chunks = 100_000

	bm := roaring64.New()
	for i := uint64(0); i < chunks; i++ {
		bm.Add(i<<chunkBits + 1)
	}

	const limit = 4 << 10

	before := splitChunkRanges.Load()
	rows, err := buildMapRows(9, bm, limit)
	work := splitChunkRanges.Load() - before

	t.Logf("%d rows, %d chunk ranges built for %d chunks", len(rows), work, chunks)

	require.NoError(t, err)
	require.Greater(t, len(rows), 100)
	require.NoError(t, CheckMapRows(rows))
	// About 12 per chunk here; searching the whole remainder for each row costs over 130.
	require.Less(t, work, int64(30*chunks), "the split's work follows the rows, not the chunks left")
}

// occupiedChunks lists each occupied chunk once, however many positions it holds, so the split's
// work follows the chunks and not the positions.
func TestOccupiedChunksListsEachChunkOnce(t *testing.T) {
	bm := roaring64.New()
	bm.AddRange(1, 50_000)
	bm.AddRange(3<<16, 3<<16+40_000)
	bm.Add(1 << 40)

	require.Equal(t, []uint64{0, 3, 1 << 24}, occupiedChunks(bm))
}

func mustBytes(t *testing.T, bm *roaring64.Bitmap) []byte {
	t.Helper()

	b, err := bm.ToBytes()
	require.NoError(t, err)

	return b
}

func FuzzDecodeHoleData(f *testing.F) {
	f.Add(mustBytesF(f, bitmapOf(1, 2, 3, 1<<40)))
	f.Add([]byte{})
	f.Add([]byte{1, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		bm, err := DecodeHoleData(data)
		if err != nil {
			return
		}

		again, err := bm.ToBytes()
		require.NoError(t, err)
		require.Equal(t, data, again)
		require.NotZero(t, bm.Minimum())
		require.LessOrEqual(t, bm.Maximum(), MaxPosition)
	})
}

func mustBytesF(f *testing.F, bm *roaring64.Bitmap) []byte {
	b, err := bm.ToBytes()
	if err != nil {
		f.Fatal(err)
	}

	return b
}

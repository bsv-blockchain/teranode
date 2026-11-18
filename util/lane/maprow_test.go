package lane

import (
	"math/rand"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/stretchr/testify/require"
)

func bitmapOf(positions ...uint64) *roaring64.Bitmap {
	bm := roaring64.New()
	bm.AddMany(positions)

	return bm
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
	require.True(t, rows[0].Taken(41_000, nil))
	require.False(t, rows[0].Taken(90_001, nil))
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

	require.True(t, r.Taken(5_003, parsed), "below the first hole needs no bitmap")
	require.True(t, r.Taken(5_003, nil))
	require.False(t, r.Taken(5_004, parsed))
	require.False(t, r.Taken(6_000, parsed))
	require.True(t, r.Taken(6_999, parsed))
	require.False(t, r.Taken(4_999, parsed))

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

	rows, err := buildMapRows(9, bm, limit)
	require.NoError(t, err)
	require.Greater(t, len(rows), 1)
	require.NoError(t, CheckMapRows(rows))

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

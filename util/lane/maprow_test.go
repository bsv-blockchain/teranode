package lane

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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

// Hole data over MaxRowHoleDataBytes is refused by size, before it is decoded.
func TestMapRowCheckRefusesHoleDataOverTheRowLimit(t *testing.T) {
	row := MapRow{Lane: 3, First: 10, Last: 13, FirstHole: 12, Tag: RowTagRoaring64, HoleData: make([]byte, MaxRowHoleDataBytes+1)}

	_, err := row.Check()
	require.ErrorContains(t, err, "over the row limit")
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

// canonicalHex is the canonical encoding of bm, the form every writer here produces.
// holeDataBitmapDigest is the SHA-256 of the canonical bitmap-container fixture's hex.
const holeDataBitmapDigest = "72bf401984116de162c09d298a87bac2c3f6684ca06b4fbb188df7d947293c87"

func canonicalHex(t *testing.T, bm *roaring64.Bitmap) string {
	t.Helper()

	c := bm.Clone()
	c.RunOptimize()

	return hex.EncodeToString(mustBytes(t, c))
}

// The canonical hole-data bytes are pinned literally, not computed by the same library, so a
// Roaring upgrade that changes how it picks array, run or bitmap containers fails here instead of
// making every stored row unreadable. Each literal also decodes.
func TestHoleDataCanonicalBytesArePinned(t *testing.T) {
	array := bitmapOf(1, 5, 9)

	run := roaring64.New()
	run.AddRange(1, 1001)

	bitmap := roaring64.New()
	for v := uint64(1); v < 20_001; v += 2 {
		bitmap.Add(v)
	}

	twoBuckets := bitmapOf(1, 1<<32+1)

	for name, c := range map[string]struct {
		bm   *roaring64.Bitmap
		want string
	}{
		"array container": {array, "0100000000000000000000003a300000010000000000020010000000010005000900"},
		"run container":   {run, "0100000000000000000000003b300000010000e70301000100e703"},
		"two buckets":     {twoBuckets, "0200000000000000000000003a3000000100000000000000100000000100010000003a3000000100000000000000100000000100"},
	} {
		require.Equal(t, c.want, canonicalHex(t, c.bm), name)

		raw, err := hex.DecodeString(c.want)
		require.NoError(t, err)

		got, err := DecodeHoleData(raw)
		require.NoError(t, err, name)
		require.True(t, got.Equals(c.bm), name)
	}

	// The bitmap container is 8,220 bytes: pin its header and a digest of the whole.
	enc := canonicalHex(t, bitmap)
	require.Equal(t, "0100000000000000000000003a3000000100000000000f2710000000", enc[:56])
	sum := sha256.Sum256([]byte(enc))
	require.Equal(t, holeDataBitmapDigest, hex.EncodeToString(sum[:]))
}

// A run container of tens of thousands of runs would make Roaring's Validate compare every pair of
// runs, seconds of CPU per container. The canonical form never holds more than 2,047 runs, so more
// is refused first, quickly; a real 2,047-run container still decodes.
func TestDecodeHoleDataRefusesMoreRunsThanCanonical(t *testing.T) {
	const runs = 32_767

	b := binary.LittleEndian.AppendUint64(nil, 1)         // one 64-bit bucket
	b = binary.LittleEndian.AppendUint32(b, 0)            // its key
	b = binary.LittleEndian.AppendUint32(b, serialCookie) // one container, with runs
	b = append(b, 0x01)                                   // container 0 is a run container
	b = binary.LittleEndian.AppendUint16(b, 0)            // container key
	b = binary.LittleEndian.AppendUint16(b, runs-1)       // cardinality less one
	b = binary.LittleEndian.AppendUint16(b, runs)         // run count

	for i := 0; i < runs; i++ {
		b = binary.LittleEndian.AppendUint16(b, uint16(2*i+1)) // start
		b = binary.LittleEndian.AppendUint16(b, 0)             // length less one
	}

	started := time.Now()
	_, err := DecodeHoleData(b)
	require.ErrorContains(t, err, "more than the canonical")
	require.Less(t, time.Since(started), 200*time.Millisecond)

	most := roaring64.New()
	for i := uint64(0); i < maxCanonicalRuns; i++ {
		most.AddRange(i*6+1, i*6+4) // runs of three, so a run container beats a bitmap
	}

	raw, err := hex.DecodeString(canonicalHex(t, most))
	require.NoError(t, err)
	require.Equal(t, uint16(maxCanonicalRuns), binary.LittleEndian.Uint16(raw[len(raw)-4*maxCanonicalRuns-2:]), "the fixture really is a 2,047-run container")

	got, err := DecodeHoleData(raw)
	require.NoError(t, err)
	require.True(t, got.Equals(most))
}

// holeDataOf builds the serialized form of a one-bucket 64-bit bitmap by hand, so a test can write
// what no encoder writes. containers are key, cardinality and body; run marks run containers.
func holeDataOf(bucket uint32, containers []rawContainer) []byte {
	b := binary.LittleEndian.AppendUint64(nil, 1)
	b = binary.LittleEndian.AppendUint32(b, bucket)

	anyRun := false
	for _, c := range containers {
		anyRun = anyRun || c.run
	}

	if anyRun {
		b = binary.LittleEndian.AppendUint32(b, serialCookie|uint32(len(containers)-1)<<16) //nolint:gosec // a test
		flags := make([]byte, (len(containers)+7)/8)

		for i, c := range containers {
			if c.run {
				flags[i/8] |= 1 << (i % 8)
			}
		}

		b = append(b, flags...)
	} else {
		b = binary.LittleEndian.AppendUint32(b, serialCookieNoRunContainer)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(containers))) //nolint:gosec // a test
	}

	for _, c := range containers {
		b = binary.LittleEndian.AppendUint16(b, c.key)
		b = binary.LittleEndian.AppendUint16(b, c.card-1)
	}

	if !anyRun || len(containers) >= noOffsetThreshold {
		offset := uint32(len(b) + 4*len(containers)) //nolint:gosec // a test
		for _, c := range containers {
			b = binary.LittleEndian.AppendUint32(b, offset)
			offset += uint32(len(c.body)) //nolint:gosec // a test
		}
	}

	for _, c := range containers {
		b = append(b, c.body...)
	}

	return b
}

// twoBuckets is a two-bucket bitmap holding position 1 of each of buckets first and second, in
// that order.
func twoBuckets(first, second uint32) []byte {
	one := []rawContainer{{key: 0, card: 1, body: u16s(1)}}

	b := holeDataOf(first, one)
	binary.LittleEndian.PutUint64(b, 2)

	return append(b, holeDataOf(second, one)[8:]...)
}

type rawContainer struct {
	key, card uint16
	run       bool
	body      []byte
}

func u16s(vs ...uint16) []byte {
	var b []byte
	for _, v := range vs {
		b = binary.LittleEndian.AppendUint16(b, v)
	}

	return b
}

// The linear check refuses each shape Roaring's Validate refuses, and a hand-built valid form
// passes both. Each malformed buffer still parses, so only the check stands between it and a
// reader.
func TestCheckContainersRefusesWhatValidateRefuses(t *testing.T) {
	runs := func(pairs ...uint16) rawContainer { // start, length less one, ...
		return rawContainer{key: 0, card: 1, run: true, body: append(u16s(uint16(len(pairs)/2)), u16s(pairs...)...)} //nolint:gosec // a test
	}

	fullBitmap := make([]byte, bitmapContainerBytes)
	for i := range fullBitmap {
		fullBitmap[i] = 0x55 // 32,768 values
	}

	for name, c := range map[string]struct {
		data  []byte
		valid bool
	}{
		"valid runs":                 {holeDataOf(0, []rawContainer{runs(1, 2, 10, 0)}), true},
		"valid array":                {holeDataOf(0, []rawContainer{{key: 0, card: 3, body: u16s(1, 5, 9)}}), true},
		"valid bitmap":               {holeDataOf(0, []rawContainer{{key: 0, card: 0x8000, body: fullBitmap}}), true},
		"overlapping runs":           {holeDataOf(0, []rawContainer{runs(1, 2, 2, 0)}), false},
		"touching runs":              {holeDataOf(0, []rawContainer{runs(1, 0, 2, 0)}), false},
		"runs out of order":          {holeDataOf(0, []rawContainer{runs(10, 0, 1, 0)}), false},
		"run past the container":     {holeDataOf(0, []rawContainer{runs(0xFFFF, 1)}), false},
		"array out of order":         {holeDataOf(0, []rawContainer{{key: 0, card: 3, body: u16s(5, 1, 9)}}), false},
		"array repeats a value":      {holeDataOf(0, []rawContainer{{key: 0, card: 3, body: u16s(1, 1, 9)}}), false},
		"bitmap miscounted":          {holeDataOf(0, []rawContainer{{key: 0, card: 0x7FFF, body: fullBitmap}}), false},
		"run container with no runs": {holeDataOf(0, []rawContainer{{key: 0, card: 1, run: true, body: u16s(0)}}), false},
		"buckets out of order":       {twoBuckets(5, 4), false},
		"buckets in order":           {twoBuckets(4, 5), true},
		"containers out of order": {holeDataOf(0, []rawContainer{
			{key: 3, card: 1, body: u16s(1)}, {key: 2, card: 1, body: u16s(1)},
		}), false},
	} {
		t.Run(name, func(t *testing.T) {
			checkErr := checkContainers(c.data)

			bm := roaring64.New()
			if _, err := bm.ReadPortableFrom(bytes.NewReader(c.data)); err == nil {
				require.Equal(t, c.valid, bm.Validate() == nil, "Roaring's Validate agrees with the fixture")
			}

			if c.valid {
				require.NoError(t, checkErr)
			} else {
				require.Error(t, checkErr)
			}
		})
	}
}

// An empty bucket is refused: no canonical encoding writes one. Roaring's Validate passes it, so
// the canonical-bytes comparison in DecodeHoleData refuses it too.
func TestCheckContainersRefusesAnEmptyBucket(t *testing.T) {
	data := append(holeDataOf(0, nil), twoBuckets(4, 5)[8:]...)
	binary.LittleEndian.PutUint64(data, 3)

	require.Error(t, checkContainers(data))

	_, err := DecodeHoleData(data)
	require.Error(t, err)
}

// A block of scattered takes fills its containers with close to the canonical 2,047 runs. Each
// decodes in microseconds, where Roaring's Validate took milliseconds a container.
func TestDecodeHoleDataOfManyFullRunContainersIsQuick(t *testing.T) {
	bm := roaring64.New()

	for chunk := uint64(0); chunk < 64; chunk++ {
		for i := uint64(0); i < maxCanonicalRuns; i++ {
			start := chunk<<chunkBits + i*6 + 1
			bm.AddRange(start, start+3)
		}
	}

	bm.RunOptimize()

	data, err := bm.ToBytes()
	require.NoError(t, err)

	started := time.Now()
	got, err := DecodeHoleData(data)
	require.NoError(t, err)
	require.True(t, got.Equals(bm))
	require.Less(t, time.Since(started), 100*time.Millisecond, "64 containers of 2,047 runs")
}

// FuzzCheckContainersMatchesValidate: on any buffer Roaring parses, the linear check refuses it
// whenever Roaring's Validate does.
func FuzzCheckContainersMatchesValidate(f *testing.F) {
	f.Add(holeDataOf(0, []rawContainer{{key: 0, card: 1, run: true, body: u16s(2, 1, 2, 5, 0)}}))
	f.Add(holeDataOf(0, []rawContainer{{key: 0, card: 3, body: u16s(1, 5, 9)}}))
	f.Add(mustBytesF(f, bitmapOf(1, 2, 3, 1<<40)))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			return
		}

		bm := roaring64.New()
		if _, err := bm.ReadPortableFrom(bytes.NewReader(data)); err != nil {
			return
		}

		if checkContainers(data) == nil {
			require.NoError(t, bm.Validate(), "the check passed what Validate refuses")
		}
	})
}

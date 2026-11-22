package lane

import (
	"math/rand"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestBitmapsBuilderCollectsByLaneAndCountsTheUnpositioned(t *testing.T) {
	b := NewBitmapsBuilder()

	b.Add(Pack(7, 3))
	b.Add(Pack(7, 1))
	b.Add(0)          // a record stored before positions were on
	b.Add(Pack(9, 0)) // lane 9 with no position is no position either
	b.Add(Pack(254, 42))
	b.Add(Pack(0, 5)) // lane 0 is a lane

	require.Equal(t, 2, b.Unpositioned())

	s := b.Subtree(chainhash.Hash{0x51})
	require.Equal(t, chainhash.Hash{0x51}, s.SubtreeHash)
	require.Len(t, s.Lanes, 3)
	require.Equal(t, []uint64{1, 3}, s.Lanes[7].ToArray())
	require.Equal(t, []uint64{42}, s.Lanes[254].ToArray())
	require.Equal(t, []uint64{5}, s.Lanes[0].ToArray())

	// It encodes as a subtree lane bitmaps file and decodes to the same positions.
	data, err := EncodeSubtreeBitmaps(s)
	require.NoError(t, err)

	got, err := DecodeSubtreeBitmaps(data)
	require.NoError(t, err)
	require.True(t, got.Lanes[7].Equals(s.Lanes[7]))
}

func TestBitmapsBuilderOfOnlyUnpositionedHasNoLanesAndStillEncodes(t *testing.T) {
	b := NewBitmapsBuilder()
	b.Add(0)

	s := b.Subtree(chainhash.Hash{0x52})
	require.Empty(t, s.Lanes)

	data, err := EncodeSubtreeBitmaps(s)
	require.NoError(t, err)

	got, err := DecodeSubtreeBitmaps(data)
	require.NoError(t, err)
	require.Empty(t, got.Lanes)
}

func TestBuildBlockRowsEqualsTheMembersPerLane(t *testing.T) {
	// Two subtrees. Lane 7 is taken whole from 1 to 6 across both; lane 9 has holes at 3 and 4;
	// lane 200 only in the second subtree.
	first := LaneBitmaps{7: bitmapOf(1, 2, 3), 9: bitmapOf(1, 2)}
	second := LaneBitmaps{7: bitmapOf(4, 5, 6), 9: bitmapOf(5), 200: bitmapOf(10)}

	rows, err := BuildBlockRows([]LaneBitmaps{first, second})
	require.NoError(t, err)

	require.Equal(t, uint64(10), rows.Members)
	require.Len(t, rows.Rows, 3)

	require.Equal(t, MapRow{Lane: 7, First: 1, Last: 6, Tag: RowTagRange}, rows.Rows[0])

	require.Equal(t, uint8(9), rows.Rows[1].Lane)
	require.Equal(t, uint64(1), rows.Rows[1].First)
	require.Equal(t, uint64(5), rows.Rows[1].Last)
	require.Equal(t, uint64(3), rows.Rows[1].FirstHole)

	taken, err := rows.Rows[1].Check()
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 5}, taken.ToArray())

	require.Equal(t, MapRow{Lane: 200, First: 10, Last: 10, Tag: RowTagRange}, rows.Rows[2])
	require.NoError(t, CheckMapRows(rows.Rows))

	require.Equal(t, []LaneRowStats{
		{Lane: 7, Bytes: RowFixedBytes, Holes: []uint64{0}},
		{Lane: 9, Bytes: RowFixedBytes + len(rows.Rows[1].HoleData), Holes: []uint64{2}},
		{Lane: 200, Bytes: RowFixedBytes, Holes: []uint64{0}},
	}, rows.Lanes)

	// The subtrees' bitmaps are left as they were.
	require.Equal(t, []uint64{1, 2, 3}, first[7].ToArray())
	require.Equal(t, []uint64{4, 5, 6}, second[7].ToArray())
}

func TestBuildBlockRowsOfNoPositionsHasNoRows(t *testing.T) {
	// No lanes, no subtree, and a lane whose only bitmap is empty: none of them is a row.
	rows, err := BuildBlockRows([]LaneBitmaps{{}, nil, {7: roaring64.New()}})
	require.NoError(t, err)
	require.Empty(t, rows.Rows)
	require.Zero(t, rows.Members)
}

func TestBuildBlockRowsRefusesAPositionNoLaneHolds(t *testing.T) {
	_, err := BuildBlockRows([]LaneBitmaps{{7: bitmapOf(0, 1)}})
	require.Error(t, err, "position 0 is no position")
}

func TestObserveBlockRowsRecordsBytesAndHoles(t *testing.T) {
	initRowsMetrics()

	bytesBefore := histogramCount(t, prometheusLaneRowBytes.WithLabelValues("9"))
	holesBefore := histogramCount(t, prometheusLaneRowHoles)

	ObserveBlockRows(BlockRows{Lanes: []LaneRowStats{{Lane: 9, Bytes: 100, Holes: []uint64{0, 3}}}})

	require.Equal(t, bytesBefore+1, histogramCount(t, prometheusLaneRowBytes.WithLabelValues("9")))
	require.Equal(t, holesBefore+2, histogramCount(t, prometheusLaneRowHoles))
}

func histogramCount(t *testing.T, o prometheus.Observer) uint64 {
	t.Helper()

	var m dto.Metric
	require.NoError(t, o.(prometheus.Metric).Write(&m))

	return m.GetHistogram().GetSampleCount()
}

func TestBuildBlockRowsCountsHolesPerSplitRow(t *testing.T) {
	// Every other position of four chunks, so each chunk is a bitmap container that a small limit
	// splits into rows of whole chunks.
	bm := roaring64.New()
	for p := uint64(1); p < 4<<chunkBits; p += 2 {
		bm.Add(p)
	}

	rows, err := buildMapRows(3, bm, 10_000)
	require.NoError(t, err)
	require.Greater(t, len(rows), 1, "the small limit splits the lane")

	// BuildBlockRows uses the real limit, so check the hole count rowHoles gives each split row.
	// Each row after the first starts above the lane's lowest position, so its count must leave
	// out what earlier rows took.
	for i, r := range rows {
		require.Equal(t, (r.Last-r.First)/2, rowHoles(bm, r), "row %d: half of its span less one is holes", i)
	}
}

// laneGateBlock builds the synthetic block the gate is measured on: lanes lanes, subtrees subtrees,
// and per lane a range of span positions cut into 9,000-position stretches (100 ms of one lane at
// 90,000 a second). A fraction takenStretches of the stretches is taken, each position of a taken
// stretch with probability density, and the block's order scrambled across its subtrees, as an SV
// Node miner orders a backlog block. Mainnet block 973,667's shape is span 1.435 million at 17
// lanes, 20 percent of stretches, density 0.7: 3.5 million taken in 24.4 million positions, 93
// percent of the holes in whole stretches.
func laneGateBlock(seed int64, lanes, subtrees int, span uint64, takenStretches, density float64) ([]LaneBitmaps, uint64) {
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // a test fixture

	builders := make([]*BitmapsBuilder, subtrees)
	for i := range builders {
		builders[i] = NewBitmapsBuilder()
	}

	const stretch = 9_000

	var taken uint64

	for l := 0; l < lanes; l++ {
		for start := uint64(1); start <= span; start += stretch {
			if rng.Float64() >= takenStretches {
				continue
			}

			for p := start; p < start+stretch && p <= span; p++ {
				if rng.Float64() < density {
					builders[rng.Intn(subtrees)].Add(Pack(uint8(l), p)) //nolint:gosec // l < 256
					taken++
				}
			}
		}
	}

	out := make([]LaneBitmaps, subtrees)
	for i, b := range builders {
		out[i] = b.Subtree(chainhash.Hash{byte(i)}).Lanes
	}

	return out, taken
}

func TestLaneGateBlockShape(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a 3.5 million transaction block")
	}

	subtrees, taken := laneGateBlock(1, 17, 4, 1_435_000, 0.2, 0.7)

	rows, err := BuildBlockRows(subtrees)
	require.NoError(t, err)
	require.Equal(t, taken, rows.Members, "the rows hold every member")
	require.Greater(t, taken, uint64(3_000_000))
	require.Len(t, rows.Lanes, 17)

	var holes, bytes uint64
	for _, s := range rows.Lanes {
		bytes += uint64(s.Bytes)
		for _, h := range s.Holes {
			holes += h
		}
	}

	t.Logf("%d members, %d rows, %d holes, %d row bytes", rows.Members, len(rows.Rows), holes, bytes)
}

// BenchmarkLaneGateBuild is the build stage of the gate: decoding each subtree's lane bitmaps
// file, as block validation reads it, then merging and building the rows. The insert stage is
// BenchmarkStoreBlockLaneMapRows in stores/blockchain/sql.
func BenchmarkLaneGateBuild(b *testing.B) {
	shapes := []struct {
		name                    string
		lanes, subtrees         int
		span                    uint64
		takenStretches, density float64
	}{
		// Mainnet's backlog shape at 17 lanes, about 3.5 million members.
		{"mainnet_backlog_3.5M", 17, 4, 1_435_000, 0.2, 0.7},
		// The scale environment's shape, about 1.36 million members: every stretch, a peer that
		// missed a few transactions.
		{"scale_contiguous_1.36M", 17, 2, 80_000, 1, 0.9999},
	}

	for _, shape := range shapes {
		subtrees, taken := laneGateBlock(1, shape.lanes, shape.subtrees, shape.span, shape.takenStretches, shape.density)

		files := make([][]byte, len(subtrees))
		for i, s := range subtrees {
			data, err := EncodeSubtreeBitmaps(SubtreeBitmaps{SubtreeHash: chainhash.Hash{byte(i)}, Lanes: s})
			require.NoError(b, err)

			files[i] = data
		}

		b.Run(shape.name, func(b *testing.B) {
			var rows BlockRows

			for b.Loop() {
				decoded := make([]LaneBitmaps, len(files))

				for i, f := range files {
					s, err := DecodeSubtreeBitmaps(f)
					require.NoError(b, err)

					decoded[i] = s.Lanes
				}

				var err error

				rows, err = BuildBlockRows(decoded)
				require.NoError(b, err)
			}

			require.Equal(b, taken, rows.Members)

			var bytes int
			for _, s := range rows.Lanes {
				bytes += s.Bytes
			}

			b.ReportMetric(float64(taken), "members")
			b.ReportMetric(float64(bytes), "row_bytes")
			b.ReportMetric(float64(len(rows.Rows)), "rows")
			b.ReportMetric(float64(b.Elapsed())/float64(b.N)/float64(time.Millisecond), "ms/block")
		})
	}
}

package lane

import (
	"math/rand/v2"
	"runtime"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/stretchr/testify/require"
)

// The reader index's two pass marks, Phase 1: a parent lookup under 1 microsecond, and two days of
// rows under 1 GB per reader. Each benchmark builds an index of 288 blocks (two days) at 17 lanes,
// folds every block 100 deep into the floors as a reader does, reports the Go heap the index holds
// and what IndexStats estimates, then times lookups in random order across lanes: taken positions
// in rows and below the floors, and untaken ones.

const (
	benchLanes  = 17
	benchBlocks = 288
)

// benchIndex holds a built index and the lookups to time.
type benchIndex struct {
	idx     *Index
	lookups []uint64
	// metrics are reported after the timed loop, since ResetTimer clears them.
	metrics map[string]float64
	// The Go heap the index holds is reported in heapUnit, divided by heapDivisor.
	heapUnit    string
	heapDivisor float64
}

// buildBenchIndex loads benchBlocks blocks whose rows per lane come from takes(lane, block), and
// reports the heap the index holds.
func buildBenchIndex(b *testing.B, takes func(laneNum uint8, block uint32) *roaring64.Bitmap, sample func(laneNum uint8) []uint64) *benchIndex {
	b.Helper()

	idx := NewIndex(DefaultSettleDepth)

	for blk := uint32(1); blk <= benchBlocks; blk++ {
		var rows []MapRow

		for l := uint8(0); l < benchLanes; l++ {
			bm := takes(l, blk)
			if bm == nil || bm.IsEmpty() {
				continue
			}

			r, err := BuildMapRows(l, bm)
			require.NoError(b, err)

			rows = append(rows, r...)
		}

		require.NoError(b, idx.AddBlock(IndexBlock{ID: blk, Height: blk, Hash: blockHash(blk), Parent: blockHash(blk - 1)}, rows))
		require.NoError(b, idx.SetTip(blockHash(blk)))
	}

	stats := idx.Snapshot().Stats()

	var lookups []uint64

	for l := uint8(0); l < benchLanes; l++ {
		for _, p := range sample(l) {
			lookups = append(lookups, Pack(l, p))
		}
	}

	rand.New(rand.NewPCG(1, 2)).Shuffle(len(lookups), func(i, j int) { lookups[i], lookups[j] = lookups[j], lookups[i] })

	// The lookups mix answers from a row, from the floor and none, reported as shares.
	var byRow, byFloor, misses float64

	v := idx.Snapshot().Main()

	for _, pos := range lookups {
		switch v.Lookup(pos).Via {
		case ViaRow:
			byRow++
		case ViaFloor:
			byFloor++
		default:
			misses++
		}
	}

	n := float64(len(lookups))

	return &benchIndex{idx: idx, lookups: lookups, heapUnit: "heap-MB", heapDivisor: 1, metrics: map[string]float64{
		"%row":     100 * byRow / n,
		"%floor":   100 * byFloor / n,
		"%miss":    100 * misses / n,
		"index-MB": float64(stats.Bytes) / (1 << 20),
		"rows":     float64(stats.Rows),
		"listed":   float64(stats.Listed),
	}}
}

func (bi *benchIndex) run(b *testing.B) {
	b.Helper()

	v := bi.idx.Snapshot().Main()
	mined := 0
	i := 0

	b.ResetTimer()

	for b.Loop() {
		if v.Lookup(bi.lookups[i]).Mined {
			mined++
		}

		i++
		if i == len(bi.lookups) {
			i = 0
		}
	}

	b.StopTimer()

	for unit, value := range bi.metrics {
		b.ReportMetric(value, unit)
	}

	// The heap the index holds: what the heap drops by when the index is released.
	runtime.GC()

	var held, released runtime.MemStats
	runtime.ReadMemStats(&held)

	v, bi.idx = nil, nil

	runtime.GC()
	runtime.ReadMemStats(&released)
	b.ReportMetric((float64(held.HeapAlloc)-float64(released.HeapAlloc))/(1<<20)/bi.heapDivisor, bi.heapUnit)

	if mined == 0 {
		b.Fatal("no lookup found a mined position")
	}
}

// mainnetShape is a mainnet backlog: a lane creates perBlock positions a block, and each position
// is taken by one block within spread blocks of its creation, chosen at random, so a block's row
// spans spread blocks of positions with the rest of them holes and about spread rows overlap at
// any position. One position in 20 is never taken, a transaction never mined, so a lookup of it
// tests every overlapping row and finds none. Block 973,667 took 3.5 million transactions created over a day; at 17 lanes that
// is about 1,430 positions per lane per block, and nine rows overlap typically, 30 at worst.
func mainnetShape(perBlock uint64, spread uint32) (func(uint8, uint32) *roaring64.Bitmap, func(uint8) []uint64) {
	taken := make([]map[uint32]*roaring64.Bitmap, benchLanes)

	for l := range taken {
		taken[l] = map[uint32]*roaring64.Bitmap{}
		rng := rand.New(rand.NewPCG(uint64(l), 7))

		for created := uint32(1); created <= benchBlocks; created++ {
			for i := uint64(0); i < perBlock; i++ {
				p := uint64(created-1)*perBlock + i + 1

				if rng.IntN(20) == 0 {
					continue
				}

				blk := min(created+uint32(rng.IntN(int(spread))), benchBlocks)
				if taken[l][blk] == nil {
					taken[l][blk] = roaring64.New()
				}

				taken[l][blk].Add(p)
			}
		}
	}

	takes := func(l uint8, blk uint32) *roaring64.Bitmap { return taken[l][blk] }

	sample := func(l uint8) []uint64 {
		rng := rand.New(rand.NewPCG(uint64(l), 9))
		out := make([]uint64, 20_000)

		for i := range out {
			out[i] = uint64(rng.IntN(int(perBlock*benchBlocks))) + 1
		}

		return out
	}

	return takes, sample
}

// BenchmarkIndexLookupMainnet times a lookup on mainnet's backlog shape: 288 blocks of 17 lanes,
// 1,430 positions per lane per block, nine (or 30) rows overlapping.
func BenchmarkIndexLookupMainnet(b *testing.B) {
	for _, spread := range []uint32{9, 30} {
		b.Run("overlap="+itoa(spread), func(b *testing.B) {
			takes, sample := mainnetShape(1_430, spread)
			buildBenchIndex(b, takes, sample).run(b)
		})
	}
}

// designShape is the scale environment's shape at the design rate, 1.5 million transactions a
// second over 17 lanes: each block takes perBlock positions of a lane, about 53 million in 600
// seconds, as one run, except the positions of the last second before its cut, holesAtTop of them,
// which the next block takes. So every row has holes, all at its top.
func designShape(perBlock, holesAtTop uint64) (func(uint8, uint32) *roaring64.Bitmap, func(uint8) []uint64) {
	takes := func(_ uint8, blk uint32) *roaring64.Bitmap {
		start := uint64(blk-1)*perBlock + 1
		end := start + perBlock // exclusive

		bm := roaring64.New()

		// The previous block's late positions, the half of its last second it left, then this
		// block's own, less its last second.
		if blk > 1 {
			for p := start - holesAtTop + 1; p < start; p += 2 {
				bm.Add(p)
			}
		}

		bm.AddRange(start, end-holesAtTop)

		// Holes in the last second: every other position of it, the rest go to the next block.
		for p := end - holesAtTop; p < end; p += 2 {
			bm.Add(p)
		}

		return bm
	}

	sample := func(l uint8) []uint64 {
		rng := rand.New(rand.NewPCG(uint64(l), 11))
		out := make([]uint64, 20_000)

		for i := range out {
			blk := uint64(rng.IntN(benchBlocks))
			// Half anywhere in the block's range, half in its holed last second.
			if i%2 == 0 {
				out[i] = blk*perBlock + uint64(rng.IntN(int(perBlock))) + 1
			} else {
				out[i] = (blk+1)*perBlock - uint64(rng.IntN(int(holesAtTop))) - 1
			}
		}

		return out
	}

	return takes, sample
}

// BenchmarkIndexLookupDesignRate times a lookup at the design rate in the scale environment's
// shape: 53 million positions per lane per block, 88,000 of them (the last second) holed.
func BenchmarkIndexLookupDesignRate(b *testing.B) {
	takes, sample := designShape(53_000_000, 88_000)
	buildBenchIndex(b, takes, sample).run(b)
}

// BenchmarkIndexLookupDesignRateScattered times a lookup in one design-rate backlog row per lane:
// a block taking a random half of 53 million positions, one by one, the shape whose hole data the
// design puts at 75 to 78 MB a block over 17 lanes. Only 4 such rows are built: 288 blocks of
// them are the 22 GB case far past the reader memory mark, which the current reader would still
// try to load (the design's bounded overflow read is not built), so this measures the lookup cost
// and the heap per row, not two days.
func BenchmarkIndexLookupDesignRateScattered(b *testing.B) {
	const (
		perBlock = 53_000_000
		blocks   = 4
	)

	idx := NewIndex(DefaultSettleDepth)
	rng := rand.New(rand.NewPCG(3, 4))

	var lookups []uint64

	for blk := uint32(1); blk <= blocks; blk++ {
		start := uint64(blk-1)*perBlock + 1
		bm := roaring64.New()
		buf := make([]uint64, 0, 1<<16)

		for p := start; p < start+perBlock; p++ {
			if rng.Uint64()&1 == 0 {
				buf = append(buf, p)
			}

			if len(buf) == cap(buf) {
				bm.AddMany(buf)
				buf = buf[:0]
			}
		}

		bm.AddMany(buf)

		rows, err := BuildMapRows(0, bm)
		require.NoError(b, err)
		require.NoError(b, idx.AddBlock(IndexBlock{ID: blk, Height: blk, Hash: blockHash(blk), Parent: blockHash(blk - 1)}, rows))
		require.NoError(b, idx.SetTip(blockHash(blk)))

		for i := 0; i < 20_000; i++ {
			lookups = append(lookups, Pack(0, start+uint64(rng.IntN(perBlock))))
		}
	}

	rand.New(rand.NewPCG(5, 6)).Shuffle(len(lookups), func(i, j int) { lookups[i], lookups[j] = lookups[j], lookups[i] })
	(&benchIndex{idx: idx, lookups: lookups, heapUnit: "heap-MB/row", heapDivisor: blocks}).run(b)
}

func itoa(v uint32) string {
	if v == 0 {
		return "0"
	}

	var buf [10]byte

	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}

	return string(buf[i:])
}

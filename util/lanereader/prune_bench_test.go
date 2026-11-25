package lanereader

import (
	"math/rand/v2"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// The pruner's lane rule per parent, task P4's cost mark: the rule must add under one core at the
// design rate of 1.5 million transactions a second. Each benchmark builds an index as P3's lookup
// benchmarks do (17 lanes, settle depth 100) and times Judge, the whole per-parent cost: the lookups
// of the parent and each spender, the depth test, and counting. ns/op is per parent; the cores the
// rule adds are ns/op times parents a second, divided by 10^9.

const pruneBenchLanes = 17

// pruneBenchCandidates is how many parents a benchmark cycles through.
var pruneBenchCandidates = 100_000

// buildPruneBenchReader loads blocks 1 to blocks, each taking takes(lane, block) in every lane.
func buildPruneBenchReader(b *testing.B, blocks uint32, takes func(laneNum uint8, block uint32) *roaring64.Bitmap) *Reader {
	b.Helper()

	idx := lane.NewIndex(lane.DefaultSettleDepth)

	for blk := uint32(1); blk <= blocks; blk++ {
		var rows []lane.MapRow

		for l := uint8(0); l < pruneBenchLanes; l++ {
			bm := takes(l, blk)
			if bm == nil || bm.IsEmpty() {
				continue
			}

			r, err := lane.BuildMapRows(l, bm)
			require.NoError(b, err)

			rows = append(rows, r...)
		}

		require.NoError(b, idx.AddBlock(lane.IndexBlock{ID: blk, Height: blk, Hash: pruneBlockHash(blk), Parent: pruneBlockHash(blk - 1)}, rows))
		require.NoError(b, idx.SetTip(pruneBlockHash(blk)))
	}

	r := newReader(ulogger.TestLogger{}, nil, lane.DefaultSettleDepth)
	r.idx.Store(idx)

	return r
}

// runPruneBench times Judge over candidates in turn and reports the share of each answer and the
// lookups per parent.
func runPruneBench(b *testing.B, r *Reader, retention uint32, candidates []*PruneCandidate) {
	b.Helper()

	j := r.PruneJudge(PruneStoreAerospike, retention)
	i := 0

	b.ResetTimer()

	for b.Loop() {
		j.Judge(true, candidates[i])

		i++
		if i == len(candidates) {
			i = 0
		}
	}

	b.StopTimer()

	var deletes, total uint64

	for _, byLanes := range j.verdicts {
		for lanes, byReason := range byLanes {
			for _, n := range byReason {
				total += n

				if lanes == lanesDelete {
					deletes += n
				}
			}
		}
	}

	lookups := j.vias[lane.ViaNone] + j.vias[lane.ViaRow] + j.vias[lane.ViaFloor]

	b.ReportMetric(100*float64(deletes)/float64(total), "%delete")
	b.ReportMetric(100*float64(j.vias[lane.ViaFloor])/float64(lookups), "%floor")
	b.ReportMetric(100*float64(j.vias[lane.ViaRow])/float64(lookups), "%row")
	b.ReportMetric(float64(lookups)/float64(total), "lookups/parent")
}

// BenchmarkPruneJudgeMainnet is mainnet's shape, P3's backlog benchmark's: 288 blocks, 1,430
// positions per lane per block, each taken by one block within 9 of its creation, one in 20 never.
// The candidate is utxoset's, one journal row: the parent and one spender. utxoset retires a journal
// partition at least 288 blocks after its spends, so the spender was created in blocks 101 to 170 and
// its block is folded, 100 or more deep; the parent was created in blocks 1 to 100. So the floor
// answers nearly everything, as the design expects on a node whose retention is at least 100.
func BenchmarkPruneJudgeMainnet(b *testing.B) {
	const (
		perBlock = 1_430
		spread   = 9
		blocks   = 288
	)

	taken := make([]map[uint32]*roaring64.Bitmap, pruneBenchLanes)

	for l := range taken {
		taken[l] = map[uint32]*roaring64.Bitmap{}
		rng := rand.New(rand.NewPCG(uint64(l), 7)) //nolint:gosec // a benchmark's data

		for created := uint32(1); created <= blocks; created++ {
			for i := uint64(0); i < perBlock; i++ {
				if rng.IntN(20) == 0 {
					continue
				}

				blk := min(created+uint32(rng.IntN(spread)), blocks) //nolint:gosec // small
				if taken[l][blk] == nil {
					taken[l][blk] = roaring64.New()
				}

				taken[l][blk].Add(uint64(created-1)*perBlock + i + 1)
			}
		}
	}

	r := buildPruneBenchReader(b, blocks, func(l uint8, blk uint32) *roaring64.Bitmap { return taken[l][blk] })

	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a benchmark's data
	at := func(fromBlock, toBlock int) uint64 {
		return lane.Pack(uint8(rng.IntN(pruneBenchLanes)), uint64((fromBlock-1)*perBlock+rng.IntN((toBlock-fromBlock+1)*perBlock))+1) //nolint:gosec // small
	}

	candidates := make([]*PruneCandidate, pruneBenchCandidates)
	for i := range candidates {
		candidates[i] = &PruneCandidate{ParentPos: at(1, 100), Spenders: []PruneSpender{{Pos: at(101, 170)}}}
	}

	runPruneBench(b, r, 288, candidates)
}

// BenchmarkPruneJudgeDesignRate is the scale environment at the design rate, P3's design benchmark's
// shape: each block takes 53 million positions of each of 17 lanes as one run, except the last
// second's 88,000, every other one of which the next block takes. The candidate is Aerospike's, one
// transaction: the parent and two spenders. At retention 10, scale's, a parent reaches its
// delete-at height 10 blocks after its last spend, so its spenders' blocks are about 10 deep and the
// floor, which settles at 100, answers almost nothing: every lookup goes to the rows. At retention 288
// the spenders are more than 100 deep, below the floor. 150 blocks are built, enough for floors and
// 50 blocks of rows.
func BenchmarkPruneJudgeDesignRate(b *testing.B) {
	const (
		perBlock   = 53_000_000
		holesAtTop = 88_000
		blocks     = 150
	)

	r := buildPruneBenchReader(b, blocks, func(_ uint8, blk uint32) *roaring64.Bitmap {
		start := uint64(blk-1)*perBlock + 1
		end := start + perBlock

		bm := roaring64.New()

		if blk > 1 {
			for p := start - holesAtTop + 1; p < start; p += 2 {
				bm.Add(p)
			}
		}

		bm.AddRange(start, end-holesAtTop)

		for p := end - holesAtTop; p < end; p += 2 {
			bm.Add(p)
		}

		return bm
	})

	for _, tc := range []struct {
		name      string
		retention uint32
		// spenders' blocks are this many deep, or up to two more; the parent's up to 20 more again.
		depth int
		// tail is the share of positions drawn from a block's holed last second. Real traffic puts
		// 88,000 of 53 million there, 0.17%; P3's lookup benchmark draws half there, a stress mix
		// where every such lookup reads the hole data of two overlapping rows.
		tail float64
	}{
		{"retention=10/uniform", 10, 10, 88_000.0 / 53_000_000},
		{"retention=10/half_in_holed_tail", 10, 10, 0.5},
		{"retention=288/uniform", 288, 120, 88_000.0 / 53_000_000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			rng := rand.New(rand.NewPCG(3, 4)) //nolint:gosec // a benchmark's data
			inBlock := func(deep int) uint64 {
				blk := blocks - deep

				var p uint64
				if rng.Float64() >= tc.tail {
					p = uint64(blk-1)*perBlock + uint64(rng.IntN(perBlock)) + 1 //nolint:gosec // small
				} else {
					p = uint64(blk)*perBlock - uint64(rng.IntN(holesAtTop)) - 1 //nolint:gosec // small
				}

				return lane.Pack(uint8(rng.IntN(pruneBenchLanes)), p) //nolint:gosec // small
			}

			candidates := make([]*PruneCandidate, pruneBenchCandidates)
			for i := range candidates {
				deep := tc.depth + rng.IntN(3)
				candidates[i] = &PruneCandidate{
					ParentPos: inBlock(deep + 1 + rng.IntN(20)),
					Spenders:  []PruneSpender{{Pos: inBlock(deep)}, {Pos: inBlock(deep)}},
				}
			}

			runPruneBench(b, r, tc.retention, candidates)
		})
	}
}

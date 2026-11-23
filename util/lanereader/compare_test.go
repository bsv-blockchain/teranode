package lanereader

import (
	"math/rand/v2"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// benchBlockHash is a distinct hash per block number.
func benchBlockHash(n uint32) chainhash.Hash {
	var h chainhash.Hash
	h[0], h[1], h[2], h[3], h[31] = byte(n), byte(n>>8), byte(n>>16), byte(n>>24), 7

	return h
}

// BenchmarkCompareHeightParallel times the validator's whole shadow of one parent, lookup,
// classification and counting, from every P at once, as the parent checks run: 288 blocks of 17
// lanes in mainnet's backlog shape, nine rows overlapping. It shows whether the shared counters
// on that path contend.
func BenchmarkCompareHeightParallel(b *testing.B) {
	const (
		lanes    = 17
		blocks   = 288
		perBlock = 1_430
		spread   = 9
	)

	r := newReader(ulogger.TestLogger{}, nil, lane.DefaultSettleDepth)
	idx := lane.NewIndex(lane.DefaultSettleDepth)
	rng := rand.New(rand.NewPCG(1, 2))

	taken := make([]map[uint32]*roaring64.Bitmap, lanes)
	for l := range taken {
		taken[l] = map[uint32]*roaring64.Bitmap{}

		for created := uint32(1); created <= blocks; created++ {
			for i := uint64(0); i < perBlock; i++ {
				blk := min(created+uint32(rng.IntN(spread)), blocks)
				if taken[l][blk] == nil {
					taken[l][blk] = roaring64.New()
				}

				taken[l][blk].Add(uint64(created-1)*perBlock + i + 1)
			}
		}
	}

	for blk := uint32(1); blk <= blocks; blk++ {
		var rows []lane.MapRow

		for l := uint8(0); l < lanes; l++ {
			if bm := taken[l][blk]; bm != nil {
				rs, err := lane.BuildMapRows(l, bm)
				require.NoError(b, err)

				rows = append(rows, rs...)
			}
		}

		require.NoError(b, idx.AddBlock(lane.IndexBlock{ID: blk, Height: blk, Hash: benchBlockHash(blk), Parent: benchBlockHash(blk - 1)}, rows))
	}

	require.NoError(b, idx.SetTip(benchBlockHash(blocks)))
	r.idx.Store(idx)

	// Each position with the height of the block that took it, so the comparisons agree, and a
	// height that does not, so they disagree.
	type probe struct {
		pos    uint64
		height uint32
	}

	probes := make([]probe, 1<<16)
	v := r.MainView()

	for i := range probes {
		pos := lane.Pack(uint8(rng.IntN(lanes)), uint64(rng.IntN(perBlock*blocks))+1) //nolint:gosec // lanes fits a byte
		probes[i] = probe{pos: pos, height: v.Lookup(pos).Height}
	}

	for _, agree := range []bool{true, false} {
		name := "agree"
		if !agree {
			name = "disagree"
		}

		b.Run(name, func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				i := rand.IntN(len(probes))

				for pb.Next() {
					p := probes[i&(len(probes)-1)]

					height := p.height
					if !agree {
						height = p.height + 1
					}

					r.CompareHeight(ReaderValidator, chainhash.Hash{}, p.pos, true, height)
					i++
				}
			})
		})
	}
}

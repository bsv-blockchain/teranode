package lanereader

import (
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	dto "github.com/prometheus/client_model/go"
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
		a := v.Lookup(pos)

		height := a.Height
		if a.Via == lane.ViaFloor {
			height = 1 // any height at or below the anchor agrees with a floor answer
		}

		probes[i] = probe{pos: pos, height: height}
	}

	// Every probe agrees at its height and disagrees at the tip's height plus one, above every
	// row's block and the anchor.
	s := r.Snapshot()
	disagreeHeight := s.TipHeight() + 1

	for _, p := range probes[:1_000] {
		require.Equal(b, KindAgree, classifyHeight(s, p.pos, v.Lookup(p.pos), true, p.height))
		require.Equal(b, KindDifferentBlock, classifyHeight(s, p.pos, v.Lookup(p.pos), true, disagreeHeight))
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
						height = disagreeHeight
					}

					r.CompareHeight(ReaderValidator, chainhash.Hash{}, p.pos, true, height)
					i++
				}
			})
		})
	}
}

// warnCounter is a logger that counts its warnings.
type warnCounter struct {
	ulogger.TestLogger
	warns atomic.Int64
}

func (w *warnCounter) Warnf(string, ...interface{}) { w.warns.Add(1) }

// The disagreement log writes the first five of a reader and kind, then at most one a minute.
func TestDisagreementLogIsRateLimited(t *testing.T) {
	logger := &warnCounter{}
	r := newReader(logger, nil, lane.DefaultSettleDepth)

	// Block 1 took lane 2's position 7; today says not mined, so every answer is lanes_only.
	idx := lane.NewIndex(lane.DefaultSettleDepth)
	rows, err := lane.BuildMapRows(2, roaring64.BitmapOf(7))
	require.NoError(t, err)
	require.NoError(t, idx.AddBlock(lane.IndexBlock{ID: 1, Height: 1, Hash: benchBlockHash(1)}, rows))
	require.NoError(t, idx.SetTip(benchBlockHash(1)))
	r.idx.Store(idx)

	for range 20 {
		r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(2, 7), false, 0) // lanes_only
	}

	require.Equal(t, int64(disagreementsLogged), logger.warns.Load(), "the first five, then none within the minute")

	// A minute later, one more.
	g, ok := r.disagree.gates.Load(ReaderValidator + "/" + KindLanesOnly)
	require.True(t, ok)
	g.(*logGate).last.Store(time.Now().Add(-2 * time.Minute).UnixNano())

	for range 20 {
		r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(2, 7), false, 0)
	}

	require.Equal(t, int64(disagreementsLogged+1), logger.warns.Load())
}

// One lookup in about lookupSampleEvery is timed: neither every lookup nor none.
func TestLookupTimesASampleOfLookups(t *testing.T) {
	initMetrics()

	r := newReader(ulogger.TestLogger{}, nil, lane.DefaultSettleDepth)
	idx := lane.NewIndex(lane.DefaultSettleDepth)
	require.NoError(t, idx.AddBlock(lane.IndexBlock{ID: 1, Height: 1, Hash: benchBlockHash(1)}, nil))
	require.NoError(t, idx.SetTip(benchBlockHash(1)))
	r.idx.Store(idx)

	timed := func() uint64 {
		m := &dto.Metric{}
		require.NoError(t, prometheusLaneReaderLookupSeconds.Write(m))

		return m.GetHistogram().GetSampleCount()
	}

	before := timed()

	const lookups = 64 * 200

	for range lookups {
		r.lookup(r.MainView(), lane.Pack(1, 1))
	}

	// One in 64: about 200 of 12,800, the rate the hot path's cost was measured at.
	require.Equal(t, 64, lookupSampleEvery)

	got := timed() - before
	require.Greater(t, got, uint64(100), "some lookups are timed")
	require.Less(t, got, uint64(400), "only about one in 64 is timed")
}

// A floor answer does not agree with a block the index never loaded: that block may be an orphan,
// and the floor's settled block need not be the one today names.
func TestFloorAnswerDoesNotAgreeWithAnUnloadedBlock(t *testing.T) {
	r := newReader(ulogger.TestLogger{}, nil, 2)
	idx := lane.NewIndex(2)

	rows, err := lane.BuildMapRows(2, roaring64.BitmapOf(7))
	require.NoError(t, err)
	require.NoError(t, idx.AddBlock(lane.IndexBlock{ID: 1, Height: 1, Hash: benchBlockHash(1)}, rows))

	for h := uint32(2); h <= 4; h++ {
		require.NoError(t, idx.AddBlock(lane.IndexBlock{ID: h, Height: h, Hash: benchBlockHash(h), Parent: benchBlockHash(h - 1)}, nil))
	}

	require.NoError(t, idx.SetTip(benchBlockHash(4)))
	r.idx.Store(idx)
	require.Equal(t, lane.ViaFloor, r.MainView().Lookup(lane.Pack(2, 7)).Via)

	requireCounted(t, ReaderBlockParent, KindNotLoaded, lane.ViaFloor, func() {
		r.CompareBlocks(ReaderBlockParent, r.MainView(), chainhash.Hash{}, lane.Pack(2, 7), []uint32{999})
	})
}

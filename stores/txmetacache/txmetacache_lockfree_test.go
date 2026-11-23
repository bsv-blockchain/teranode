package txmetacache

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func newTestTxMetaCache(t *testing.T, bucket BucketType) *TxMetaCache {
	t.Helper()

	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	c, err := NewTxMetaCache(context.Background(), settings.NewSettings(), ulogger.TestLogger{}, ns, bucket, 64)
	require.NoError(t, err)

	return c.(*TxMetaCache)
}

var allBucketTypes = []struct {
	name   string
	bucket BucketType
}{
	{"unallocated", Unallocated},
	{"native", Native},
	{"pointer", Pointer},
	{"lockfree", Lockfree},
}

func TestTxMetaCache_Lockfree_RoundTrip(t *testing.T) {
	c := newTestTxMetaCache(t, Lockfree)
	ctx := context.Background()

	_, ok := c.cache.(*lockFreeBackend)
	require.True(t, ok)

	hash := mixedKey(1)
	data := &meta.Data{
		Fee:         7,
		SizeInBytes: 250,
		TxInpoints:  subtree.NewTxInpointsFromPacked([]chainhash.Hash{mixedKey(2)}, []uint32{1, 3}),
	}

	require.NoError(t, c.SetCache(&hash, data))

	got, found := c.GetMetaCached(ctx, hash)
	require.True(t, found)
	require.Equal(t, uint64(7), got.Fee)
	require.Equal(t, data.TxInpoints.ParentTxHashes, got.TxInpoints.ParentTxHashes)

	var viaGetMeta meta.Data
	require.NoError(t, c.GetMeta(ctx, &hash, &viaGetMeta))
	require.Equal(t, uint64(250), viaGetMeta.SizeInBytes)

	require.NoError(t, c.Delete(ctx, &hash))
	_, found = c.GetMetaCached(ctx, hash)
	require.False(t, found)

	other := mixedKey(3)
	require.NoError(t, c.SetCacheMultiSequential([][]byte{other[:]}, [][]byte{appendMixedValue(nil, 3)}))
	got, found = c.GetMetaCached(ctx, other)
	require.True(t, found)
	require.Equal(t, uint64(3), got.Fee)
}

func TestTxMetaCache_GetMetaCachedInto_MatchesGetMetaCached(t *testing.T) {
	for _, bt := range allBucketTypes {
		t.Run(bt.name, func(t *testing.T) {
			c := newTestTxMetaCache(t, bt.bucket)
			ctx := context.Background()

			const n = 3000

			hashes := make([]chainhash.Hash, n)

			for i := range hashes {
				hashes[i] = mixedKey(uint64(i))

				if i%5 != 0 { // every fifth is a miss
					require.NoError(t, c.SetCacheFromBytes(hashes[i][:], appendMixedValue(nil, uint64(i))))
				}
			}

			out := make([]CachedMeta, n)
			found := make([]bool, n)
			hits, err := c.GetMetaCachedInto(ctx, hashes, nil, out, found)
			require.NoError(t, err)

			want := 0

			for i := range hashes {
				ref, ok := c.GetMetaCached(ctx, hashes[i])
				require.Equal(t, ok, found[i], "hash %d", i)

				if !ok {
					continue
				}

				want++

				require.Equal(t, ref.Fee, out[i].Fee)
				require.Equal(t, ref.SizeInBytes, out[i].SizeInBytes)
				require.Equal(t, ref.IsCoinbase, out[i].IsCoinbase)
				require.Equal(t, ref.Conflicting, out[i].Conflicting)
				require.Equal(t, ref.TxInpoints.ParentTxHashes, out[i].TxInpoints.ParentTxHashes)

				refBytes, err := ref.TxInpoints.Serialize()
				require.NoError(t, err)
				gotBytes, err := out[i].TxInpoints.Serialize()
				require.NoError(t, err)
				require.Equal(t, refBytes, gotBytes)
			}

			require.Equal(t, want, hits)
			require.Positive(t, hits)
		})
	}
}

func TestTxMetaCache_GetMetaCachedInto_ResultsOutliveLaterCalls(t *testing.T) {
	c := newTestTxMetaCache(t, Lockfree)
	ctx := context.Background()

	first := []chainhash.Hash{mixedKey(10), mixedKey(11)}
	second := []chainhash.Hash{mixedKey(12), mixedKey(13)}

	require.NoError(t, c.SetCacheFromBytes(first[0][:], appendMixedValue(nil, 10)))
	require.NoError(t, c.SetCacheFromBytes(first[1][:], appendMixedValue(nil, 11)))
	require.NoError(t, c.SetCacheFromBytes(second[0][:], appendMixedValue(nil, 12)))
	require.NoError(t, c.SetCacheFromBytes(second[1][:], appendMixedValue(nil, 13)))

	out1 := make([]CachedMeta, 2)
	found1 := make([]bool, 2)
	hits, err := c.GetMetaCachedInto(ctx, first, nil, out1, found1)
	require.NoError(t, err)
	require.Equal(t, 2, hits)

	snapshot := append([]chainhash.Hash{}, out1[0].TxInpoints.ParentTxHashes...)

	out2 := make([]CachedMeta, 2)
	found2 := make([]bool, 2)
	hits, err = c.GetMetaCachedInto(ctx, second, nil, out2, found2)
	require.NoError(t, err)
	require.Equal(t, 2, hits)

	require.Equal(t, snapshot, out1[0].TxInpoints.ParentTxHashes, "a later call must not reuse an earlier call's slabs")

	// Appending to one entry's parents must not overwrite its neighbour.
	neighbour := append([]chainhash.Hash{}, out1[1].TxInpoints.ParentTxHashes...)
	_ = append(out1[0].TxInpoints.ParentTxHashes, chainhash.Hash{0xde, 0xad})
	require.Equal(t, neighbour, out1[1].TxInpoints.ParentTxHashes)
}

func TestTxMetaCache_GetMetaCachedInto_AllocationsDoNotScaleWithHits(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector adds allocations of its own")
	}

	c := newTestTxMetaCache(t, Lockfree)
	ctx := context.Background()

	const n = 1024

	hashes := make([]chainhash.Hash, n)
	for i := range hashes {
		hashes[i] = mixedKey(uint64(i))
		require.NoError(t, c.SetCacheFromBytes(hashes[i][:], appendMixedValue(nil, uint64(i))))
	}

	allocsFor := func(m int) float64 {
		out := make([]CachedMeta, m)
		found := make([]bool, m)

		return testing.AllocsPerRun(50, func() {
			if hits, err := c.GetMetaCachedInto(ctx, hashes[:m], nil, out, found); err != nil || hits != m {
				t.Fatal("expected every lookup to hit")
			}
		})
	}

	// A handful per call at most (a 256 KB decode chunk per few thousand
	// hits), never one or more per hit as GetMetaCached does.
	require.LessOrEqual(t, allocsFor(16), 2.0)
	require.LessOrEqual(t, allocsFor(n), 10.0)
}

func TestTxMetaCache_GetMetaCachedInto_ShortOutputsAreAnError(t *testing.T) {
	c := newTestTxMetaCache(t, Lockfree)
	hashes := []chainhash.Hash{mixedKey(1), mixedKey(2)}

	_, err := c.GetMetaCachedInto(context.Background(), hashes, nil, make([]CachedMeta, 1), make([]bool, 2))
	require.Error(t, err)

	_, err = c.GetMetaCachedInto(context.Background(), hashes, nil, make([]CachedMeta, 2), make([]bool, 1))
	require.Error(t, err)

	_, err = c.GetMetaCachedInto(context.Background(), hashes, make([]uint64, 1), make([]CachedMeta, 2), make([]bool, 2))
	require.Error(t, err)
}

// TestTxMetaCache_Lockfree_CacheStatsReportsLappedEvictions checks that the
// lockfree cache's ring-wrap evictions reach CacheStats, which feeds the
// teranode_tx_meta_cache_evictions gauge.
func TestTxMetaCache_Lockfree_CacheStatsReportsLappedEvictions(t *testing.T) {
	c := newTestTxMetaCache(t, Lockfree)
	lanes := c.cache.(*lockFreeBackend).cache

	require.Zero(t, c.GetCacheStats().Evictions)

	for i := uint64(0); i < 1_500_000; i++ {
		k := mixedKey(i)
		require.NoError(t, lanes.Set(k[:], lfValue(i)))
	}

	stats := c.GetCacheStats()
	require.Positive(t, stats.Evictions)
	require.Equal(t, stats.TotalElementsAdded, stats.Evictions+stats.ValidEntriesCount)
}

// TestTxMetaCache_BucketLocality pins which backends gain from lookups grouped
// by ProbeRangeOf: the bucketed ones take a lock per bucket, the lockfree
// cache has one global index and gains nothing.
func TestTxMetaCache_BucketLocality(t *testing.T) {
	for _, tc := range allBucketTypes {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestTxMetaCache(t, tc.bucket)
			require.Equal(t, tc.bucket != Lockfree, c.BucketLocality())
		})
	}
}

// A scratch rebuilt after its pool was emptied (every GC) is sized for the
// whole batch at once, instead of doubling its way up through the reads.
func TestIntoScratch_ReserveSizesForTheBatch(t *testing.T) {
	sc := &intoScratch{}
	sc.reserve(4096)
	require.GreaterOrEqual(t, cap(sc.raw), 4096*intoScratchBytesPerEntry)
	require.Empty(t, sc.raw)

	big := &intoScratch{}
	big.reserve(1 << 20)
	require.LessOrEqual(t, cap(big.raw), intoScratchMaxRetain, "never reserve more than the pool retains")

	kept := &intoScratch{raw: make([]byte, 0, 1<<20)}
	kept.reserve(16)
	require.Equal(t, 1<<20, cap(kept.raw), "a big enough buffer is kept")
}

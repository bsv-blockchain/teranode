package model

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

func TestGetTxMap_ReusesPooledInstance(t *testing.T) {
	// First Get + Put returns instance to pool.
	m1 := GetTxMap(100_000)
	require.NotNil(t, m1)
	var h chainhash.Hash
	h[0] = 0x42
	require.NoError(t, m1.Put(h, 1))
	require.Equal(t, 1, m1.Length())
	PutTxMap(m1, 100_000)

	// Second Get for the same size class should yield a cleared map.
	// We can't guarantee it's the same instance because sync.Pool is
	// allowed to drop entries, but if we do get a pooled instance the
	// length must be zero.
	m2 := GetTxMap(100_000)
	require.NotNil(t, m2)
	require.Equal(t, 0, m2.Length(), "pooled map must be cleared before reuse")
	PutTxMap(m2, 100_000)
}

func TestGetTxMap_OversizedAllocatesFresh(t *testing.T) {
	// An n above the largest size class is not pooled: GetTxMap allocates fresh
	// and PutTxMap drops it rather than retaining a giant map. Verify that
	// contract through the classification and the Put-drop path.
	//
	// This previously did `m := GetTxMap(2<<30)` — actually allocating a real
	// ~2-billion-entry swiss map. Its constructor writes one control byte per
	// group across 8192 sub-maps (~3 GiB of dirtied pages), which ballooned to
	// ~10 GiB RSS under -race and made model.test the dominant memory user in CI
	// (#1051) — all to exercise a branch that guards blocks larger than any that
	// can exist. (The unbounded fresh allocation for adversarial n is a separate
	// production-hardening concern, not addressed here.)
	require.Equal(t, -1, txMapClassIdxFor(2<<30), "n above the max size class must not map to a pool")

	// PutTxMap with an oversized n must drop the map (idx -1) without panicking.
	m := GetTxMap(1 << 12)
	require.NotNil(t, m)
	require.NotPanics(t, func() { PutTxMap(m, 2<<30) })
}

func TestGetTxMap_DifferentSizeClassesAreSeparate(t *testing.T) {
	// Put a small map, Get a larger one — must not be the same instance.
	small := GetTxMap(1 << 12) // 4K class
	var h chainhash.Hash
	h[0] = 0x01
	require.NoError(t, small.Put(h, 1))
	PutTxMap(small, 1<<12)

	large := GetTxMap(1 << 22) // 4M class
	require.NotNil(t, large)
	require.Equal(t, 0, large.Length())
	// Sanity: the large map is not the small one we just put back.
	require.NotSame(t, small, large)
	PutTxMap(large, 1<<22)
}

func TestGetParentSpendsMap_RoundTrip(t *testing.T) {
	m1 := GetParentSpendsMap(1_000_000)
	require.NotNil(t, m1)
	require.Equal(t, parentSpendsBuckets, m1.NrOfBuckets())

	// Insert a few inpoints.
	for i := 0; i < 100; i++ {
		var inp subtreepkg.Inpoint
		inp.Hash[0] = byte(i)
		inp.Index = uint32(i)
		ok, err := m1.SetIfNotExists(inp)
		require.NoError(t, err)
		require.True(t, ok)
	}
	PutParentSpendsMap(m1, 1_000_000)

	// Re-Get should produce a cleared map for the same size class.
	m2 := GetParentSpendsMap(1_000_000)
	require.NotNil(t, m2)
	// Every previously-inserted inpoint must be absent.
	for i := 0; i < 100; i++ {
		var inp subtreepkg.Inpoint
		inp.Hash[0] = byte(i)
		inp.Index = uint32(i)
		ok, err := m2.SetIfNotExists(inp)
		require.NoError(t, err)
		require.True(t, ok, "cleared map should accept inpoint")
	}
	PutParentSpendsMap(m2, 1_000_000)
}

func TestPools_NilSafe(t *testing.T) {
	// Defensive: PutTxMap(nil) and PutParentSpendsMap(nil) must not panic.
	require.NotPanics(t, func() {
		PutTxMap(nil, 100)
		PutParentSpendsMap(nil, 100)
	})
}

func TestTxMapPool_ConcurrentReuse(t *testing.T) {
	// Sanity check that simultaneous Get/Put traffic doesn't race or
	// hand the same instance to two goroutines at once.
	const goroutines = 16
	const iters = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				m := GetTxMap(10_000)
				require.Equal(t, 0, m.Length())
				var h chainhash.Hash
				h[0] = byte(g)
				h[1] = byte(i)
				require.NoError(t, m.Put(h, uint64(g*iters+i)))
				PutTxMap(m, 10_000)
			}
		}(g)
	}
	wg.Wait()
}

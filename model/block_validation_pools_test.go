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
	// n above the maximum size class returns a fresh map, sized to n,
	// and PutTxMap drops it (does not panic).
	m := GetTxMap(2 << 30) // 2B — above the 1B max class
	require.NotNil(t, m)
	PutTxMap(m, 2<<30)
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
		require.True(t, m1.SetIfNotExists(inp))
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
		require.True(t, m2.SetIfNotExists(inp), "cleared map should accept inpoint")
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

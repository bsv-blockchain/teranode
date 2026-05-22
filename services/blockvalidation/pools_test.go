package blockvalidation

import (
	"sync"
	"testing"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

func TestNodePool_RoundsUpToClass(t *testing.T) {
	// 1500 -> class 4096; len must be 0, cap must equal the class size.
	s := GetNodeSlice(1500)
	require.Equal(t, 0, len(s))
	require.Equal(t, 1<<12, cap(s))
}

func TestNodePool_ExactClassMatch(t *testing.T) {
	for _, class := range nodePoolClasses {
		s := GetNodeSlice(class)
		require.Equalf(t, class, cap(s), "exact-class request must produce cap=%d", class)
	}
}

func TestNodePool_AboveMaxFallsThrough(t *testing.T) {
	max := nodePoolClasses[len(nodePoolClasses)-1]
	s := GetNodeSlice(max + 1)
	require.Equal(t, max+1, cap(s), "above-max must allocate exactly the requested cap, not a pooled class")

	// Put must NOT retain odd caps in the pool.
	PutNodeSlice(s)
	again := GetNodeSlice(max + 1)
	require.Equal(t, max+1, cap(again), "odd-cap slice must not be returned from the pool")
}

func TestNodePool_PutAndGetReusesBacking(t *testing.T) {
	first := GetNodeSlice(2048) // class 4096
	require.Equal(t, 1<<12, cap(first))

	// Stamp a sentinel into the backing array and put it back.
	first = first[:cap(first)]
	first[0].Fee = 0xdeadbeef
	PutNodeSlice(first)

	// Drain the pool until we recover the sentinel (sync.Pool can return nil).
	for i := 0; i < 16; i++ {
		next := GetNodeSlice(2048)
		require.Equal(t, 1<<12, cap(next))
		next = next[:cap(next)]
		if next[0].Fee == 0xdeadbeef {
			return
		}
		// Not our slice; drop it (do not Put — would pollute the pool).
	}
	t.Skip("pool returned a fresh slice every call; sync.Pool eviction makes this expected on some Go versions")
}

func TestNodePool_PutNilIsSafe(t *testing.T) {
	require.NotPanics(t, func() { PutNodeSlice(nil) })
}

func TestNodePool_ConcurrentGetPutNoRace(t *testing.T) {
	const workers = 16
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				s := GetNodeSlice(1024 + j*17)
				PutNodeSlice(s)
			}
		}()
	}
	wg.Wait()
}

func TestNodeAllocFromPool_IsAdapter(t *testing.T) {
	// NodeAllocFromPool must satisfy subtreepkg.NodeAllocator.
	var alloc subtreepkg.NodeAllocator = NodeAllocFromPool
	s := alloc(800)
	require.GreaterOrEqual(t, cap(s), 800)
}

package subtreevalidation

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArenaPool_GetPutLifecycle(t *testing.T) {
	a := getSubtreeArena()
	require.NotNil(t, a)
	require.Equal(t, 0, a.Used())

	_ = a.Alloc(1024)
	require.Equal(t, 1024, a.Used())

	putSubtreeArena(a)
	// After Put, the arena should have been Reset. Get an arena and verify.
	a2 := getSubtreeArena()
	require.Equal(t, 0, a2.Used())
	putSubtreeArena(a2)
}

func TestArenaPool_ShrinkAfterLargeUse(t *testing.T) {
	a := getSubtreeArena()
	_ = a.Alloc(128 << 20) // 128 MiB
	require.GreaterOrEqual(t, a.Cap(), 128<<20)

	putSubtreeArena(a) // ResetAndShrink(64 MiB) — slab dropped

	// sync.Pool does not guarantee returning the same arena, but if it does,
	// its capacity should be 0 (or significantly less than 128 MiB). Either
	// way, the pool should not be retaining a 128 MiB slab indefinitely.
	a2 := getSubtreeArena()
	require.Less(t, a2.Cap(), 128<<20, "oversized slab should have been dropped on Put")
	putSubtreeArena(a2)
}

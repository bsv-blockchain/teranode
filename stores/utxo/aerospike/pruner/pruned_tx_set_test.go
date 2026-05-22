package pruner

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func makeHash(b byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = b
	return h
}

func TestPrunedTxSet_AddAndContains(t *testing.T) {
	set := NewPrunedTxSet(16, 0)

	h1 := makeHash(0x01)
	h2 := makeHash(0x02)
	h3 := makeHash(0x03)

	set.Add(h1)
	set.Add(h2)

	require.True(t, set.Contains(h1))
	require.True(t, set.Contains(h2))
	require.False(t, set.Contains(h3))
}

func TestPrunedTxSet_CheckAndRemove(t *testing.T) {
	set := NewPrunedTxSet(16, 0)

	h1 := makeHash(0x01)
	h2 := makeHash(0x02)

	set.Add(h1)
	set.Add(h2)

	// CheckAndRemove returns true and removes
	require.True(t, set.CheckAndRemove(h1))
	// Second call returns false — already removed
	require.False(t, set.CheckAndRemove(h1))
	require.False(t, set.Contains(h1))

	// h2 still present
	require.True(t, set.Contains(h2))
}

func TestPrunedTxSet_Len(t *testing.T) {
	set := NewPrunedTxSet(16, 0)

	require.Equal(t, 0, set.Len())

	set.Add(makeHash(0x01))
	set.Add(makeHash(0x02))
	require.Equal(t, 2, set.Len())

	set.CheckAndRemove(makeHash(0x01))
	require.Equal(t, 1, set.Len())
}

func TestPrunedTxSet_ConcurrentAccess(t *testing.T) {
	set := NewPrunedTxSet(256, 0)
	const numGoroutines = 100
	const opsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Half the goroutines add entries
	for g := 0; g < numGoroutines; g++ {
		go func(base int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				var h chainhash.Hash
				val := uint16(base*opsPerGoroutine + i)
				h[0] = byte(val >> 8)
				h[1] = byte(val)
				set.Add(h)
			}
		}(g)
	}

	// Other half check and remove
	for g := 0; g < numGoroutines; g++ {
		go func(base int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				var h chainhash.Hash
				val := uint16(base*opsPerGoroutine + i)
				h[0] = byte(val >> 8)
				h[1] = byte(val)
				set.CheckAndRemove(h) // may or may not find it — just must not panic
			}
		}(g)
	}

	wg.Wait()
	// No assertion on final count — just verifying no data races or panics
}

func TestPrunedTxSet_ShardDistribution(t *testing.T) {
	set := NewPrunedTxSet(256, 0)

	// Add hashes with different first bytes to verify they go to different shards
	for i := 0; i < 256; i++ {
		set.Add(makeHash(byte(i)))
	}

	require.Equal(t, 256, set.Len())

	// Remove all
	for i := 0; i < 256; i++ {
		require.True(t, set.CheckAndRemove(makeHash(byte(i))))
	}

	require.Equal(t, 0, set.Len())
}

func TestPrunedTxSet_SimulateChainPruning(t *testing.T) {
	// Simulate a tight chain: A -> B -> C -> D
	// All four TXs are in the same block and will be pruned
	// When processing B, A should be found in the set (skip parent update)
	// When processing C, B should be found (skip parent update)
	// etc.

	set := NewPrunedTxSet(16, 0)

	txA := makeHash(0x0A)
	txB := makeHash(0x0B)
	txC := makeHash(0x0C)
	txD := makeHash(0x0D)

	// Stage 1 (reader) registers all TXIDs before processing starts
	set.Add(txA)
	set.Add(txB)
	set.Add(txC)
	set.Add(txD)

	require.Equal(t, 4, set.Len())

	// Stage 2 (processor) processes B — parent is A
	require.True(t, set.CheckAndRemove(txA), "parent A should be found and removed")

	// Stage 2 processes C — parent is B
	require.True(t, set.CheckAndRemove(txB), "parent B should be found and removed")

	// Stage 2 processes D — parent is C
	require.True(t, set.CheckAndRemove(txC), "parent C should be found and removed")

	// D has no child in this block — stays in set as dangling
	require.True(t, set.Contains(txD))
	require.Equal(t, 1, set.Len())
}

func TestPrunedTxSet_DuplicateAdd(t *testing.T) {
	set := NewPrunedTxSet(16, 0)

	h1 := makeHash(0x01)

	set.Add(h1)
	require.Equal(t, 1, set.Len())

	// Adding the same TXID again should not increment the count
	set.Add(h1)
	require.Equal(t, 1, set.Len())

	// Should still be removable exactly once
	require.True(t, set.CheckAndRemove(h1))
	require.Equal(t, 0, set.Len())
	require.False(t, set.CheckAndRemove(h1))
}

func TestPrunedTxSet_ParentNotInBlock(t *testing.T) {
	// TX_child's parent is NOT in this block — should not be found
	set := NewPrunedTxSet(16, 0)

	txChild := makeHash(0x01)
	txParent := makeHash(0xFF) // parent from a previous block

	set.Add(txChild)

	// Parent not in set — must not skip update
	require.False(t, set.CheckAndRemove(txParent))
}

func TestPrunedTxSet_SoftCap(t *testing.T) {
	// With a cap of 3, the 4th Add must be a silent no-op
	set := NewPrunedTxSet(16, 3)

	h1 := makeHash(0x01)
	h2 := makeHash(0x02)
	h3 := makeHash(0x03)
	h4 := makeHash(0x04)

	set.Add(h1)
	set.Add(h2)
	set.Add(h3)
	require.Equal(t, 3, set.Len())
	require.True(t, set.Saturated())

	// 4th add is dropped — entry is not stored, count does not move
	set.Add(h4)
	require.Equal(t, 3, set.Len())
	require.False(t, set.Contains(h4))

	// Removing an entry frees a slot but Saturated() is sticky-up-to-cap,
	// so a subsequent Add succeeds again once we're below the cap.
	require.True(t, set.CheckAndRemove(h1))
	require.False(t, set.Saturated())
	require.Equal(t, 2, set.Len())

	set.Add(h4)
	require.Equal(t, 3, set.Len())
	require.True(t, set.Contains(h4))
}

func TestPrunedTxSet_UnlimitedWhenCapZero(t *testing.T) {
	set := NewPrunedTxSet(16, 0)
	for i := 0; i < 1000; i++ {
		set.Add(makeHash(byte(i % 256)))
	}
	require.False(t, set.Saturated())
	// 256 distinct first-byte values → 256 entries
	require.Equal(t, 256, set.Len())
}

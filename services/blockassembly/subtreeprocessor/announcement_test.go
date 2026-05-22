package subtreeprocessor

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

func TestNearlyFullSubtree(t *testing.T) {
	t.Run("nil subtree is not nearly full", func(t *testing.T) {
		require.False(t, nearlyFullSubtree(nil))
	})

	t.Run("empty subtree is not nearly full", func(t *testing.T) {
		st, err := subtreepkg.NewTreeByLeafCount(16)
		require.NoError(t, err)
		require.False(t, nearlyFullSubtree(st))
	})

	t.Run("half-full subtree is not nearly full", func(t *testing.T) {
		st := buildSubtreeAtFill(t, 16, 8)
		require.False(t, nearlyFullSubtree(st))
	})

	t.Run("subtree just below threshold is not nearly full", func(t *testing.T) {
		// 16 * 90 / 100 = 14.4, so 14 nodes is below threshold (14 * 100 < 16 * 90)
		st := buildSubtreeAtFill(t, 16, 14)
		require.False(t, nearlyFullSubtree(st))
	})

	t.Run("subtree at threshold is nearly full", func(t *testing.T) {
		// 15 * 100 = 1500 >= 16 * 90 = 1440
		st := buildSubtreeAtFill(t, 16, 15)
		require.True(t, nearlyFullSubtree(st))
	})

	t.Run("completely full subtree is nearly full", func(t *testing.T) {
		st := buildSubtreeAtFill(t, 16, 16)
		require.True(t, nearlyFullSubtree(st))
	})
}

// buildSubtreeAtFill returns a subtree of the given capacity with `length` nodes added.
// The first node is the coinbase placeholder (counts toward Length()).
func buildSubtreeAtFill(t *testing.T, capacity, length int) *subtreepkg.Subtree {
	t.Helper()

	require.LessOrEqual(t, length, capacity, "length must not exceed capacity")

	st, err := subtreepkg.NewTreeByLeafCount(capacity)
	require.NoError(t, err)

	if length == 0 {
		return st
	}

	require.NoError(t, st.AddCoinbaseNode())

	for i := 1; i < length; i++ {
		var h chainhash.Hash
		h[0] = byte(i)
		require.NoError(t, st.AddSubtreeNode(subtreepkg.Node{Hash: h, Fee: 1}))
	}

	require.Equal(t, length, st.Length())
	require.Equal(t, capacity, st.Size())

	return st
}

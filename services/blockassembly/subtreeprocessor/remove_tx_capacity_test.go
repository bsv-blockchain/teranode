package subtreeprocessor

import (
	"testing"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// TestRemoveTxFromSubtrees_KeepsCurrentSubtreeCapacity pins that removing a tx from the
// current subtree keeps its capacity. Duplicate() allocates exactly len(Nodes), so without
// care the next add makes len == cap and completes a short subtree: a block built from it
// has a non-last subtree shorter than the first one, which makes the block invalid.
func TestRemoveTxFromSubtrees_KeepsCurrentSubtreeCapacity(t *testing.T) {
	stp, _ := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+100_000)
	hashes := addBlockMaxSizeTestTxs(t, stp, sizes(10, blockMaxSizeTestTxSize)...)

	require.NoError(t, stp.removeTxFromSubtrees(t.Context(), hashes[3]))

	current := stp.currentSubtree.Load()
	require.Equal(t, 64, current.Size(), "the current subtree must keep its capacity")
	require.False(t, current.IsComplete())

	addBlockMaxSizeTestTxs(t, stp, sizes(2, blockMaxSizeTestTxSize)...)

	require.Empty(t, stp.chainedSubtrees, "a partly filled subtree must not be completed")
	require.Len(t, stp.currentSubtree.Load().Nodes, 12) // placeholder + 9 + 2
}

// TestRemoveTxFromSubtrees_RetiresReplacedCurrentSubtree checks that the current subtree
// replaced by a removal is retired, so an mmap-backed one is released at the next block.
func TestRemoveTxFromSubtrees_RetiresReplacedCurrentSubtree(t *testing.T) {
	stp, _ := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+100_000, WithMmapDir(t.TempDir()))
	hashes := addBlockMaxSizeTestTxs(t, stp, sizes(10, blockMaxSizeTestTxSize)...)

	original := stp.currentSubtree.Load()
	require.True(t, original.IsMmapBacked())

	require.NoError(t, stp.removeTxFromSubtrees(t.Context(), hashes[3]))

	require.Equal(t, []*subtreepkg.Subtree{original}, stp.retiredSubtrees)
}

// TestRemoveTxFromSubtrees_RetiresReplacedChainedSubtree checks that the chained subtree a
// removal replaces with a copy is retired, so an mmap-backed original is unmapped at the
// next block instead of leaking its mapping and file.
func TestRemoveTxFromSubtrees_RetiresReplacedChainedSubtree(t *testing.T) {
	stp, _ := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+100_000, WithMmapDir(t.TempDir()))
	hashes := addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)

	// hashes[70] lives in the second chained subtree
	original := stp.chainedSubtrees[1]
	require.True(t, original.IsMmapBacked())

	require.NoError(t, stp.removeTxFromSubtrees(t.Context(), hashes[70]))

	require.Contains(t, stp.retiredSubtrees, original)
}

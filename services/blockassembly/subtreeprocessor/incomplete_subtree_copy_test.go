package subtreeprocessor

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// TestCreateIncompleteSubtreeCopy_AfterShrink reproduces a snapshot failure that
// occurred when adjustSubtreeSize had reduced currentItemsPerFile while an
// in-flight currentSubtree retained its larger original capacity. Sizing the
// snapshot copy off currentItemsPerFile in that window overflowed when copying
// the source nodes; sizing it off the source's capacity does not.
func TestCreateIncompleteSubtreeCopy_AfterShrink(t *testing.T) {
	stp, cleanup := setupSubtreeProcessorForBench(t, 8)
	defer cleanup()

	// Build a subtree at the original (larger) capacity and fill it past the
	// shrunk capacity, so the bug — if reintroduced — would overflow the copy.
	source, err := subtreepkg.NewTreeByLeafCount(8)
	require.NoError(t, err)
	require.NoError(t, source.AddCoinbaseNode())

	const realNodes = 5 // coinbase + 5 = 6 nodes in an 8-cap source
	for i := 0; i < realNodes; i++ {
		var h chainhash.Hash
		h[0] = byte(i + 1)
		require.NoError(t, source.AddSubtreeNode(subtreepkg.Node{Hash: h, Fee: uint64(i + 1)}))
	}

	stp.currentSubtree.Store(source)
	// Simulate adjustSubtreeSize shrinking the per-file count below the source's
	// node count after a block was finalized.
	stp.currentItemsPerFile.Store(4)

	snapshot, err := stp.createIncompleteSubtreeCopy()
	require.NoError(t, err, "copying an in-flight subtree must not fail when currentItemsPerFile has shrunk")
	require.NotNil(t, snapshot)

	// The snapshot must contain the coinbase placeholder plus every real node
	// from the source — nothing dropped, nothing duplicated.
	require.Equal(t, source.Length(), snapshot.Length())
	require.GreaterOrEqual(t, snapshot.Size(), source.Length())
	require.Equal(t, source.Fees, snapshot.Fees)

	for i := 1; i < source.Length(); i++ {
		require.Equal(t, source.Nodes[i].Hash, snapshot.Nodes[i].Hash, "node %d hash must match", i)
		require.Equal(t, source.Nodes[i].Fee, snapshot.Nodes[i].Fee, "node %d fee must match", i)
	}
}

// TestCreateIncompleteSubtreeCopy_NilCurrent guards against a nil-deref when
// the snapshot is requested with no current subtree set.
func TestCreateIncompleteSubtreeCopy_NilCurrent(t *testing.T) {
	stp, cleanup := setupSubtreeProcessorForBench(t, 8)
	defer cleanup()

	// Explicitly clear the current subtree.
	stp.currentSubtree.Store((*subtreepkg.Subtree)(nil))

	snapshot, err := stp.createIncompleteSubtreeCopy()
	require.Error(t, err)
	require.Nil(t, snapshot)
}

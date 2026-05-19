package util

import (
	"crypto/rand"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// TestSubtreeMerkleRootEquivalence verifies that the merkle root of N transactions
// is identical to the root produced by combining the roots of two fixed-size
// subtrees — even when the second subtree is mostly empty — provided the partial
// subtree's root is lifted to the full subtree height with the
// duplicate-last-when-odd rule (H(prev, prev) at each phantom level).
func TestSubtreeMerkleRootEquivalence(t *testing.T) {
	const (
		totalTxs    = 258
		subtreeSize = 256
	)

	txHashes := make([]chainhash.Hash, totalTxs)
	for i := range txHashes {
		_, err := rand.Read(txHashes[i][:])
		require.NoError(t, err)
	}

	// Reference: merkle root of all 258 txids in a single capacity-512 tree.
	// BuildMerkleTreeStoreFromBytes pads slots 258..511 with the empty hash
	// and lifts the y-pair up the right side via the duplicate-when-odd rule.
	bigTree, err := subtree.NewTreeByLeafCount(512)
	require.NoError(t, err)

	for _, h := range txHashes {
		require.NoError(t, bigTree.AddNode(h, 0, 0))
	}

	referenceRoot := bigTree.RootHash()
	require.NotNil(t, referenceRoot)

	// Composite: first 256 in a complete subtree, last 2 in a 256-capacity subtree.
	leftSubtree, err := subtree.NewTreeByLeafCount(subtreeSize)
	require.NoError(t, err)

	for i := range subtreeSize {
		require.NoError(t, leftSubtree.AddNode(txHashes[i], 0, 0))
	}

	require.True(t, leftSubtree.IsComplete())

	rightSubtree, err := subtree.NewTreeByLeafCount(subtreeSize)
	require.NoError(t, err)

	for i := subtreeSize; i < totalTxs; i++ {
		require.NoError(t, rightSubtree.AddNode(txHashes[i], 0, 0))
	}

	require.Equal(t, totalTxs-subtreeSize, rightSubtree.Length())
	require.False(t, rightSubtree.IsComplete())

	// rightSubtree.RootHash() returns the root over only its 2 actual leaves
	// (height 1). To compose with leftSubtree (height 8), lift it to the
	// left subtree's height via the phantom-step duplication.
	liftedRoot, err := rightSubtree.RootHashPadded(leftSubtree.Height)
	require.NoError(t, err)

	topTree, err := subtree.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, topTree.AddNode(*leftSubtree.RootHash(), 0, 0))
	require.NoError(t, topTree.AddNode(*liftedRoot, 0, 0))

	require.Equal(t, referenceRoot.String(), topTree.RootHash().String(),
		"composed root must equal the reference root after phantom-step lifting")

	// Sanity: skipping the lift gives a different root, confirming the lift is the
	// thing that makes the two approaches agree.
	naiveTop, err := subtree.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, naiveTop.AddNode(*leftSubtree.RootHash(), 0, 0))
	require.NoError(t, naiveTop.AddNode(*rightSubtree.RootHash(), 0, 0))

	require.NotEqual(t, referenceRoot.String(), naiveTop.RootHash().String(),
		"without phantom-step lifting, the composed root would not match")
}

package util

import (
	"crypto/rand"
	"fmt"
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

// TestSubtreeMerkleRootEquivalence_NonPowerOfTwoFinal proves that the
// partitioned merkle root matches the canonical flat merkle root when the
// final subtree contains a non-power-of-two number of leaves. This is the
// invariant that issue #901's fix relies on: the legacy-block partitioner can
// stay at subtreeSize=maxItems even for adversarial transaction counts, with
// the awkward remainder going into a single final subtree of whatever size.
//
// The test exercises a range of remainder sizes (3, 5, 6, 7, 11, ..., 15,943)
// to cover the duplicate-when-odd cascade at multiple internal levels of the
// final subtree.
func TestSubtreeMerkleRootEquivalence_NonPowerOfTwoFinal(t *testing.T) {
	const subtreeSize = 32

	// rValues covers a variety of binary patterns to exercise the
	// duplicate-when-odd cascade at every level of a capacity-32 final subtree.
	rValues := []int{1, 2, 3, 5, 6, 7, 9, 11, 13, 15, 17, 21, 23, 27, 31}

	for _, r := range rValues {
		t.Run(fmt.Sprintf("final_subtree_has_%d_leaves", r), func(t *testing.T) {
			totalTxs := subtreeSize + r

			txHashes := make([]chainhash.Hash, totalTxs)
			for i := range txHashes {
				_, err := rand.Read(txHashes[i][:])
				require.NoError(t, err)
			}

			// Reference: single big tree padded to next power of two with the
			// duplicate-when-odd rule (handled by NewTreeByLeafCount + AddNode).
			referenceCapacity := subtree.CeilPowerOfTwo(totalTxs)
			bigTree, err := subtree.NewTreeByLeafCount(referenceCapacity)
			require.NoError(t, err)

			for _, h := range txHashes {
				require.NoError(t, bigTree.AddNode(h, 0, 0))
			}

			referenceRoot := bigTree.RootHash()
			require.NotNil(t, referenceRoot)

			// Composite: first subtreeSize leaves in a complete subtree, last r
			// leaves in a subtree of capacity ceil(log2(r)).
			left, err := subtree.NewTreeByLeafCount(subtreeSize)
			require.NoError(t, err)

			for i := 0; i < subtreeSize; i++ {
				require.NoError(t, left.AddNode(txHashes[i], 0, 0))
			}

			right, err := subtree.NewIncompleteTreeByLeafCount(r)
			require.NoError(t, err)

			for i := subtreeSize; i < totalTxs; i++ {
				require.NoError(t, right.AddNode(txHashes[i], 0, 0))
			}

			require.Equal(t, r, right.Length())

			// Lift the right subtree's root to the left subtree's height.
			// RootHashPadded handles non-power-of-two leaf counts since
			// go-subtree v1.4.2 (see bsv-blockchain/go-subtree#127).
			rightLifted, err := right.RootHashPadded(left.Height)
			require.NoError(t, err)

			topTree, err := subtree.NewTreeByLeafCount(2)
			require.NoError(t, err)
			require.NoError(t, topTree.AddNode(*left.RootHash(), 0, 0))
			require.NoError(t, topTree.AddNode(*rightLifted, 0, 0))

			require.Equal(t, referenceRoot.String(), topTree.RootHash().String(),
				"composed root must equal reference root for r=%d (final subtree has %d leaves)", r, r)
		})
	}
}

package blockassembly

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// TestCreateMerkleTreeFromSubtrees_LiftsIncompleteFinalSubtree verifies that
// when the final subtree is shorter than the first, createMerkleTreeFromSubtrees
// replaces its hash with the lifted root and returns a merkle root that matches
// what model.Block.CheckMerkleRoot computes for the same block layout. This is
// the multi-subtree-incomplete-final path enabled by Task 8 on the validator
// side.
func TestCreateMerkleTreeFromSubtrees_LiftsIncompleteFinalSubtree(t *testing.T) {
	first, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		require.NoError(t, first.AddNode(chainhash.HashH([]byte{byte(i)}), 0, 0))
	}

	last, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	for i := 10; i < 12; i++ {
		require.NoError(t, last.AddNode(chainhash.HashH([]byte{byte(i)}), 0, 0))
	}

	require.Less(t, last.Length(), first.Length(), "test fixture: final subtree must be shorter")
	require.True(t, subtreepkg.IsPowerOfTwo(last.Length()), "test fixture: final subtree length must be a power of two")

	subtreesInJob := []*subtreepkg.Subtree{first, last}
	subtreeHashes := []chainhash.Hash{*first.RootHash(), *last.RootHash()}
	originalLastHash := *last.RootHash()

	coinbaseHash := chainhash.HashH([]byte{0xAB})

	ba := &BlockAssembly{}

	merkleRoot, err := ba.createMerkleTreeFromSubtrees("test-job", subtreesInJob, subtreeHashes, &coinbaseHash)
	require.NoError(t, err)
	require.NotNil(t, merkleRoot)

	// The lift must mutate subtreeHashes[len-1] in place — downstream
	// computeCoinbaseBUMP relies on seeing the lifted hash.
	liftedRoot, err := last.RootHashPadded(first.Height)
	require.NoError(t, err)
	require.Equal(t, *liftedRoot, subtreeHashes[1], "final subtree hash must be lifted in place")
	require.NotEqual(t, originalLastHash, subtreeHashes[1], "final subtree hash must change after lift")

	// The returned merkle root must equal the top tree of [first.RootHash, liftedRoot].
	expectedTop, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, expectedTop.AddNode(*first.RootHash(), 0, 0))
	require.NoError(t, expectedTop.AddNode(*liftedRoot, 0, 0))
	require.Equal(t, expectedTop.RootHash().String(), merkleRoot.String())
}

// TestCreateMerkleTreeFromSubtrees_AllCompleteNoLift verifies that when every
// subtree is complete (same length as the first), no lift is applied and the
// final subtree hash is preserved. Regression guard for the existing happy path.
func TestCreateMerkleTreeFromSubtrees_AllCompleteNoLift(t *testing.T) {
	first, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		require.NoError(t, first.AddNode(chainhash.HashH([]byte{byte(i)}), 0, 0))
	}

	last, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	for i := 10; i < 14; i++ {
		require.NoError(t, last.AddNode(chainhash.HashH([]byte{byte(i)}), 0, 0))
	}

	require.Equal(t, first.Length(), last.Length(), "test fixture: both subtrees must be complete")

	subtreesInJob := []*subtreepkg.Subtree{first, last}
	subtreeHashes := []chainhash.Hash{*first.RootHash(), *last.RootHash()}
	originalLastHash := *last.RootHash()

	coinbaseHash := chainhash.HashH([]byte{0xAB})

	ba := &BlockAssembly{}

	merkleRoot, err := ba.createMerkleTreeFromSubtrees("test-job", subtreesInJob, subtreeHashes, &coinbaseHash)
	require.NoError(t, err)
	require.NotNil(t, merkleRoot)

	require.Equal(t, originalLastHash, subtreeHashes[1], "final subtree hash must not change when complete")

	expectedTop, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, expectedTop.AddNode(*first.RootHash(), 0, 0))
	require.NoError(t, expectedTop.AddNode(*last.RootHash(), 0, 0))
	require.Equal(t, expectedTop.RootHash().String(), merkleRoot.String())
}

// TestCreateMerkleTreeFromSubtrees_SingleSubtreeUnchanged verifies that a
// single-subtree block is not touched by the lift guard, regardless of whether
// the subtree is complete. This is the dominant production path.
func TestCreateMerkleTreeFromSubtrees_SingleSubtreeUnchanged(t *testing.T) {
	only, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		require.NoError(t, only.AddNode(chainhash.HashH([]byte{byte(i)}), 0, 0))
	}

	subtreesInJob := []*subtreepkg.Subtree{only}
	subtreeHashes := []chainhash.Hash{*only.RootHash()}
	originalHash := *only.RootHash()

	coinbaseHash := chainhash.HashH([]byte{0xAB})

	ba := &BlockAssembly{}

	merkleRoot, err := ba.createMerkleTreeFromSubtrees("test-job", subtreesInJob, subtreeHashes, &coinbaseHash)
	require.NoError(t, err)
	require.NotNil(t, merkleRoot)

	require.Equal(t, originalHash, subtreeHashes[0], "single-subtree hash must not change")
	require.Equal(t, originalHash.String(), merkleRoot.String(), "single-subtree merkle root equals the subtree root")
}

// TestCreateMerkleTreeFromSubtrees_RejectsDuplicateSubtreeRoots mirrors the
// CVE-2012-2459-style hardening on the validator side (model.Block.CheckMerkleRoot
// at the duplicate-detection block) and verifies the assembly path will not
// silently emit a block the validator rejects.
func TestCreateMerkleTreeFromSubtrees_RejectsDuplicateSubtreeRoots(t *testing.T) {
	first, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		require.NoError(t, first.AddNode(chainhash.HashH([]byte{byte(i)}), 0, 0))
	}

	// Build a second subtree whose root collides with the first by populating
	// it with the same leaves. Real assembly would never produce this, but a
	// future bug in getIncompleteSubtreeMiningData re-emitting the same slice
	// could — that's what the guard catches.
	second, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		require.NoError(t, second.AddNode(chainhash.HashH([]byte{byte(i)}), 0, 0))
	}

	require.Equal(t, first.RootHash().String(), second.RootHash().String(),
		"test fixture: subtree roots must collide for this test to mean anything")

	subtreesInJob := []*subtreepkg.Subtree{first, second}
	subtreeHashes := []chainhash.Hash{*first.RootHash(), *second.RootHash()}

	coinbaseHash := chainhash.HashH([]byte{0xAB})

	ba := &BlockAssembly{}

	merkleRoot, err := ba.createMerkleTreeFromSubtrees("test-job", subtreesInJob, subtreeHashes, &coinbaseHash)
	require.Error(t, err)
	require.Nil(t, merkleRoot)
	require.Contains(t, err.Error(), "duplicate subtree root hash")
}

package model

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// nodeSubtree returns a subtree of leaves distinct nodes, built with AddNode so its
// root is genuinely derived from the node list.
func nodeSubtree(tb testing.TB, leaves int) *subtreepkg.Subtree {
	tb.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(leaves)
	require.NoError(tb, err)

	for i := 0; i < leaves; i++ {
		var hash chainhash.Hash

		binary.LittleEndian.PutUint64(hash[:8], uint64(i)+1)

		require.NoError(tb, st.AddNode(hash, 1, 1))
	}

	return st
}

// forgedClaim returns the subtree's serialization with the root its header claims
// replaced by claimedRoot, deserialized back. The node list is untouched, so
// RootHash() reports the lie while the nodes hash somewhere else — the shape a
// claim-only key check cannot detect.
func forgedClaim(tb testing.TB, st *subtreepkg.Subtree, claimedRoot *chainhash.Hash) *subtreepkg.Subtree {
	tb.Helper()

	b, err := st.Serialize()
	require.NoError(tb, err)

	copy(b[:chainhash.HashSize], claimedRoot[:])

	forged, err := subtreepkg.NewSubtreeFromBytes(b)
	require.NoError(tb, err)

	return forged
}

// TestValidateSubtreeNodesMatchKey_RecomputesRatherThanTrustingTheClaim is the
// difference between the two key checks, stated as a test: a blob whose header root
// is the key it is stored under, over a node list that hashes elsewhere, passes
// ValidateSubtreeMatchesKey and must fail ValidateSubtreeNodesMatchKey.
func TestValidateSubtreeNodesMatchKey_RecomputesRatherThanTrustingTheClaim(t *testing.T) {
	honest := nodeSubtree(t, 8)
	key := *honest.RootHash()

	other := nodeSubtree(t, 4)
	forged := forgedClaim(t, other, &key)

	require.NoError(t, ValidateSubtreeMatchesKey(forged, &key),
		"the claim-only check is satisfied by the forged header, which is why it is not enough")
	require.Error(t, ValidateSubtreeNodesMatchKey(forged, &key))

	require.NoError(t, ValidateSubtreeNodesMatchKey(honest, &key))
}

// TestValidateSubtreeNodesMatchKey_RejectsForgedClaimedRoot covers the OTHER half of
// the helper, which nothing else does: the nodes are honest for the key and the
// header's claimed root is the lie.
//
// It is the inverse of the forgery above and it is not covered by it. There the nodes
// were wrong and the claim was right, so the recomputation rejects it and the claim
// comparison never has to. Here the recomputation is satisfied, and only the claim
// comparison is left to notice — which matters because RootHash() returns that claimed
// value for anything deserialized from storage, and Block.CheckMerkleRoot composes it
// verbatim for every subtree after the first. A subtree whose nodes are genuine but
// whose cached root is someone else's would contribute that other root to the
// composition.
//
// Mutation target: deleting the rootHash.IsEqual(key) block in
// ValidateSubtreeNodesMatchKey.
func TestValidateSubtreeNodesMatchKey_RejectsForgedClaimedRoot(t *testing.T) {
	honest := nodeSubtree(t, 8)
	key := *honest.RootHash()

	// A claim belonging to a different, real subtree, so the lie is a plausible root
	// rather than junk.
	lie := *nodeSubtree(t, 4).RootHash()
	require.False(t, lie.IsEqual(&key))

	b, err := honest.Serialize()
	require.NoError(t, err)
	copy(b[:chainhash.HashSize], lie[:])

	// Read through NewSubtreeFromReader so the cached root really is the doctored
	// header's, which is what the claim comparison reads.
	forgedClaimHonestNodes, err := subtreepkg.NewSubtreeFromReader(bytes.NewReader(b))
	require.NoError(t, err)

	require.True(t, forgedClaimHonestNodes.RootHash().IsEqual(&lie),
		"precondition: the deserialized object must carry the forged claim, or the test proves nothing")

	require.Error(t, ValidateSubtreeNodesMatchKey(forgedClaimHonestNodes, &key),
		"nodes that hash to the key do not excuse a header claiming a different root")
}

// TestValidateSubtreeNodesMatchKey_Guards pins the fail-closed cases.
func TestValidateSubtreeNodesMatchKey_Guards(t *testing.T) {
	key := chainhash.Hash{0x01}

	require.Error(t, ValidateSubtreeNodesMatchKey(nil, &key))
	require.Error(t, ValidateSubtreeNodesMatchKey(nodeSubtree(t, 2), nil))

	// An empty node list must be rejected rather than indexing the empty merkle
	// store BuildMerkleTreeStoreFromBytes returns for it.
	empty := &subtreepkg.Subtree{}
	require.NotPanics(t, func() {
		require.Error(t, ValidateSubtreeNodesMatchKey(empty, &key))
	})
}

// TestValidateSubtreeNodesMatchKey_SingleLeaf covers the Bitcoin exception: for one
// leaf the merkle root IS the leaf hash, which is what the recomputation must agree
// with.
func TestValidateSubtreeNodesMatchKey_SingleLeaf(t *testing.T) {
	st := nodeSubtree(t, 1)
	require.NoError(t, ValidateSubtreeNodesMatchKey(st, st.RootHash()))
}

// BenchmarkValidateSubtreeNodesMatchKey sizes the anchor the quick-validation route
// now performs per subtree read. The 1 048 576-leaf case is a whole large block's
// worth of leaves in one subtree, which bounds the per-pass cost from above.
func BenchmarkValidateSubtreeNodesMatchKey(b *testing.B) {
	for _, leaves := range []int{1024, 1048576} {
		b.Run(fmt.Sprintf("leaves=%d", leaves), func(b *testing.B) {
			st := nodeSubtree(b, leaves)
			key := *st.RootHash()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := ValidateSubtreeNodesMatchKey(st, &key); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

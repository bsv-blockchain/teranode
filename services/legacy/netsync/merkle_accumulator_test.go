package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// buildSubtrees partitions leafCount leaves into subtrees of maxItems, filling
// slot zero of the first subtree with the coinbase placeholder exactly as
// prepareSubtrees does. It returns the subtrees and the leaf hashes in order.
func buildSubtrees(t *testing.T, leafCount, maxItems int) ([]*subtreepkg.Subtree, []chainhash.Hash) {
	t.Helper()

	size, k, finalLeaves, err := partitionLegacyBlock(leafCount, maxItems)
	require.NoError(t, err)

	subtrees := make([]*subtreepkg.Subtree, k)
	leaves := make([]chainhash.Hash, 0, leafCount)

	next := 0

	for i := 0; i < k; i++ {
		capacity := size
		if i == k-1 && k > 1 && finalLeaves < size {
			capacity = finalLeaves
		}

		st, err := subtreepkg.NewIncompleteTreeByLeafCount(capacity)
		require.NoError(t, err)

		if i == 0 {
			require.NoError(t, st.AddCoinbaseNode())
			leaves = append(leaves, chainhash.Hash{})
			next++
		}

		for st.Length() < capacity {
			var h chainhash.Hash
			h[0] = byte(next)
			h[1] = byte(next >> 8)
			h[2] = byte(next >> 16)

			require.NoError(t, st.AddNode(h, 0, 100))

			leaves = append(leaves, h)
			next++
		}

		subtrees[i] = st
	}

	return subtrees, leaves
}

// TestMerkleAccumulator_MatchesCheckMerkleRoot is the contract. The accumulator
// exists to avoid holding every subtree at once, so the only thing that makes it
// safe is producing a root the real model.Block.CheckMerkleRoot accepts — the
// function every block in the chain is actually validated against, not a
// transcription of its logic. referenceRootFromSubtrees (below) omits every one
// of that function's rejection guards, so a match against it would prove
// nothing about safety; this test builds a real model.Block over the same
// coinbase and subtree slices instead and asks CheckMerkleRoot directly. The
// lift rules, the power-of-two guard on the first subtree and the duplicate
// check are subtle and security-relevant, so this asserts against the real
// function rather than re-deriving them.
func TestMerkleAccumulator_MatchesCheckMerkleRoot(t *testing.T) {
	cb := coinbaseTx(t)
	coinbaseID := cb.TxIDChainHash()
	coinbaseSize := uint64(cb.Size())

	for _, tc := range []struct {
		name      string
		leafCount int
		maxItems  int
	}{
		{"single subtree exactly full", 8, 8},
		{"single partial subtree", 5, 8},
		{"two full subtrees", 16, 8},
		{"final subtree short", 20, 8},
		{"final subtree holds one leaf", 17, 8},
		{"mainnet subtree size", 9000, 4096},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subtrees, _ := buildSubtrees(t, tc.leafCount, tc.maxItems)

			acc, err := newMerkleAccumulator(len(subtrees), coinbaseID, coinbaseSize)
			require.NoError(t, err)

			for i, st := range subtrees {
				require.NoError(t, acc.Add(st, i == len(subtrees)-1))
			}

			got, err := acc.Root()
			require.NoError(t, err)

			require.NoError(t, checkMerkleRootAgainst(t, cb, subtrees, got),
				"the accumulator's root must be the one the real model.Block.CheckMerkleRoot accepts, or it is not a safe substitute")
		})
	}

	// The final reviewer's own check: the crafted non-power-of-two [3, 2]
	// partition from TestMerkleAccumulator_RejectsANonPowerOfTwoFirstSubtree,
	// run through the real CheckMerkleRoot instead of the accumulator, rejected
	// for the same reason.
	t.Run("rejects the crafted non-power-of-two partition, same reason as the accumulator", func(t *testing.T) {
		first, err := subtreepkg.NewIncompleteTreeByLeafCount(3)
		require.NoError(t, err)
		require.NoError(t, first.AddCoinbaseNode())
		require.NoError(t, first.AddNode(chainhash.Hash{0x01}, 0, 100))
		require.NoError(t, first.AddNode(chainhash.Hash{0x02}, 0, 100))

		second, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
		require.NoError(t, err)
		require.NoError(t, second.AddNode(chainhash.Hash{0x03}, 0, 100))
		require.NoError(t, second.AddNode(chainhash.Hash{0x04}, 0, 100))

		// The placeholder root does not matter: CheckMerkleRoot must reject this
		// partition on the power-of-two guard before it ever reaches a root
		// comparison.
		err = checkMerkleRootAgainst(t, cb, []*subtreepkg.Subtree{first, second}, &chainhash.Hash{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "not a power of two")
	})
}

// checkMerkleRootAgainst constructs a model.Block over the same coinbase and
// subtree slices the caller fed the accumulator, stamps root as the header's
// merkle root, and asks the real model.Block.CheckMerkleRoot to accept or
// reject it.
func checkMerkleRootAgainst(t *testing.T, cb *bt.Tx, subtrees []*subtreepkg.Subtree, root *chainhash.Hash) error {
	t.Helper()

	// CheckMerkleRoot only checks the length of block.Subtrees against
	// block.SubtreeSlices, never the hash values in it, so any correctly sized
	// slice will do.
	subtreeHashes := make([]*chainhash.Hash, len(subtrees))

	// HashPrevBlock must be non-nil: a failing CheckMerkleRoot builds the error
	// message via Block.String(), which hashes the header, and the header's
	// Bytes() dereferences HashPrevBlock unconditionally.
	header := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: root,
	}

	block, err := model.NewBlock(header, cb, subtreeHashes, 0, 0, 0, 0)
	require.NoError(t, err)

	block.SubtreeSlices = subtrees

	return block.CheckMerkleRoot(context.Background())
}

// TestMerkleAccumulator_RejectsANonPowerOfTwoFirstSubtree pins the guard that
// stops a peer crafting a partition whose root a canonical validator would not
// agree with. Without it, lengths like [3, 2] produce a root SV Node rejects.
func TestMerkleAccumulator_RejectsANonPowerOfTwoFirstSubtree(t *testing.T) {
	coinbaseID := chainhash.Hash{0xcb}

	first, err := subtreepkg.NewIncompleteTreeByLeafCount(3)
	require.NoError(t, err)
	require.NoError(t, first.AddCoinbaseNode())
	require.NoError(t, first.AddNode(chainhash.Hash{0x01}, 0, 100))
	require.NoError(t, first.AddNode(chainhash.Hash{0x02}, 0, 100))

	second, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, second.AddNode(chainhash.Hash{0x03}, 0, 100))
	require.NoError(t, second.AddNode(chainhash.Hash{0x04}, 0, 100))

	acc, err := newMerkleAccumulator(2, &coinbaseID, 200)
	require.NoError(t, err)

	require.NoError(t, acc.Add(first, false))

	err = acc.Add(second, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a power of two",
		"the message must name the actual fault, or a future change can satisfy this test without enforcing the rule")
}

// TestMerkleAccumulator_RejectsADuplicateSubtreeRoot pins the top-tree
// malleability guard: two distinct subtrees folding to the same root hash. This
// is NOT the CVE-2012-2459 transaction-level duplication check — that scan is
// model.CheckSubtreeSlicesForDuplicateTxs, needs every subtree's transactions at
// once, and is not present in this component at all.
func TestMerkleAccumulator_RejectsADuplicateSubtreeRoot(t *testing.T) {
	coinbaseID := chainhash.Hash{0xcb}

	subtrees, _ := buildSubtrees(t, 16, 8)

	acc, err := newMerkleAccumulator(3, &coinbaseID, 200)
	require.NoError(t, err)

	require.NoError(t, acc.Add(subtrees[0], false))
	require.NoError(t, acc.Add(subtrees[1], false))

	err = acc.Add(subtrees[1], true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate subtree root",
		"a repeated subtree root is the CVE-2012-2459 shape and must be named as such")
}

// TestMerkleAccumulator_RejectsAShortNonFinalSubtree pins the rule that only the
// final subtree may be incomplete. A short subtree in the middle changes where
// every later leaf sits in the tree.
func TestMerkleAccumulator_RejectsAShortNonFinalSubtree(t *testing.T) {
	coinbaseID := chainhash.Hash{0xcb}

	subtrees, _ := buildSubtrees(t, 24, 8)

	short, err := subtreepkg.NewIncompleteTreeByLeafCount(8)
	require.NoError(t, err)
	require.NoError(t, short.AddNode(chainhash.Hash{0xaa}, 0, 100))

	acc, err := newMerkleAccumulator(3, &coinbaseID, 200)
	require.NoError(t, err)

	require.NoError(t, acc.Add(subtrees[0], false))

	err = acc.Add(short, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "only the final subtree may be incomplete")
}

// TestMerkleAccumulator_RejectsAnOversizedFinalSubtree pins the guard that
// rejects a final subtree longer than the target set by the first subtree. A
// final subtree bigger than the others changes where later leaves sit in the
// tree, so a root computed over it would not match what a canonical validator
// produces.
func TestMerkleAccumulator_RejectsAnOversizedFinalSubtree(t *testing.T) {
	coinbaseID := chainhash.Hash{0xcb}

	first, err := subtreepkg.NewIncompleteTreeByLeafCount(8)
	require.NoError(t, err)
	require.NoError(t, first.AddCoinbaseNode())

	for i := 1; i < 8; i++ {
		var h chainhash.Hash
		h[0] = byte(i)

		require.NoError(t, first.AddNode(h, 0, 100))
	}

	oversized, err := subtreepkg.NewIncompleteTreeByLeafCount(10)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		var h chainhash.Hash
		h[0] = byte(0x10 + i)

		require.NoError(t, oversized.AddNode(h, 0, 100))
	}

	acc, err := newMerkleAccumulator(2, &coinbaseID, 200)
	require.NoError(t, err)

	require.NoError(t, acc.Add(first, false))

	err = acc.Add(oversized, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds",
		"a final subtree bigger than the target must be named as exceeding it, or a future change can satisfy this test without enforcing the rule")
}

// referenceRootFromSubtrees is a root-value oracle, not a validator: it computes
// the merkle root the way CheckMerkleRoot does, holding every subtree at once,
// but it carries none of that function's rejection guards. It does not check
// that the first subtree's length is a power of two, that only the final
// subtree is incomplete, that no final subtree exceeds the target length, or
// that no subtree root repeats. Given a malformed partition it will compute a
// root instead of returning an error, so it must never be handed one in the
// expectation of a failure — the four negative tests exercise the guards
// directly against the accumulator instead. Valid only for well-formed
// partitions, this exists so the accumulator's root VALUE can be asserted
// equal to it.
func referenceRootFromSubtrees(subtrees []*subtreepkg.Subtree, coinbaseID *chainhash.Hash, coinbaseSize uint64) (*chainhash.Hash, error) {
	hashes := make([]chainhash.Hash, len(subtrees))

	for i, st := range subtrees {
		if i == 0 {
			root, err := st.RootHashWithReplaceRootNode(coinbaseID, 0, coinbaseSize)
			if err != nil {
				return nil, err
			}

			hashes[i] = *root

			continue
		}

		hashes[i] = *st.RootHash()
	}

	if len(hashes) == 1 {
		return &hashes[0], nil
	}

	targetLength := subtrees[0].Length()
	targetHeight := subtrees[0].Height

	last := subtrees[len(subtrees)-1]
	if last.Length() < targetLength {
		lifted, err := last.RootHashPadded(targetHeight)
		if err != nil {
			return nil, err
		}

		hashes[len(hashes)-1] = *lifted
	}

	top, err := subtreepkg.NewIncompleteTreeByLeafCount(len(subtrees))
	if err != nil {
		return nil, err
	}

	for _, h := range hashes {
		if err = top.AddNode(h, 1, 0); err != nil {
			return nil, err
		}
	}

	root := top.RootHash()

	return chainhash.NewHash(root[:])
}

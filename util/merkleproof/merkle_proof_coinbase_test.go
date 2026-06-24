package merkleproof

import (
	"math/bits"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// A real coinbase transaction (reused from blockassembly tests). Its txid is what the block
// header's merkle root is actually computed with, in place of the coinbase placeholder.
const testCoinbaseHex = "02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000"

// canonicalMerkleRoot computes the block merkle root the same way model.Block.CheckMerkleRoot does:
//   - subtree 0: replace the coinbase placeholder at Nodes[0] with the real coinbase txid
//   - middle subtrees: their natural root
//   - final subtree: lifted (padded) to the first subtree's height if it is incomplete
//
// then build the top-level merkle tree over those roots.
func canonicalMerkleRoot(t *testing.T, subtrees []*subtreepkg.Subtree, coinbaseTx *bt.Tx) *chainhash.Hash {
	t.Helper()

	targetLength := subtrees[0].Length()
	targetHeight := subtrees[0].Height

	roots := make([]subtreepkg.Node, len(subtrees))

	for i, st := range subtrees {
		switch {
		case i == 0:
			r, err := st.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size()))
			require.NoError(t, err)
			roots[i] = subtreepkg.Node{Hash: *r}
		case i == len(subtrees)-1 && st.Length() < targetLength:
			r, err := st.RootHashPadded(targetHeight)
			require.NoError(t, err)
			roots[i] = subtreepkg.Node{Hash: *r}
		default:
			roots[i] = subtreepkg.Node{Hash: *st.RootHash()}
		}
	}

	if len(roots) == 1 {
		return &roots[0].Hash
	}

	store, err := subtreepkg.BuildMerkleTreeStoreFromBytes(roots)
	require.NoError(t, err)

	root := (*store)[len(*store)-1]

	return &root
}

// newPlaceholderSubtree builds a subtree whose first node is the coinbase placeholder (matching how
// the first subtree of a block is stored), followed by the supplied transaction hashes.
func newPlaceholderSubtree(t *testing.T, txHashes ...*chainhash.Hash) *subtreepkg.Subtree {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(len(txHashes) + 1)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode()) // adds CoinbasePlaceholder at index 0

	for _, h := range txHashes {
		require.NoError(t, st.AddNode(*h, 1, 1))
	}

	return st
}

func newRegularSubtree(t *testing.T, txHashes ...*chainhash.Hash) *subtreepkg.Subtree {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(len(txHashes))
	require.NoError(t, err)

	for _, h := range txHashes {
		require.NoError(t, st.AddNode(*h, 1, 1))
	}

	return st
}

// buildMock wires up a MockMerkleProofConstructor from a set of subtrees, the coinbase tx and the
// subtree index of the transaction being proven. The header merkle root is the canonical one.
func buildMock(t *testing.T, subtrees []*subtreepkg.Subtree, coinbaseTx *bt.Tx, subtreeIdx int) *MockMerkleProofConstructor {
	t.Helper()

	merkleRoot := canonicalMerkleRoot(t, subtrees, coinbaseTx)

	subtreeHashes := make([]*chainhash.Hash, len(subtrees))
	subtreeMap := make(map[string]*subtreepkg.Subtree, len(subtrees))

	for i, st := range subtrees {
		h := st.RootHash() // stored subtree identity = root WITH the placeholder
		subtreeHashes[i] = h
		subtreeMap[h.String()] = st
	}

	prevBlock, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevBlock,
		HashMerkleRoot: merkleRoot,
		Timestamp:      1,
		Nonce:          1,
	}

	block := &model.Block{
		Header:     blockHeader,
		CoinbaseTx: coinbaseTx,
		Subtrees:   subtreeHashes,
	}

	return &MockMerkleProofConstructor{
		txMeta: &TxMetaData{
			BlockIDs:     []uint32{1},
			BlockHeights: []uint32{100},
			SubtreeIdxs:  []int{subtreeIdx},
		},
		block:       block,
		blockHeader: blockHeader,
		subtrees:    subtreeMap,
	}
}

// containsHash reports whether the proof contains the given hash at any level.
func proofContainsHash(proof *MerkleProof, h chainhash.Hash) bool {
	for _, p := range proof.SubtreeProof {
		if p.IsEqual(&h) {
			return true
		}
	}

	for _, p := range proof.BlockProof {
		if p.IsEqual(&h) {
			return true
		}
	}

	return false
}

// TestConstructMerkleProofCoinbasePlaceholder is the regression test for discussion #989:
// the first subtree of a block stores a coinbase placeholder at index 0, and proofs must replace
// it with the real coinbase txid so the reconstructed root matches the block header merkle root.
func TestConstructMerkleProofCoinbasePlaceholder(t *testing.T) {
	coinbaseTx, err := bt.NewTxFromString(testCoinbaseHex)
	require.NoError(t, err)

	coinbaseHash := *coinbaseTx.TxIDChainHash()
	placeholder := subtreepkg.CoinbasePlaceholderHashValue

	tx1, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
	tx2, _ := chainhash.NewHashFromStr("2222222222222222222222222222222222222222222222222222222222222222")
	tx3, _ := chainhash.NewHashFromStr("3333333333333333333333333333333333333333333333333333333333333333")

	t.Run("single subtree - tx#1 sibling is coinbase, not placeholder", func(t *testing.T) {
		// subtree 0: [placeholder, tx1, tx2, tx3]
		st0 := newPlaceholderSubtree(t, tx1, tx2, tx3)
		mock := buildMock(t, []*subtreepkg.Subtree{st0}, coinbaseTx, 0)

		proof, err := ConstructMerkleProof(tx1, mock)
		require.NoError(t, err)
		require.Equal(t, 1, proof.TxIndexInSubtree)

		// The level-0 sibling of tx#1 is the coinbase node. It MUST be the real coinbase txid.
		require.Len(t, proof.SubtreeProof, 2)
		require.Equal(t, coinbaseHash, proof.SubtreeProof[0],
			"tx#1's coinbase sibling must be the real coinbase txid")
		require.False(t, proofContainsHash(proof, placeholder),
			"proof must not contain the coinbase placeholder")

		valid, _, err := VerifyMerkleProof(proof)
		require.NoError(t, err)
		require.True(t, valid, "reconstructed merkle root must match the block header merkle root")
	})

	t.Run("single subtree - tx#2 (deeper) verifies, no placeholder", func(t *testing.T) {
		st0 := newPlaceholderSubtree(t, tx1, tx2, tx3)
		mock := buildMock(t, []*subtreepkg.Subtree{st0}, coinbaseTx, 0)

		proof, err := ConstructMerkleProof(tx2, mock)
		require.NoError(t, err)
		require.False(t, proofContainsHash(proof, placeholder))

		valid, _, err := VerifyMerkleProof(proof)
		require.NoError(t, err)
		require.True(t, valid)
	})

	t.Run("coinbase tx proof", func(t *testing.T) {
		st0 := newPlaceholderSubtree(t, tx1, tx2, tx3)
		mock := buildMock(t, []*subtreepkg.Subtree{st0}, coinbaseTx, 0)

		proof, err := ConstructMerkleProof(&coinbaseHash, mock)
		require.NoError(t, err)
		require.Equal(t, 0, proof.TxIndexInSubtree)

		valid, _, err := VerifyMerkleProofForCoinbase(proof)
		require.NoError(t, err)
		require.True(t, valid)
	})

	t.Run("multi-subtree complete - tx in subtree 1 verifies", func(t *testing.T) {
		// subtree 0: [placeholder, tx1] ; subtree 1: [tx2, tx3] (both complete, length 2)
		st0 := newPlaceholderSubtree(t, tx1)
		st1 := newRegularSubtree(t, tx2, tx3)
		mock := buildMock(t, []*subtreepkg.Subtree{st0, st1}, coinbaseTx, 1)

		proof, err := ConstructMerkleProof(tx2, mock)
		require.NoError(t, err)
		require.Equal(t, 1, proof.SubtreeIndex)
		require.False(t, proofContainsHash(proof, placeholder),
			"block-level sibling for subtree 0 must use the coinbase-replaced root")

		valid, _, err := VerifyMerkleProof(proof)
		require.NoError(t, err)
		require.True(t, valid)
	})
}

// TestConstructMerkleProofFinalSubtreePadding covers the block-level overhaul: a multi-subtree block
// whose final subtree is incomplete must lift (pad) that subtree's root to the first subtree's height
// exactly as model.Block.CheckMerkleRoot does.
func TestConstructMerkleProofFinalSubtreePadding(t *testing.T) {
	coinbaseTx, err := bt.NewTxFromString(testCoinbaseHex)
	require.NoError(t, err)

	tx := make([]*chainhash.Hash, 8)
	for i := range tx {
		h := chainhash.DoubleHashH([]byte{byte(i + 1)})
		tx[i] = &h
	}

	// subtree 0: [placeholder, tx0, tx1, tx2] (length 4, complete, target)
	// subtree 1: [tx3, tx4, tx5, tx6]          (length 4, complete)
	// subtree 2: [tx7]                          (length 1, INCOMPLETE -> padded to height 2)
	st0 := newPlaceholderSubtree(t, tx[0], tx[1], tx[2])
	st1 := newRegularSubtree(t, tx[3], tx[4], tx[5], tx[6])
	st2 := newRegularSubtree(t, tx[7])

	require.Equal(t, 4, st0.Length())
	require.True(t, st2.Length() < st0.Length(), "final subtree must be incomplete for this test")

	subtrees := []*subtreepkg.Subtree{st0, st1, st2}

	t.Run("tx NOT in incomplete final subtree", func(t *testing.T) {
		mock := buildMock(t, subtrees, coinbaseTx, 1)

		proof, err := ConstructMerkleProof(tx[4], mock)
		require.NoError(t, err)

		valid, _, err := VerifyMerkleProof(proof)
		require.NoError(t, err)
		require.True(t, valid, "padded final-subtree leaf must be used as a block-level sibling")
	})

	t.Run("tx IN incomplete final subtree", func(t *testing.T) {
		mock := buildMock(t, subtrees, coinbaseTx, 2)

		proof, err := ConstructMerkleProof(tx[7], mock)
		require.NoError(t, err)

		valid, _, err := VerifyMerkleProof(proof)
		require.NoError(t, err)
		require.True(t, valid, "proof for a tx in the incomplete final subtree must include padding levels")
	})

	t.Run("padding height matches CheckMerkleRoot expectation", func(t *testing.T) {
		// Sanity check on the height math used by the implementation/test helper.
		actualHeight := bits.Len(uint(st2.Length() - 1))
		require.Equal(t, 0, actualHeight)
		require.Equal(t, 2, st0.Height)
	})
}

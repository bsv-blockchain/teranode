package subtree

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetMerkleProofForCoinbase(t *testing.T) {
	hash1, _ := chainhash.NewHashFromStr("97af9ad3583e2f83fc1e44e475e3a3ee31ec032449cc88b491479ef7d187c115")
	hash2, _ := chainhash.NewHashFromStr("7ce05dda56bc523048186c0f0474eb21c92fe35de6d014bd016834637a3ed08d")
	hash3, _ := chainhash.NewHashFromStr("3070fb937289e24720c827cbc24f3fce5c361cd7e174392a700a9f42051609e0")
	hash4, _ := chainhash.NewHashFromStr("d3cde0ab7142cc99acb31c5b5e1e941faed1c5cf5f8b63ed663972845d663487")

	hash5, _ := chainhash.NewHashFromStr("87af9ad3583e2f83fc1e44e475e3a3ee31ec032449cc88b491479ef7d187c115")
	hash6, _ := chainhash.NewHashFromStr("6ce05dda56bc523048186c0f0474eb21c92fe35de6d014bd016834637a3ed08d")
	hash7, _ := chainhash.NewHashFromStr("2070fb937289e24720c827cbc24f3fce5c361cd7e174392a700a9f42051609e0")
	hash8, _ := chainhash.NewHashFromStr("c3cde0ab7142cc99acb31c5b5e1e941faed1c5cf5f8b63ed663972845d663487")

	expectedRootHash := "86867b9f3e7dcb4bdf5b5cc99322122fe492bc466621f3709d4e389e7e14c16c"

	t.Run("", func(t *testing.T) {
		subtree1, err := NewTree(2)
		require.NoError(t, err)

		require.NoError(t, subtree1.AddNode(*hash1, 12, 0))
		require.NoError(t, subtree1.AddNode(*hash2, 13, 0))
		require.NoError(t, subtree1.AddNode(*hash3, 14, 0))
		require.NoError(t, subtree1.AddNode(*hash4, 15, 0))

		subtree2, err := NewTree(2)
		require.NoError(t, err)

		require.NoError(t, subtree2.AddNode(*hash5, 16, 0))
		require.NoError(t, subtree2.AddNode(*hash6, 17, 0))
		require.NoError(t, subtree2.AddNode(*hash7, 18, 0))
		require.NoError(t, subtree2.AddNode(*hash8, 19, 0))

		merkleProof, err := GetMerkleProofForCoinbase([]*Subtree{subtree1, subtree2})
		require.NoError(t, err)
		assert.Equal(t, "7ce05dda56bc523048186c0f0474eb21c92fe35de6d014bd016834637a3ed08d", merkleProof[0].String())
		assert.Equal(t, "c32db78e5f8437648888713982ea3d49628dbde0b4b48857147f793b55d26f09", merkleProof[1].String())

		topTree, err := NewTreeByLeafCount(2)
		require.NoError(t, err)

		require.NoError(t, topTree.AddNode(*subtree1.RootHash(), subtree1.Fees, subtree1.SizeInBytes))
		require.NoError(t, topTree.AddNode(*subtree2.RootHash(), subtree2.Fees, subtree1.SizeInBytes))
		assert.Equal(t, expectedRootHash, topTree.RootHash().String())
	})
}

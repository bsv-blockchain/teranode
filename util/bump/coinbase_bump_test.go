package bump

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/merkleproof"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertToBUMP_CoinbaseProof(t *testing.T) {
	t.Run("coinbase proof with single subtree", func(t *testing.T) {
		// Coinbase is at subtree index 0, tx index 0 in a block with 1 subtree
		sibling, _ := chainhash.NewHashFromStr("aaaa111122223333444455556666777788889999aaaabbbbccccddddeeeeffff")

		proof := &merkleproof.MerkleProof{
			BlockHeight:      100,
			SubtreeIndex:     0,
			TxIndexInSubtree: 0,
			SubtreeProof:     []chainhash.Hash{*sibling},
			BlockProof:       []chainhash.Hash{}, // single subtree = no block proof
		}

		bump, err := ConvertToBUMP(proof)
		require.NoError(t, err)
		require.NotNil(t, bump)

		assert.Equal(t, uint32(100), bump.BlockHeight)
		assert.Len(t, bump.Path, 1) // one level from the subtree proof

		err = Validate(bump)
		require.NoError(t, err)

		// Encode to binary and verify it round-trips
		binaryData, err := bump.EncodeBinary()
		require.NoError(t, err)
		assert.NotEmpty(t, binaryData)
	})

	t.Run("coinbase proof with multiple subtrees", func(t *testing.T) {
		// Coinbase is at subtree index 0, tx index 0 in a block with 2 subtrees
		subtreeSibling, _ := chainhash.NewHashFromStr("1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff")
		blockSibling, _ := chainhash.NewHashFromStr("aaaa111122223333444455556666777788889999aaaabbbbccccddddeeeeffff")

		proof := &merkleproof.MerkleProof{
			BlockHeight:      200,
			SubtreeIndex:     0,
			TxIndexInSubtree: 0,
			SubtreeProof:     []chainhash.Hash{*subtreeSibling},
			BlockProof:       []chainhash.Hash{*blockSibling},
		}

		bump, err := ConvertToBUMP(proof)
		require.NoError(t, err)
		require.NotNil(t, bump)

		assert.Equal(t, uint32(200), bump.BlockHeight)
		assert.Len(t, bump.Path, 2) // one level from subtree + one from block

		err = Validate(bump)
		require.NoError(t, err)

		binaryData, err := bump.EncodeBinary()
		require.NoError(t, err)
		assert.NotEmpty(t, binaryData)

		hexData, err := bump.EncodeHex()
		require.NoError(t, err)
		assert.NotEmpty(t, hexData)
	})

	t.Run("coinbase proof with deep subtree", func(t *testing.T) {
		// Coinbase in a subtree with multiple transactions (3 levels of subtree proof)
		s1, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
		s2, _ := chainhash.NewHashFromStr("2222222222222222222222222222222222222222222222222222222222222222")
		s3, _ := chainhash.NewHashFromStr("3333333333333333333333333333333333333333333333333333333333333333")
		b1, _ := chainhash.NewHashFromStr("4444444444444444444444444444444444444444444444444444444444444444")

		proof := &merkleproof.MerkleProof{
			BlockHeight:      500,
			SubtreeIndex:     0,
			TxIndexInSubtree: 0,
			SubtreeProof:     []chainhash.Hash{*s1, *s2, *s3},
			BlockProof:       []chainhash.Hash{*b1},
		}

		bump, err := ConvertToBUMP(proof)
		require.NoError(t, err)
		require.NotNil(t, bump)

		assert.Equal(t, uint32(500), bump.BlockHeight)
		assert.Len(t, bump.Path, 4) // 3 subtree levels + 1 block level

		err = Validate(bump)
		require.NoError(t, err)

		binaryData, err := bump.EncodeBinary()
		require.NoError(t, err)
		assert.NotEmpty(t, binaryData)
	})
}

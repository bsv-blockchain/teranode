package util

import (
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var (
	hash0 = hash1
	hash1 = chainhash.HashH([]byte{0x01})
	hash2 = chainhash.HashH([]byte{0x02})
	hash3 = chainhash.HashH([]byte{0x03})
	hash4 = chainhash.HashH([]byte{0x04})
)

func TestNewSubtreeMeta(t *testing.T) {
	t.Run("TestNewSubtreeMeta", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		_ = subtree.AddNode(hash1, 1, 1)
		subtreeMeta := NewSubtreeMeta(subtree)

		assert.Equal(t, 4, len(subtreeMeta.ParentTxHashes))
		assert.Equal(t, 0, len(subtreeMeta.ParentTxMeta))

		for i := 0; i < 4; i++ {
			assert.Equal(t, 0, len(subtreeMeta.ParentTxHashes[i]))
		}
	})

	t.Run("TestNewSubtreeMeta without subtree node", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		_ = subtree.AddNode(hash1, 1, 1)
		subtreeMeta := NewSubtreeMeta(subtree)

		err := subtreeMeta.SetParentTxHash(0, hash1)
		require.NoError(t, err)
		err = subtreeMeta.SetParentTxHash(1, hash2)
		require.Error(t, err)
	})

	t.Run("TestNewSubtreeMeta with 1 set", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		_ = subtree.AddNode(hash1, 1, 1)
		subtreeMeta := NewSubtreeMeta(subtree)

		subtreeMeta.SetParentTxHash(0, hash1)
		subtreeMeta.SetParentTxMeta(hash1, txmeta.Data{
			ParentTxHashes: nil,
			Fee:            123,
			SizeInBytes:    321,
		})

		assert.Equal(t, 4, len(subtreeMeta.ParentTxHashes))
		assert.Equal(t, 1, len(subtreeMeta.ParentTxMeta))

		assert.Equal(t, 1, len(subtreeMeta.ParentTxHashes[0]))
		for i := 1; i < 4; i++ {
			assert.Equal(t, 0, len(subtreeMeta.ParentTxHashes[i]))
		}
	})

	t.Run("TestNewSubtreeMeta with all set", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		_ = subtree.AddNode(hash1, 1, 1)
		_ = subtree.AddNode(hash2, 2, 2)
		_ = subtree.AddNode(hash3, 3, 3)
		_ = subtree.AddNode(hash4, 4, 4)
		subtreeMeta := NewSubtreeMeta(subtree)

		subtreeMeta.SetParentTxHash(0, hash1)
		subtreeMeta.SetParentTxHash(1, hash2)
		subtreeMeta.SetParentTxHash(2, hash3)
		subtreeMeta.SetParentTxHash(3, hash4)
		subtreeMeta.SetParentTxMeta(hash1, txmeta.Data{
			ParentTxHashes: nil,
			Fee:            123,
			SizeInBytes:    321,
		})

		assert.Equal(t, 4, len(subtreeMeta.ParentTxHashes))
		assert.Equal(t, 1, len(subtreeMeta.ParentTxMeta))

		for i := 1; i < 4; i++ {
			assert.Equal(t, 1, len(subtreeMeta.ParentTxHashes[i]))
		}
	})
}

func TestNewSubtreeMetaFromBytes(t *testing.T) {
	t.Run("TestNewSubtreeMetaFromBytes", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		_ = subtree.AddNode(hash1, 1, 1)
		subtreeMeta := NewSubtreeMeta(subtree)

		bytes, err := subtreeMeta.Serialize()
		require.NoError(t, err)
		subtreeMeta2, err := NewSubtreeMetaFromBytes(bytes)
		require.NoError(t, err)

		assert.Equal(t, subtreeMeta.RootHash, subtreeMeta2.RootHash)
		assert.Equal(t, len(subtreeMeta.ParentTxHashes), len(subtreeMeta2.ParentTxHashes))
		for i := 0; i < 4; i++ {
			assert.Equal(t, len(subtreeMeta.ParentTxHashes[i]), len(subtreeMeta2.ParentTxHashes[i]))
			for j := 0; j < len(subtreeMeta.ParentTxHashes[i]); j++ {
				assert.Equal(t, subtreeMeta.ParentTxHashes[i][j], subtreeMeta2.ParentTxHashes[i][j])
			}
		}
		assert.Equal(t, len(subtreeMeta.ParentTxMeta), len(subtreeMeta2.ParentTxMeta))
	})

	t.Run("TestNewSubtreeMetaFromBytes with all set", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		_ = subtree.AddNode(hash1, 1, 1)
		_ = subtree.AddNode(hash2, 2, 2)
		_ = subtree.AddNode(hash3, 3, 3)
		_ = subtree.AddNode(hash4, 4, 4)
		subtreeMeta := NewSubtreeMeta(subtree)

		err := subtreeMeta.SetParentTxHash(0, hash1)
		require.NoError(t, err)
		err = subtreeMeta.SetParentTxHash(1, hash2)
		require.NoError(t, err)
		err = subtreeMeta.SetParentTxHash(2, hash3)
		require.NoError(t, err)
		err = subtreeMeta.SetParentTxHash(3, hash4)
		require.NoError(t, err)

		subtreeMeta.SetParentTxMeta(hash1, txmeta.Data{
			ParentTxHashes: nil,
			Fee:            1,
			SizeInBytes:    1,
		})
		subtreeMeta.SetParentTxMeta(hash2, txmeta.Data{
			ParentTxHashes: nil,
			Fee:            2,
			SizeInBytes:    2,
		})
		subtreeMeta.SetParentTxMeta(hash3, txmeta.Data{
			ParentTxHashes: nil,
			Fee:            3,
			SizeInBytes:    3,
		})
		subtreeMeta.SetParentTxMeta(hash4, txmeta.Data{
			ParentTxHashes: nil,
			Fee:            4,
			SizeInBytes:    4,
		})

		bytes, err := subtreeMeta.Serialize()
		require.NoError(t, err)
		subtreeMeta2, err := NewSubtreeMetaFromBytes(bytes)
		require.NoError(t, err)

		assert.Equal(t, subtreeMeta.RootHash, subtreeMeta2.RootHash)
		assert.Equal(t, len(subtreeMeta.ParentTxHashes), len(subtreeMeta2.ParentTxHashes))
		for i := 0; i < 4; i++ {
			assert.Equal(t, len(subtreeMeta.ParentTxHashes[i]), len(subtreeMeta2.ParentTxHashes[i]))
			for j := 0; j < len(subtreeMeta.ParentTxHashes[i]); j++ {
				assert.Equal(t, subtreeMeta.ParentTxHashes[i][j], subtreeMeta2.ParentTxHashes[i][j])
			}
		}
		assert.Equal(t, len(subtreeMeta.ParentTxMeta), len(subtreeMeta2.ParentTxMeta))
		for k, v := range subtreeMeta.ParentTxMeta {
			assert.Equal(t, v.Fee, subtreeMeta2.ParentTxMeta[k].Fee)
			assert.Equal(t, v.SizeInBytes, subtreeMeta2.ParentTxMeta[k].SizeInBytes)
			for i := 0; i < len(v.ParentTxHashes); i++ {
				assert.Equal(t, v.ParentTxHashes[i], subtreeMeta2.ParentTxMeta[k].ParentTxHashes[i])
			}
		}
	})
}

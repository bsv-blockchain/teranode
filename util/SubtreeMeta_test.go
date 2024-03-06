package util

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	hash0 = hash1
	hash1 = chainhash.HashH([]byte{0x01})
	hash2 = chainhash.HashH([]byte{0x02})
	hash3 = chainhash.HashH([]byte{0x03})
	hash4 = chainhash.HashH([]byte{0x04})

	txMeta1 = &txmeta.Data{
		ParentTxHashes: nil,
		Fee:            1,
		SizeInBytes:    1,
	}
	txMeta2 = &txmeta.Data{
		ParentTxHashes: nil,
		Fee:            2,
		SizeInBytes:    2,
	}
	txMeta3 = &txmeta.Data{
		ParentTxHashes: nil,
		Fee:            3,
		SizeInBytes:    3,
	}
	txMeta4 = &txmeta.Data{
		ParentTxHashes: nil,
		Fee:            4,
		SizeInBytes:    4,
	}
)

func TestNewSubtreeMeta(t *testing.T) {
	t.Run("TestNewSubtreeMeta", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		subtreeMeta := NewSubtreeMeta(subtree)

		assert.Equal(t, 4, len(subtreeMeta.TxMeta))

	})

	t.Run("TestNewSubtreeMeta with 1 set", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		subtreeMeta := NewSubtreeMeta(subtree)

		subtreeMeta.AddNode(hash1, txMeta1)

		assert.Equal(t, 4, len(subtreeMeta.TxMeta))
		assert.Equal(t, txMeta1, &subtreeMeta.TxMeta[0])
	})

	t.Run("TestNewSubtreeMeta with all set", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		subtreeMeta := NewSubtreeMeta(subtree)

		subtreeMeta.AddNode(hash1, txMeta1)
		subtreeMeta.AddNode(hash2, txMeta2)
		subtreeMeta.AddNode(hash3, txMeta3)
		subtreeMeta.AddNode(hash4, txMeta4)

		assert.Equal(t, 4, len(subtreeMeta.TxMeta))

		assert.Equal(t, txMeta1, &subtreeMeta.TxMeta[0])
		assert.Equal(t, txMeta2, &subtreeMeta.TxMeta[1])
		assert.Equal(t, txMeta3, &subtreeMeta.TxMeta[2])
		assert.Equal(t, txMeta4, &subtreeMeta.TxMeta[3])
	})
}

func TestNewSubtreeMetaFromBytes(t *testing.T) {
	t.Run("TestNewSubtreeMetaFromBytes empty", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		subtreeMeta := NewSubtreeMeta(subtree)

		_, err := subtreeMeta.Serialize()
		require.Error(t, err)
	})

	t.Run("TestNewSubtreeMetaFromBytes", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		subtreeMeta := NewSubtreeMeta(subtree)

		err := subtreeMeta.AddNode(hash1, txMeta1)
		require.NoError(t, err)

		bytes, err := subtreeMeta.Serialize()
		require.NoError(t, err)
		subtreeMeta2, err := NewSubtreeMetaFromBytes(bytes)
		require.NoError(t, err)

		assert.Equal(t, subtreeMeta.RootHash, subtreeMeta2.RootHash)
		assert.Equal(t, len(subtreeMeta.TxMeta), len(subtreeMeta2.TxMeta))
		for k, v := range subtreeMeta.TxMeta {
			assert.Equal(t, v.Fee, subtreeMeta2.TxMeta[k].Fee)
			assert.Equal(t, v.SizeInBytes, subtreeMeta2.TxMeta[k].SizeInBytes)
			for i := 0; i < len(v.ParentTxHashes); i++ {
				assert.Equal(t, v.ParentTxHashes[i], subtreeMeta2.TxMeta[k].ParentTxHashes[i])
			}
		}
	})

	t.Run("TestNewSubtreeMetaFromBytes with all set", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		subtreeMeta := NewSubtreeMeta(subtree)

		err := subtreeMeta.AddNode(hash1, txMeta1)
		require.NoError(t, err)
		err = subtreeMeta.AddNode(hash2, txMeta2)
		require.NoError(t, err)
		err = subtreeMeta.AddNode(hash3, txMeta3)
		require.NoError(t, err)
		err = subtreeMeta.AddNode(hash4, txMeta4)
		require.NoError(t, err)

		bytes, err := subtreeMeta.Serialize()
		require.NoError(t, err)
		subtreeMeta2, err := NewSubtreeMetaFromBytes(bytes)
		require.NoError(t, err)

		assert.Equal(t, subtreeMeta.RootHash, subtreeMeta2.RootHash)
		assert.Equal(t, len(subtreeMeta.TxMeta), len(subtreeMeta2.TxMeta))
		for k, v := range subtreeMeta.TxMeta {
			assert.Equal(t, v.Fee, subtreeMeta2.TxMeta[k].Fee)
			assert.Equal(t, v.SizeInBytes, subtreeMeta2.TxMeta[k].SizeInBytes)
			for i := 0; i < len(v.ParentTxHashes); i++ {
				assert.Equal(t, v.ParentTxHashes[i], subtreeMeta2.TxMeta[k].ParentTxHashes[i])
			}
		}
	})
}

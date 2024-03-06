package util

import (
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewSubtreeMeta(t *testing.T) {
	t.Run("TestNewSubtreeMeta", func(t *testing.T) {
		subtreeMeta := NewSubtreeMeta(chainhash.Hash{}, 4)

		assert.Equal(t, 4, len(subtreeMeta.ParentTxHashes))
		assert.Equal(t, 0, len(subtreeMeta.ParentTxMeta))

		for i := 0; i < 4; i++ {
			assert.Equal(t, 0, len(subtreeMeta.ParentTxHashes[i]))
		}
	})

	t.Run("TestNewSubtreeMeta", func(t *testing.T) {
		subtreeMeta := NewSubtreeMeta(chainhash.Hash{}, 4)

		subtreeMeta.SetParentTxMeta(chainhash.Hash{}, txmeta.Data{
			ParentTxHashes: nil,
			Fee:            123,
			SizeInBytes:    321,
		})

		assert.Equal(t, 4, len(subtreeMeta.ParentTxHashes))
		assert.Equal(t, 1, len(subtreeMeta.ParentTxMeta))

		for i := 0; i < 4; i++ {
			assert.Equal(t, 0, len(subtreeMeta.ParentTxHashes[i]))
		}
	})
}

package subtree

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSubtreeMeta(t *testing.T) {
	tx1 := tx.Clone()
	tx1.Version = 1

	tx2 := tx.Clone()
	tx2.Version = 2

	tx3 := tx.Clone()
	tx3.Version = 3

	tx4 := tx.Clone()
	tx4.Version = 4

	t.Run("TestNewSubtreeMeta", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		_ = subtree.AddNode(*tx1.TxIDChainHash(), 1, 1)
		subtreeMeta := NewSubtreeMeta(subtree)

		assert.Equal(t, 4, len(subtreeMeta.TxInpoints))

		for i := 0; i < 4; i++ {
			assert.Nil(t, subtreeMeta.TxInpoints[i].ParentTxHashes)
		}
	})

	t.Run("TestNewSubtreeMeta without subtree node", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		_ = subtree.AddNode(*tx1.TxIDChainHash(), 1, 1)
		subtreeMeta := NewSubtreeMeta(subtree)

		err := subtreeMeta.SetTxInpointsFromTx(tx1)
		require.NoError(t, err)

		err = subtreeMeta.SetTxInpointsFromTx(tx2)
		require.Error(t, err)
	})

	t.Run("TestNewSubtreeMeta with 1 set", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		_ = subtree.AddNode(*tx1.TxIDChainHash(), 1, 1)
		subtreeMeta := NewSubtreeMeta(subtree)

		err := subtreeMeta.SetTxInpointsFromTx(tx1)
		require.NoError(t, err)

		assert.Equal(t, 4, len(subtreeMeta.TxInpoints))

		assert.Equal(t, 1, len(subtreeMeta.TxInpoints[0].GetParentTxHashes()))

		for i := 1; i < 4; i++ {
			assert.Nil(t, subtreeMeta.TxInpoints[i].ParentTxHashes)
		}
	})

	t.Run("TestNewSubtreeMeta with all set", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 1))
		require.NoError(t, subtree.AddNode(*tx2.TxIDChainHash(), 2, 2))
		require.NoError(t, subtree.AddNode(*tx3.TxIDChainHash(), 3, 3))
		require.NoError(t, subtree.AddNode(*tx4.TxIDChainHash(), 4, 4))

		subtreeMeta := NewSubtreeMeta(subtree)

		_ = subtreeMeta.SetTxInpointsFromTx(tx1)
		_ = subtreeMeta.SetTxInpointsFromTx(tx2)
		_ = subtreeMeta.SetTxInpointsFromTx(tx3)
		_ = subtreeMeta.SetTxInpointsFromTx(tx4)

		assert.Equal(t, 4, len(subtreeMeta.TxInpoints))

		for i := 1; i < 4; i++ {
			assert.Equal(t, 1, len(subtreeMeta.TxInpoints[i].GetParentTxHashes()))
		}
	})
}

func TestNewSubtreeMetaFromBytes(t *testing.T) {
	tx1 := tx.Clone()
	tx1.Version = 1

	tx2 := tx.Clone()
	tx2.Version = 2

	tx3 := tx.Clone()
	tx3.Version = 3

	tx4 := tx.Clone()
	tx4.Version = 4

	t.Run("TestNewSubtreeMetaFromBytes", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 1))

		subtreeMeta := NewSubtreeMeta(subtree)
		require.NoError(t, subtreeMeta.SetTxInpointsFromTx(tx1))

		b, err := subtreeMeta.Serialize()
		require.NoError(t, err)

		subtreeMeta2, err := NewSubtreeMetaFromBytes(subtree, b)
		require.NoError(t, err)

		assert.Equal(t, subtreeMeta.rootHash, subtreeMeta2.rootHash)
		assert.Equal(t, len(subtreeMeta.TxInpoints), len(subtreeMeta2.TxInpoints))

		for i := 0; i < 4; i++ {
			if subtreeMeta.TxInpoints[i].ParentTxHashes == nil {
				assert.Nil(t, subtreeMeta2.TxInpoints[i].ParentTxHashes)
				continue
			}

			assert.Equal(t, len(subtreeMeta.TxInpoints[i].GetParentTxHashes()), len(subtreeMeta2.TxInpoints[i].GetParentTxHashes()))

			for j := 0; j < len(subtreeMeta.TxInpoints[i].GetParentTxHashes()); j++ {
				assert.Equal(t, subtreeMeta.TxInpoints[i].GetParentTxHashes()[j], subtreeMeta2.TxInpoints[i].GetParentTxHashes()[j])
			}
		}
	})

	t.Run("TestNewSubtreeMetaFromReader", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		_ = subtree.AddNode(*tx1.TxIDChainHash(), 1, 1)
		subtreeMeta := NewSubtreeMeta(subtree)
		_ = subtreeMeta.SetTxInpointsFromTx(tx1)

		b, err := subtreeMeta.Serialize()
		require.NoError(t, err)

		buf := bytes.NewReader(b)

		subtreeMeta2, err := NewSubtreeMetaFromReader(subtree, buf)
		require.NoError(t, err)

		assert.Equal(t, subtreeMeta.rootHash, subtreeMeta2.rootHash)
		assert.Equal(t, len(subtreeMeta.TxInpoints), len(subtreeMeta2.TxInpoints))

		for i := 0; i < 4; i++ {
			if subtreeMeta.TxInpoints[i].ParentTxHashes == nil {
				assert.Nil(t, subtreeMeta2.TxInpoints[i].ParentTxHashes)
				continue
			}

			assert.Equal(t, len(subtreeMeta.TxInpoints[i].GetParentTxHashes()), len(subtreeMeta2.TxInpoints[i].GetParentTxHashes()))

			for j := 0; j < len(subtreeMeta.TxInpoints[i].GetParentTxHashes()); j++ {
				assert.Equal(t, subtreeMeta.TxInpoints[i].GetParentTxHashes()[j], subtreeMeta2.TxInpoints[i].GetParentTxHashes()[j])
			}
		}
	})

	t.Run("TestNewSubtreeMetaFromReader with large cap", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(32 * 1024)
		_ = subtree.AddNode(*tx1.TxIDChainHash(), 1, 1)

		b, err := subtree.Serialize()
		require.NoError(t, err)

		subtree2, err := NewSubtreeFromBytes(b)
		require.NoError(t, err)

		subtreeMeta := NewSubtreeMeta(subtree2)
		_ = subtreeMeta.SetTxInpointsFromTx(tx1)

		b, err = subtreeMeta.Serialize()
		require.NoError(t, err)

		buf := bytes.NewReader(b)

		subtreeMeta2, err := NewSubtreeMetaFromReader(subtree2, buf)
		require.NoError(t, err)

		assert.Equal(t, subtreeMeta.rootHash, subtreeMeta2.rootHash)
		assert.Equal(t, len(subtreeMeta.TxInpoints), len(subtreeMeta2.TxInpoints))

		assert.Equal(t, subtree2.Size(), cap(subtreeMeta.TxInpoints))
	})

	t.Run("TestNewSubtreeMetaFromBytes with all set", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 1))
		require.NoError(t, subtree.AddNode(*tx2.TxIDChainHash(), 2, 2))
		require.NoError(t, subtree.AddNode(*tx3.TxIDChainHash(), 3, 3))
		require.NoError(t, subtree.AddNode(*tx4.TxIDChainHash(), 4, 4))

		subtreeMeta := NewSubtreeMeta(subtree)

		require.NoError(t, subtreeMeta.SetTxInpointsFromTx(tx1))
		require.NoError(t, subtreeMeta.SetTxInpointsFromTx(tx2))
		require.NoError(t, subtreeMeta.SetTxInpointsFromTx(tx3))
		require.NoError(t, subtreeMeta.SetTxInpointsFromTx(tx4))

		b, err := subtreeMeta.Serialize()
		require.NoError(t, err)
		subtreeMeta2, err := NewSubtreeMetaFromBytes(subtree, b)
		require.NoError(t, err)

		assert.Equal(t, subtreeMeta.rootHash, subtreeMeta2.rootHash)
		assert.Equal(t, len(subtreeMeta.TxInpoints), len(subtreeMeta2.TxInpoints))

		for i := 0; i < 4; i++ {
			assert.Equal(t, len(subtreeMeta.TxInpoints[i].GetParentTxHashes()), len(subtreeMeta2.TxInpoints[i].GetParentTxHashes()))

			for j := 0; j < len(subtreeMeta.TxInpoints[i].GetParentTxHashes()); j++ {
				assert.Equal(t, subtreeMeta.TxInpoints[i].GetParentTxHashes()[j], subtreeMeta2.TxInpoints[i].GetParentTxHashes()[j])
			}
		}
	})
}

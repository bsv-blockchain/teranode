package subtree

import (
	"bytes"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
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

		allParentTxHashes, err := subtreeMeta.GetParentTxHashes(0)
		require.NoError(t, err)
		assert.Equal(t, 1, len(allParentTxHashes))

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
		_, subtree, subtreeMeta := initSubtreeMeta(t)

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

func TestNewSubtreeMetaGetParentTxHashes(t *testing.T) {
	txs, _, subtreeMeta := initSubtreeMeta(t)

	t.Run("TestGetParentTxHashes", func(t *testing.T) {
		for i := 0; i < 4; i++ {
			allParentTxHashes, err := subtreeMeta.GetParentTxHashes(i)
			require.NoError(t, err)

			assert.Equal(t, 1, len(allParentTxHashes))
			assert.Equal(t, *txs[i].Inputs[0].PreviousTxIDChainHash(), allParentTxHashes[0])
		}
	})

	t.Run("TestGetParentTxHashes with out of range index", func(t *testing.T) {
		allParentTxHashes, err := subtreeMeta.GetParentTxHashes(5)
		require.Error(t, err)
		assert.Nil(t, allParentTxHashes)
	})
}

func TestSubtreeMetaGetTxInpoints(t *testing.T) {
	txs, _, subtreeMeta := initSubtreeMeta(t)

	t.Run("empty subtree", func(t *testing.T) {
		subtree, _ := NewTreeByLeafCount(4)
		subtreeMeta := NewSubtreeMeta(subtree)

		inpoints, err := subtreeMeta.GetTxInpoints(0)
		require.NoError(t, err)

		assert.Equal(t, 0, len(inpoints))
	})

	t.Run("out of range index", func(t *testing.T) {
		inpoints, err := subtreeMeta.GetTxInpoints(5)
		require.Error(t, err)

		assert.Nil(t, inpoints)
	})

	t.Run("TestGetTxInpoints", func(t *testing.T) {
		for i := 0; i < 4; i++ {
			inpoints, err := subtreeMeta.GetTxInpoints(0)
			require.NoError(t, err)

			assert.Equal(t, 1, len(inpoints))
			assert.Equal(t, *txs[i].Inputs[0].PreviousTxIDChainHash(), inpoints[0].Hash)
			assert.Equal(t, txs[i].Inputs[0].PreviousTxOutIndex, inpoints[0].Index)
		}
	})
}

func TestSubtreeMetaSetTxInpoints(t *testing.T) {
	t.Run("TestSetTxInpointsFromTx", func(t *testing.T) {
		txs, _, subtreeMeta := initSubtreeMeta(t)

		for i := 0; i < 4; i++ {
			err := subtreeMeta.SetTxInpointsFromTx(txs[i])
			require.NoError(t, err)

			inpoints, err := subtreeMeta.GetTxInpoints(i)
			require.NoError(t, err)

			assert.Equal(t, 1, len(inpoints))
			assert.Equal(t, *txs[i].Inputs[0].PreviousTxIDChainHash(), inpoints[0].Hash)
			assert.Equal(t, txs[i].Inputs[0].PreviousTxOutIndex, inpoints[0].Index)
		}
	})

	t.Run("TestSetTxInpoints", func(t *testing.T) {
		txs, _, subtreeMeta := initSubtreeMeta(t)

		// Test setting inpoints for a subtree node that does not exist
		err := subtreeMeta.SetTxInpoints(2, TxInpoints{
			ParentTxHashes: []chainhash.Hash{*txs[0].Inputs[0].PreviousTxIDChainHash()},
			Idxs:           [][]uint32{{1, 2, 3}},
			nrInpoints:     3,
		})
		require.NoError(t, err)

		inpoints, err := subtreeMeta.GetTxInpoints(2)
		require.NoError(t, err)

		assert.Equal(t, 3, len(inpoints))
		assert.Equal(t, *txs[0].Inputs[0].PreviousTxIDChainHash(), inpoints[0].Hash)
		assert.Equal(t, *txs[0].Inputs[0].PreviousTxIDChainHash(), inpoints[0].Hash)
		assert.Equal(t, *txs[0].Inputs[0].PreviousTxIDChainHash(), inpoints[0].Hash)
		assert.Equal(t, uint32(1), inpoints[0].Index)
		assert.Equal(t, uint32(2), inpoints[1].Index)
		assert.Equal(t, uint32(3), inpoints[2].Index)

		// Test setting inpoints for a subtree node that does not exist
		err = subtreeMeta.SetTxInpoints(5, TxInpoints{
			ParentTxHashes: []chainhash.Hash{*txs[0].Inputs[0].PreviousTxIDChainHash()},
			Idxs:           [][]uint32{{1, 2, 3}},
			nrInpoints:     3,
		})
		require.Error(t, err)
		assert.Equal(t, "index out of range", err.Error())
	})
}

func initSubtreeMeta(t *testing.T) ([]*bt.Tx, *Subtree, *SubtreeMeta) {
	tx1 := tx.Clone()
	tx1.Version = 1

	tx2 := tx.Clone()
	tx2.Version = 2

	tx3 := tx.Clone()
	tx3.Version = 3

	tx4 := tx.Clone()
	tx4.Version = 4

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

	return []*bt.Tx{tx1, tx2, tx3, tx4}, subtree, subtreeMeta
}

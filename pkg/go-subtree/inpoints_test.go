package subtree

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTxInpoints(t *testing.T) {
	t.Run("TestTxInpoints", func(t *testing.T) {
		p, err := NewTxInpointsFromTx(tx)
		require.NoError(t, err)

		assert.Equal(t, 1, len(p.ParentTxHashes))
		assert.Equal(t, 1, len(p.Idxs[0]))
	})

	t.Run("serialize", func(t *testing.T) {
		p, err := NewTxInpointsFromTx(tx)
		require.NoError(t, err)

		b, err := p.Serialize()
		require.NoError(t, err)
		assert.Equal(t, 44, len(b))

		p2, err := NewTxInpointsFromBytes(b)
		require.NoError(t, err)

		assert.Equal(t, 1, len(p2.ParentTxHashes))
		assert.Equal(t, 1, len(p2.Idxs[0]))

		assert.Equal(t, p.ParentTxHashes[0], p2.ParentTxHashes[0])
		assert.Equal(t, p.Idxs[0][0], p2.Idxs[0][0])
	})

	t.Run("serialize with error", func(t *testing.T) {
		p := NewTxInpoints()
		p.ParentTxHashes = []chainhash.Hash{chainhash.HashH([]byte("test"))}
		p.Idxs = [][]uint32{}

		_, err := p.Serialize()
		require.Error(t, err)
	})

	t.Run("from inputs", func(t *testing.T) {
		p, err := NewTxInpointsFromTx(tx)
		require.NoError(t, err)

		p2, err := NewTxInpointsFromInputs(tx.Inputs)
		require.NoError(t, err)

		// make sure they are the same
		assert.Equal(t, len(p.ParentTxHashes), len(p2.ParentTxHashes))
		assert.Equal(t, len(p.Idxs), len(p2.Idxs))
		assert.Equal(t, p.ParentTxHashes[0], p2.ParentTxHashes[0])
		assert.Equal(t, p.Idxs[0][0], p2.Idxs[0][0])
	})
}

func TestGetTxInpoints(t *testing.T) {
	p, err := NewTxInpointsFromTx(tx)
	require.NoError(t, err)

	// Test getting inpoints
	inpoints := p.GetTxInpoints()
	assert.Equal(t, 1, len(inpoints))
	assert.Equal(t, uint32(5), inpoints[0].Index)
	assert.Equal(t, *tx.Inputs[0].PreviousTxIDChainHash(), inpoints[0].Hash)
}

func TestGetParentTxHashAtIndex(t *testing.T) {
	t.Run("TestGetParentTxHashAtIndex", func(t *testing.T) {
		p, err := NewTxInpointsFromTx(tx)
		require.NoError(t, err)

		// Test getting parent tx hash at index
		hash, err := p.GetParentTxHashAtIndex(0)
		require.NoError(t, err)

		assert.Equal(t, *tx.Inputs[0].PreviousTxIDChainHash(), hash)
	})

	t.Run("out of range", func(t *testing.T) {
		p, err := NewTxInpointsFromTx(tx)
		require.NoError(t, err)

		// Test getting parent tx hash at index
		hash, err := p.GetParentTxHashAtIndex(1)
		require.Error(t, err)

		assert.Equal(t, chainhash.Hash{}, hash)
	})
}

func TestGetParentVoutsAtIndex(t *testing.T) {
	t.Run("TestGetParentVoutsAtIndex", func(t *testing.T) {
		p, err := NewTxInpointsFromTx(tx)
		require.NoError(t, err)

		// Test getting parent vouts at index
		vouts, err := p.GetParentVoutsAtIndex(0)
		require.NoError(t, err)

		assert.Equal(t, 1, len(vouts))
		assert.Equal(t, uint32(5), vouts[0])
	})

	t.Run("out of range", func(t *testing.T) {
		p, err := NewTxInpointsFromTx(tx)
		require.NoError(t, err)

		// Test getting parent vouts at index
		vouts, err := p.GetParentVoutsAtIndex(1)
		require.Error(t, err)

		assert.Nil(t, vouts)
	})
}

func BenchmarkNewTxInpoints(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := NewTxInpointsFromTx(tx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

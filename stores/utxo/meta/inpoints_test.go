package meta

import (
	"testing"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	tx, _ = bt.NewTxFromString("010000000000000000ef0158ef6d539bf88c850103fa127a92775af48dba580c36bbde4dc6d8b9da83256d050000006a47304402200ca69c5672d0e0471cd4ff1f9993f16103fc29b98f71e1a9760c828b22cae61c0220705e14aa6f3149130c3a6aa8387c51e4c80c6ae52297b2dabfd68423d717be4541210286dbe9cd647f83a4a6b29d2a2d3227a897a4904dc31769502cb013cbe5044dddffffffff8c2f6002000000001976a914308254c746057d189221c36418ba93337de33bc988ac03002d3101000000001976a91498cde576de501ceb5bb1962c6e49a4d1af17730788ac80969800000000001976a914eb7772212c334c0bdccee75c0369aa675fc21d2088ac706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac00000000")
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

func BenchmarkNewTxInpoints(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := NewTxInpointsFromTx(tx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

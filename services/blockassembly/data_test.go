package blockassembly

import (
	"testing"

	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestData_Bytes(t *testing.T) {
	t.Run("should return the correct bytes", func(t *testing.T) {
		d := &Data{
			TxIDChainHash: *hash0,
			Fee:           1,
			Size:          2,
		}

		b := d.Bytes()

		dd, err := NewFromBytes(b)
		require.NoError(t, err)

		assert.Equal(t, d.TxIDChainHash, dd.TxIDChainHash)
		assert.Equal(t, d.Fee, dd.Fee)
		assert.Equal(t, d.Size, dd.Size)
	})

	t.Run("should return the correct bytes, with parents", func(t *testing.T) {
		d := &Data{
			TxIDChainHash: *hash0,
			Fee:           1,
			Size:          2,
			TxInpoints: meta.TxInpoints{
				ParentTxHashes: []chainhash.Hash{
					*hash1,
					*hash2,
				},
				Idxs: [][]uint32{
					{1, 2},
					{3, 4},
				},
			},
		}

		b := d.Bytes()

		dd, err := NewFromBytes(b)
		require.NoError(t, err)

		assert.Equal(t, d.TxIDChainHash, dd.TxIDChainHash)
		assert.Equal(t, d.Fee, dd.Fee)
		assert.Equal(t, d.Size, dd.Size)
		assert.Equal(t, len(d.TxInpoints.ParentTxHashes), len(dd.TxInpoints.ParentTxHashes))
		assert.Equal(t, d.TxInpoints.ParentTxHashes[0], dd.TxInpoints.ParentTxHashes[0])
		assert.Equal(t, d.TxInpoints.ParentTxHashes[1], dd.TxInpoints.ParentTxHashes[1])
	})
}

func BenchmarkNewFromBytes(b *testing.B) {
	d := Data{
		TxIDChainHash: *hash0,
		Fee:           1,
		Size:          2,
	}

	bytes := d.Bytes()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = NewFromBytes(bytes)
	}
}

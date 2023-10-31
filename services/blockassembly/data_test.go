package blockassembly

import (
	"testing"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestData_Bytes(t *testing.T) {
	t.Run("should return the correct bytes", func(t *testing.T) {
		d := &Data{
			TxIDChainHash: hash0,
			Fee:           1,
			Size:          2,
			LockTime:      3,
			UtxoHashes: []*chainhash.Hash{
				utxo1,
				utxo2,
				utxo3,
				utxo4,
			},
		}

		b := d.Bytes()

		dd, err := NewFromBytes(b)
		require.NoError(t, err)

		assert.Equal(t, *d.TxIDChainHash, *dd.TxIDChainHash)
		assert.Equal(t, d.Fee, dd.Fee)
		assert.Equal(t, d.Size, dd.Size)
		assert.Equal(t, d.LockTime, dd.LockTime)
		assert.Equal(t, len(d.UtxoHashes), len(dd.UtxoHashes))
		for i := 0; i < len(d.UtxoHashes); i++ {
			assert.Equal(t, *d.UtxoHashes[i], *dd.UtxoHashes[i])
		}
	})
}

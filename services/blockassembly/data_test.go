package blockassembly

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestData_Bytes(t *testing.T) {
	t.Run("should return the correct bytes", func(t *testing.T) {
		d := &Data{
			TxIDChainHash: hash0,
			Fee:           1,
			Size:          2,
		}

		b := d.Bytes()

		dd, err := NewFromBytes(b)
		require.NoError(t, err)

		assert.Equal(t, *d.TxIDChainHash, *dd.TxIDChainHash)
		assert.Equal(t, d.Fee, dd.Fee)
		assert.Equal(t, d.Size, dd.Size)
	})
}

func BenchmarkNewFromBytes(b *testing.B) {
	d := Data{
		TxIDChainHash: hash0,
		Fee:           1,
		Size:          2,
	}

	bytes := d.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NewFromBytes(bytes)
	}
}

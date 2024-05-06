package txmeta

import (
	"testing"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	hash3, _ = chainhash.NewHashFromStr("300000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	hash4, _ = chainhash.NewHashFromStr("400000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd")
)

func Test_NewDataFromBytes(t *testing.T) {
	t.Run("test simple", func(t *testing.T) {
		data := &Data{
			Fee:         100,
			SizeInBytes: 200,
			ParentTxHashes: []chainhash.Hash{
				*hash3,
				*hash4,
			},
			BlockIDs: []uint32{
				123,
				321,
			},
			Tx:         &bt.Tx{},
			IsCoinbase: true,
		}

		b := data.Bytes()

		d, err := NewDataFromBytes(b)
		require.NoError(t, err)

		assert.Equal(t, data.Fee, d.Fee)
		assert.Equal(t, data.SizeInBytes, d.SizeInBytes)
		assert.True(t, d.IsCoinbase)

		require.Len(t, data.ParentTxHashes, 2)
		require.Equal(t, len(data.ParentTxHashes), len(d.ParentTxHashes))
		assert.Equal(t, data.ParentTxHashes[0].String(), d.ParentTxHashes[0].String())
		assert.Equal(t, data.ParentTxHashes[1].String(), d.ParentTxHashes[1].String())

		require.Len(t, data.BlockIDs, 2)
		require.Equal(t, len(data.BlockIDs), len(d.BlockIDs))
		assert.Equal(t, data.BlockIDs[0], d.BlockIDs[0])
		assert.Equal(t, data.BlockIDs[1], d.BlockIDs[1])
	})
}

func Benchmark_NewMetaDataFromBytes(b *testing.B) {
	data := &Data{
		Fee:         100,
		SizeInBytes: 200,
		ParentTxHashes: []chainhash.Hash{
			*hash3,
			*hash4,
		},
		BlockIDs: []uint32{
			5,
			6,
		},
		Tx: &bt.Tx{},
	}

	dataBytes := data.Bytes()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewMetaDataFromBytes(&dataBytes, data)
	}
}

func Benchmark_Bytes(b *testing.B) {
	data := &Data{
		Fee:         100,
		SizeInBytes: 200,
		ParentTxHashes: []chainhash.Hash{
			*hash3,
			*hash4,
		},
		BlockIDs: []uint32{
			5,
			6,
		},
		Tx: &bt.Tx{},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = data.Bytes()
	}
}

func Benchmark_MetaBytes(b *testing.B) {
	data := &Data{
		Fee:         100,
		SizeInBytes: 200,
		ParentTxHashes: []chainhash.Hash{
			*hash3,
			*hash4,
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = data.MetaBytes()
	}
}

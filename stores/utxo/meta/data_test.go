package meta

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	hash3, _ = chainhash.NewHashFromStr("0ab59604a1c249d0cbfe18f01fe423df3035840f9a609395ccd177d2b217cae6")
	hash4, _ = chainhash.NewHashFromStr("08c3d6e8388415d8f6190a40c0acb9328b41a89a5854468e62c2bbd1dc740460")

	// testInpointsHash3Hash4 is the deduplicated TxInpoints with vouts {1, 2}
	// for hash3 and {3, 4} for hash4 — built once and shared across tests.
	testInpointsHash3Hash4 = mustInpoints(hash3, []uint32{1, 2}, hash4, []uint32{3, 4})
)

// mustInpoints builds a TxInpoints from alternating (parentHash, vouts) pairs
// using fake *bt.Input values. Replaces the pre-packed-layout
// `subtree.TxInpoints{ParentTxHashes: ..., Idxs: ...}` struct-literal pattern.
func mustInpoints(args ...interface{}) subtree.TxInpoints {
	inputs := make([]*bt.Input, 0)

	for i := 0; i < len(args); i += 2 {
		parent := args[i].(*chainhash.Hash)
		vouts := args[i+1].([]uint32)

		for _, v := range vouts {
			in := &bt.Input{PreviousTxOutIndex: v}
			if err := in.PreviousTxIDAdd(parent); err != nil {
				panic(err)
			}

			inputs = append(inputs, in)
		}
	}

	ti, err := subtree.NewTxInpointsFromInputs(inputs)
	if err != nil {
		panic(err)
	}

	return ti
}

// inpointsVouts returns the vouts for the i-th parent of a TxInpoints. Used
// in tests that previously read `txInpoints.Idxs[i]` directly.
func inpointsVouts(ti subtree.TxInpoints, i int) []uint32 {
	v, err := ti.GetParentVoutsAtIndex(i)
	if err != nil {
		panic(err)
	}

	return v
}

func Test_NewDataFromBytes(t *testing.T) {
	t.Run("test simple", func(t *testing.T) {
		data := &Data{
			Fee:         100,
			SizeInBytes: 200,
			TxInpoints: testInpointsHash3Hash4,
			BlockIDs: []uint32{
				123,
				321,
			},
			Tx:         &bt.Tx{},
			IsCoinbase: true,
		}

		b, err := data.Bytes()
		require.NoError(t, err)

		d, err := NewDataFromBytes(b)
		require.NoError(t, err)

		assert.Equal(t, data.Fee, d.Fee)
		assert.Equal(t, data.SizeInBytes, d.SizeInBytes)
		assert.True(t, d.IsCoinbase)

		require.Len(t, data.TxInpoints.ParentTxHashes, 2)
		require.Equal(t, len(data.TxInpoints.ParentTxHashes), len(d.TxInpoints.ParentTxHashes))
		assert.Equal(t, data.TxInpoints.ParentTxHashes[0].String(), d.TxInpoints.ParentTxHashes[0].String())
		assert.Equal(t, data.TxInpoints.ParentTxHashes[1].String(), d.TxInpoints.ParentTxHashes[1].String())
		assert.Equal(t, inpointsVouts(data.TxInpoints, 0), inpointsVouts(d.TxInpoints, 0))
		assert.Equal(t, inpointsVouts(data.TxInpoints, 1), inpointsVouts(d.TxInpoints, 1))

		require.Len(t, data.BlockIDs, 2)
		require.Equal(t, len(data.BlockIDs), len(d.BlockIDs))
		assert.Equal(t, data.BlockIDs[0], d.BlockIDs[0])
		assert.Equal(t, data.BlockIDs[1], d.BlockIDs[1])
	})

	t.Run("test simple MetaBytes", func(t *testing.T) {
		data := &Data{
			Fee:         100,
			SizeInBytes: 200,
			TxInpoints: testInpointsHash3Hash4,
			BlockIDs: []uint32{
				123,
				321,
			},
			Tx:         &bt.Tx{},
			IsCoinbase: true,
		}

		b, err := data.MetaBytes()
		require.NoError(t, err)

		d := &Data{}
		err = NewMetaDataFromBytes(b, d)
		require.NoError(t, err)

		assert.Equal(t, data.Fee, d.Fee)
		assert.Equal(t, data.SizeInBytes, d.SizeInBytes)
		assert.True(t, d.IsCoinbase)

		require.Len(t, data.TxInpoints.ParentTxHashes, 2)
		require.Equal(t, len(data.TxInpoints.ParentTxHashes), len(d.TxInpoints.ParentTxHashes))
		assert.Equal(t, data.TxInpoints.ParentTxHashes[0].String(), d.TxInpoints.ParentTxHashes[0].String())
		assert.Equal(t, data.TxInpoints.ParentTxHashes[1].String(), d.TxInpoints.ParentTxHashes[1].String())
		assert.Equal(t, inpointsVouts(data.TxInpoints, 0), inpointsVouts(d.TxInpoints, 0))
		assert.Equal(t, inpointsVouts(data.TxInpoints, 1), inpointsVouts(d.TxInpoints, 1))
	})

	t.Run("test frozen conflicting", func(t *testing.T) {
		data := &Data{
			Fee:         100,
			SizeInBytes: 200,
			TxInpoints: testInpointsHash3Hash4,
			BlockIDs: []uint32{
				123,
				321,
			},
			Tx:          &bt.Tx{},
			IsCoinbase:  true,
			Frozen:      true,
			Conflicting: true,
		}

		b, err := data.Bytes()
		require.NoError(t, err)

		d, err := NewDataFromBytes(b)
		require.NoError(t, err)

		assert.Equal(t, data.Fee, d.Fee)
		assert.Equal(t, data.SizeInBytes, d.SizeInBytes)
		assert.True(t, d.IsCoinbase)
		assert.True(t, d.Frozen)
		assert.True(t, d.Conflicting)

		require.Len(t, data.TxInpoints.ParentTxHashes, 2)
		require.Equal(t, len(data.TxInpoints.ParentTxHashes), len(d.TxInpoints.ParentTxHashes))
		assert.Equal(t, data.TxInpoints.ParentTxHashes[0].String(), d.TxInpoints.ParentTxHashes[0].String())
		assert.Equal(t, data.TxInpoints.ParentTxHashes[1].String(), d.TxInpoints.ParentTxHashes[1].String())

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
		TxInpoints: testInpointsHash3Hash4,
		BlockIDs: []uint32{
			5,
			6,
		},
		Tx: &bt.Tx{},
	}

	dataBytes, _ := data.Bytes()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = NewMetaDataFromBytes(dataBytes, data)
	}
}

func Benchmark_Bytes(b *testing.B) {
	data := &Data{
		Fee:         100,
		SizeInBytes: 200,
		TxInpoints: testInpointsHash3Hash4,
		BlockIDs: []uint32{
			5,
			6,
		},
		Tx: &bt.Tx{},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = data.Bytes()
	}
}

func Benchmark_MetaBytes(b *testing.B) {
	data := &Data{
		Fee:         100,
		SizeInBytes: 200,
		TxInpoints: testInpointsHash3Hash4,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = data.MetaBytes()
	}
}

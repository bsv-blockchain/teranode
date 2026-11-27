package packedsql

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

func TestSpendingDataRoundTrip(t *testing.T) {
	txid, err := chainhash.NewHashFromStr("6a5147bb37a13d7f8347d409bcbfb450d3fbd0dd93af9d81858c9ec4ed338e07")
	require.NoError(t, err)

	sd := &spend.SpendingData{TxID: txid, Vin: 7}
	b := packSpendingData(sd)
	require.Len(t, b, slotSpendSize)

	got := unpackSpendingData(b)
	require.Equal(t, sd.TxID, got.TxID)
	require.Equal(t, sd.Vin, got.Vin)
}

func TestUnpackSpendingDataZeroIsNil(t *testing.T) {
	require.Nil(t, unpackSpendingData(make([]byte, slotSpendSize)))
	require.Nil(t, unpackSpendingData(nil))
}

func TestOffsetBlobRoundTrip(t *testing.T) {
	items := [][]byte{{0x01}, {}, {0x02, 0x03, 0x04}}
	blob := packOffsetBlob(items)
	require.Equal(t, 3, offsetBlobCount(blob))

	for i, want := range items {
		got, err := offsetBlobItem(blob, i)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}

	_, err := offsetBlobItem(blob, 3)
	require.Error(t, err)

	start, length, err := offsetBlobRange(blob, 2)
	require.NoError(t, err)
	require.Equal(t, uint32(3), length)

	item, err := offsetBlobItem(blob, 2)
	require.NoError(t, err)
	require.Equal(t, blob[start-1:start-1+length], item)
}

func TestBlockRefs(t *testing.T) {
	b := packBlockRefs([]utxo.MinedBlockInfo{{BlockID: 5, BlockHeight: 100, SubtreeIdx: 2}})
	b = appendBlockRef(b, utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 101, SubtreeIdx: 0})
	b = appendBlockRef(b, utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 101, SubtreeIdx: 0})

	ids, heights, subtrees := unpackBlockRefs(b)
	require.Equal(t, []uint32{5, 9}, ids)
	require.Equal(t, []uint32{100, 101}, heights)
	require.Equal(t, []int{2, 0}, subtrees)

	b = removeBlockRef(b, 5)
	ids, _, _ = unpackBlockRefs(b)
	require.Equal(t, []uint32{9}, ids)
}

func TestPageMath(t *testing.T) {
	require.Equal(t, uint32(0), pageOfVout(63, 64))
	require.Equal(t, uint32(1), pageOfVout(64, 64))
	require.Equal(t, uint32(63), slotOfVout(63, 64))
	require.Equal(t, uint32(0), slotOfVout(64, 64))
}

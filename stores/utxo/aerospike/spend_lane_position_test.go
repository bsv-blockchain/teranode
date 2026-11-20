package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

func laneTestSpendItem(lanePos uint64) *batchSpend {
	return &batchSpend{
		spend: &utxo.Spend{
			TxID:         &chainhash.Hash{0x01},
			Vout:         2,
			UTXOHash:     &chainhash.Hash{0xaa},
			SpendingData: spendpkg.NewSpendingData(&chainhash.Hash{0x09}, 1),
		},
		lanePos: lanePos,
	}
}

// With lane positions off a spend item is exactly what it was: no lanePos key, so neither
// teranode.lua nor the C module writes anything new. With a position the key carries it, signed bit
// for bit, as every lane integer bin does.
func TestSpendItemCarriesTheLanePositionOnlyWhenItHasOne(t *testing.T) {
	s := &Store{utxoBatchSize: 128}

	off := s.createSpendMapValue(0, laneTestSpendItem(lane.None))
	_, has := off[spendItemLanePos]
	require.False(t, has, "lane positions off: no lanePos key")
	require.Len(t, off, 5, "the five keys a spend item always had")

	position := lane.Pack(200, 41_000)
	on := s.createSpendMapValue(0, laneTestSpendItem(position))
	require.Equal(t, lane.ToSigned(position), on[spendItemLanePos])
	require.Less(t, on[spendItemLanePos].(int64), int64(0), "lane 200 travels as a negative integer")

	// The native path encodes the same map, so the key reaches the C module too.
	payload, err := encodeNativeOpPayload(subOpSpendMulti, []any{[]aerospike.MapValue{on}, false, false, uint32(1), uint32(288)})
	require.NoError(t, err)
	require.NotEmpty(t, payload)
}

// A parent list holding any position that is not valid is left out whole, so a record never gets a
// zero in its parentLanePos bin.
func TestValidParentLanes(t *testing.T) {
	good := []uint64{lane.Pack(1, 1), lane.Pack(2, 2)}

	require.Equal(t, good, validParentLanes(good))
	require.Nil(t, validParentLanes(nil))
	require.Nil(t, validParentLanes([]uint64{lane.Pack(1, 1), lane.None}))

	bins, err := withParentLanes([][]*aerospike.Bin{{}}, good)
	require.NoError(t, err)
	require.Len(t, bins[0], 1)
	require.Equal(t, fields.ParentLanePositions.String(), bins[0][0].Name)

	bins, err = withParentLanes([][]*aerospike.Bin{{}}, nil)
	require.NoError(t, err)
	require.Empty(t, bins[0], "no positions, no bin")
}

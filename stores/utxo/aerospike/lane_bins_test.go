package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

func TestLaneBinNamesFitAerospike(t *testing.T) {
	// Aerospike refuses a bin name longer than 15 bytes.
	for _, f := range []fields.FieldName{fields.LanePosition, fields.SpendingLanePositions, fields.ParentLanePositions} {
		require.LessOrEqual(t, len(f.String()), 15, f)
	}
}

func TestParentLanePositionsBinRoundTrip(t *testing.T) {
	parents := []uint64{lane.Pack(200, 5), lane.Pack(0, 1), lane.Pack(255, lane.MaxPosition)}

	bin, err := parentLanePositionsBin(parents)
	require.NoError(t, err)
	require.Equal(t, fields.ParentLanePositions.String(), bin.Name)

	list, ok := bin.Value.GetObject().([]interface{})
	require.True(t, ok)
	require.Less(t, list[0].(int64), int64(0), "lane 200 is stored negative, bit for bit")

	got, err := decodeParentLanePositions(list, 3)
	require.NoError(t, err)
	require.Equal(t, parents, got)

	// The client hands integers back as int.
	asInt := []interface{}{int(lane.ToSigned(parents[0])), int(lane.ToSigned(parents[1])), int(lane.ToSigned(parents[2]))}
	got, err = decodeParentLanePositions(asInt, 3)
	require.NoError(t, err)
	require.Equal(t, parents, got)

	_, err = parentLanePositionsBin([]uint64{lane.Pack(3, 0)})
	require.Error(t, err)
}

func TestDecodeParentLanePositionsRejects(t *testing.T) {
	for name, v := range map[string]interface{}{
		"not a list":    int64(5),
		"short":         []interface{}{int64(lane.Pack(1, 1))},
		"zero":          []interface{}{int64(lane.Pack(1, 1)), int64(0)},
		"nil entry":     []interface{}{int64(lane.Pack(1, 1)), nil},
		"lane only":     []interface{}{int64(lane.Pack(1, 1)), lane.ToSigned(lane.Pack(9, 0))},
		"wrong type":    []interface{}{int64(lane.Pack(1, 1)), "x"},
		"absent bin":    nil,
		"too long list": []interface{}{int64(1), int64(2), int64(3)},
	} {
		_, err := decodeParentLanePositions(v, 2)
		require.Error(t, err, name)
	}
}

func TestSpendingLanePositionsBin(t *testing.T) {
	bin := spendingLanePositionsBin(3)
	require.Equal(t, fields.SpendingLanePositions.String(), bin.Name)

	list, ok := bin.Value.GetObject().([]interface{})
	require.True(t, ok)

	got, err := decodeSpendingLanePositions(list, 3)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 0, 0}, got, "every slot unspent")

	list[1] = lane.ToSigned(lane.Pack(254, 77))
	list[2] = int64(0)
	got, err = decodeSpendingLanePositions(list, 3)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, lane.Pack(254, 77), 0}, got)

	for name, v := range map[string]interface{}{
		"wrong length": []interface{}{nil, nil},
		"lane only":    []interface{}{nil, lane.ToSigned(lane.Pack(9, 0)), nil},
		"wrong type":   []interface{}{nil, []byte{1}, nil},
		"not a list":   "spent",
	} {
		_, err := decodeSpendingLanePositions(v, 3)
		require.Error(t, err, name)
	}
}

package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
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

	// With no parents expected, only the list type rule refuses a bin of another type.
	_, err := decodeParentLanePositions(int64(5), 0)
	require.Error(t, err)

	got, err := decodeParentLanePositions([]interface{}{}, 0)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestSpendingLanePositionsBytes(t *testing.T) {
	got, err := decodeSpendingLanePositions(nil, 3)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 0, 0}, got, "no bin: no output has a position")

	// Grown only to output 1, the last one spent with a position; lane 254 sets the top bit.
	b := appendSlotLanePositions(nil, []uint64{0, lane.Pack(254, 77)})
	require.Len(t, b, 16)

	got, err = decodeSpendingLanePositions(b, 3)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, lane.Pack(254, 77), 0}, got, "an output past the end has none")

	for name, v := range map[string]interface{}{
		"not whole":  []byte{1, 2, 3},
		"too long":   appendSlotLanePositions(nil, []uint64{0, 0, 0, 0}),
		"lane only":  appendSlotLanePositions(nil, []uint64{lane.Pack(9, 0)}),
		"wrong type": []interface{}{int64(1)},
		"not bytes":  "spent",
	} {
		_, err := decodeSpendingLanePositions(v, 3)
		require.Error(t, err, name)
	}
}

// A spend counts as carrying its position only on a route that records it, on a server the probe
// passed: the expression route, one output per record, records none.
func TestCountLaneSpendsOnlyOnARouteThatRecords(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Aerospike.EnableSpendFilterExpressions = true

	for _, tc := range []struct {
		name          string
		utxoBatchSize int
		probe         bool
		carried       float64
	}{
		{name: "lua route, probe passed", utxoBatchSize: 128, probe: true, carried: 2},
		{name: "lua route, probe failed", utxoBatchSize: 128, probe: false, carried: 0},
		{name: "expression route, probe passed", utxoBatchSize: 1, probe: true, carried: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Store{settings: tSettings, utxoBatchSize: tc.utxoBatchSize}
			s.laneSlotWrites.Store(tc.probe)

			yes, no := laneSpendCount(t, "yes"), laneSpendCount(t, "no")
			s.countLaneSpends(2, true)
			require.InDelta(t, yes+tc.carried, laneSpendCount(t, "yes"), 0)
			require.InDelta(t, no+2-tc.carried, laneSpendCount(t, "no"), 0)
		})
	}
}

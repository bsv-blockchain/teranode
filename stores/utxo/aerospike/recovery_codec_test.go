package aerospike

import (
	"math"
	"testing"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/stretchr/testify/require"
)

func TestRecoveryCodecLossless(t *testing.T) {
	bins := as.BinMap{"geo": as.GeoJSONValue(`{"type":"Point","coordinates":[0,0]}`), "hll": as.HLLValue{1, 2, 3}, "integer": int(9007199254740993), "bytes": []byte{0, 255}, "list": []interface{}{nil, true, "text", math.Float64frombits(0x8000000000000000)}, "map": map[interface{}]interface{}{int(2): "two", "2": int(2)}}
	encoded, err := encodeRecoveryBins(bins)
	require.NoError(t, err)
	decoded, err := decodeRecoveryBins(encoded)
	require.NoError(t, err)
	require.Equal(t, bins, decoded)
	again, err := encodeRecoveryBins(decoded)
	require.NoError(t, err)
	require.Equal(t, encoded, again)
	_, err = decodeRecoveryBins(append(encoded, byte('!')))
	require.Error(t, err)
	_, err = encodeRecoveryBins(as.BinMap{"unsupported": complex(1, 2)})
	require.Error(t, err)
}

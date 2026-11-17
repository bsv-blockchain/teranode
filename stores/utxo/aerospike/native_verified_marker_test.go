package aerospike

import (
	"testing"

	aerospike "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	aeropruner "github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// TestEncodeVerifiedMarkerPayload pins the sub-op 15 wire shape the server
// decodes: [15, [[key, txid, offsets]...]], with the key the reversed hex the
// spend path looks up, the txid in internal byte order, and the offsets as a
// list of integers, empty rather than nil for a child with none.
func TestEncodeVerifiedMarkerPayload(t *testing.T) {
	childA := chainhash.HashH([]byte("a"))
	childB := chainhash.HashH([]byte("b"))

	payload, err := encodeVerifiedMarkerPayload([]aeropruner.MarkerChild{
		{Hash: &childA, Offsets: []uint32{0, 7}},
		{Hash: &childB},
	})
	require.NoError(t, err)

	var decoded []any
	require.NoError(t, msgpack.Unmarshal(payload, &decoded))
	require.Len(t, decoded, 2)
	require.EqualValues(t, subOpAddVerifiedDeleted, decoded[0])

	args, ok := decoded[1].([]any)
	require.True(t, ok)
	require.Len(t, args, 1)

	entries, ok := args[0].([]any)
	require.True(t, ok)
	require.Len(t, entries, 2)

	first, ok := entries[0].([]any)
	require.True(t, ok)
	require.Equal(t, childA.String(), first[0])
	require.Equal(t, childA.CloneBytes(), first[1])

	offsets, ok := first[2].([]any)
	require.True(t, ok)
	require.Len(t, offsets, 2)
	require.EqualValues(t, 0, offsets[0])
	require.EqualValues(t, 7, offsets[1])

	second, ok := entries[1].([]any)
	require.True(t, ok)
	require.Equal(t, []any{}, second[2], "a child with no offsets sends an empty list")
}

// TestPrunerVerifiedMarkerRecord: the builder answers native only when native
// ops are on and the sub-op 15 probe passed, and its record keeps the policy of
// the filtered write it replaces.
func TestPrunerVerifiedMarkerRecord(t *testing.T) {
	s := &Store{logger: ulogger.TestLogger{}}
	child := chainhash.HashH([]byte("child"))
	children := []aeropruner.MarkerChild{{Hash: &child, Offsets: []uint32{1}}}

	key, err := aerospike.NewKey("test", "utxo", []byte("parent"))
	require.NoError(t, err)

	_, ok := s.prunerVerifiedMarkerRecord(key, children)
	require.False(t, ok, "native ops off")

	s.useNativeVerifiedMarker.Store(true)
	_, ok = s.prunerVerifiedMarkerRecord(key, children)
	require.False(t, ok, "native ops off, even with the sub-op 15 probe passed")

	s.useNativeTeranodeOps.Store(true)
	s.useNativeVerifiedMarker.Store(false)
	_, ok = s.prunerVerifiedMarkerRecord(key, children)
	require.False(t, ok, "native ops on but the server does not serve sub-op 15")

	s.useNativeVerifiedMarker.Store(true)
	record, ok := s.prunerVerifiedMarkerRecord(key, children)
	require.True(t, ok)

	batchWrite, isWrite := record.(*aerospike.BatchWrite)
	require.True(t, isWrite)
	require.Equal(t, aerospike.UPDATE_ONLY, batchWrite.Policy.RecordExistsAction, "a parent that is gone is never re-created")
	require.Equal(t, uint32(aerospike.TTLDontUpdate), batchWrite.Policy.Expiration, "a marker never changes the parent's lifetime")
	require.Equal(t, aerospike.COMMIT_ALL, batchWrite.Policy.CommitLevel)
	require.Nil(t, batchWrite.Policy.FilterExpression)
	require.Len(t, batchWrite.Ops, 1)
}

// TestObservePrunerNativeOpError: a PARAMETER_ERROR on a sub-op 15 write turns
// the native marker write off and leaves the other native ops on, and nothing
// else does.
func TestObservePrunerNativeOpError(t *testing.T) {
	s := &Store{logger: ulogger.TestLogger{}}
	s.useNativeTeranodeOps.Store(true)
	s.useNativeVerifiedMarker.Store(true)

	for _, code := range []types.ResultCode{types.KEY_NOT_FOUND_ERROR, types.TIMEOUT, types.FILTERED_OUT} {
		s.observePrunerNativeOpError(&aerospike.AerospikeError{ResultCode: code})
		require.True(t, s.useNativeVerifiedMarkerWrite(), "%v leaves sub-op 15 on", code)
	}

	s.observePrunerNativeOpError(nil)
	require.True(t, s.useNativeVerifiedMarkerWrite())

	s.observePrunerNativeOpError(&aerospike.AerospikeError{ResultCode: types.PARAMETER_ERROR})
	require.False(t, s.useNativeVerifiedMarkerWrite(), "PARAMETER_ERROR turns sub-op 15 off")
	require.True(t, s.useNativeTeranodeOps.Load(), "every other native op stays on")
}

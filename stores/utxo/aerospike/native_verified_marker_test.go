package aerospike

import (
	"fmt"
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

	payload := encodeVerifiedMarkerPayload([]aeropruner.MarkerChild{
		{Hash: &childA, Offsets: []uint32{0, 7}},
		{Hash: &childB},
	})

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

// TestVerifiedMarkerAnswer covers how the sub-op 15 probe reads each answer,
// which a stock server never reaches because it refuses the opcode outright.
func TestVerifiedMarkerAnswer(t *testing.T) {
	s := &Store{logger: ulogger.TestLogger{}}

	answer := func(resp interface{}) *aerospike.BatchRecord {
		return &aerospike.BatchRecord{ResultCode: types.OK, Record: &aerospike.Record{Bins: aerospike.BinMap{nativeOpResultBin: resp}}}
	}

	code, err := s.verifiedMarkerAnswer(answer(map[interface{}]interface{}{"status": "OK"}))
	require.NoError(t, err)
	require.Empty(t, code, "OK")

	code, err = s.verifiedMarkerAnswer(answer(map[interface{}]interface{}{"status": "ERROR", "errorCode": "NOT_SPENDER", "message": "no"}))
	require.NoError(t, err)
	require.Equal(t, LuaErrorCodeNotSpender, code)

	_, err = s.verifiedMarkerAnswer(answer(map[interface{}]interface{}{"status": "ERROR"}))
	require.Error(t, err, "ERROR without a code")

	_, err = s.verifiedMarkerAnswer(answer(map[interface{}]interface{}{"status": "MAYBE"}))
	require.Error(t, err, "unknown status")

	_, err = s.verifiedMarkerAnswer(answer("OK"))
	require.Error(t, err, "a response that is not a map")

	_, err = s.verifiedMarkerAnswer(&aerospike.BatchRecord{ResultCode: types.OK})
	require.Error(t, err, "no response record")

	wireErr := &aerospike.AerospikeError{ResultCode: types.PARAMETER_ERROR}
	_, err = s.verifiedMarkerAnswer(&aerospike.BatchRecord{Err: wireErr, ResultCode: types.PARAMETER_ERROR})
	require.True(t, hasResultCode(err, types.PARAMETER_ERROR), "the wire error reaches the probe")
}

// TestMarkerSet: the probe reads the marker the way the spend path does, a
// true value under the child's key.
func TestMarkerSet(t *testing.T) {
	require.True(t, markerSet(map[interface{}]interface{}{"a": true}, "a"))
	require.True(t, markerSet(map[string]interface{}{"a": true}, "a"))
	require.False(t, markerSet(map[interface{}]interface{}{"a": false}, "a"), "false is no marker")
	require.False(t, markerSet(map[interface{}]interface{}{"b": true}, "a"))
	require.False(t, markerSet(nil, "a"))
	require.False(t, markerSet("invalid-map", "a"))
}

// TestObservePrunerNativeOpError:a PARAMETER_ERROR on a sub-op 15 write turns
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

// reflectionVerifiedMarkerPayload is the reflection-based encoding the hand-written
// encoder replaced: the entries as []any, each offset boxed as int64, through msgpack.
func reflectionVerifiedMarkerPayload(t testing.TB, children []aeropruner.MarkerChild) []byte {
	t.Helper()

	entries := make([]any, len(children))

	for i, child := range children {
		offsets := make([]any, len(child.Offsets))
		for j, offset := range child.Offsets {
			offsets[j] = int64(offset)
		}

		entries[i] = []any{child.Hash.String(), child.Hash.CloneBytes(), offsets}
	}

	payload, err := encodeNativeOpPayload(subOpAddVerifiedDeleted, []any{entries})
	require.NoError(t, err)

	return payload
}

// TestEncodeVerifiedMarkerPayloadMatchesMsgpack pins the hand-written encoder byte for
// byte against the reflection encoding, across every array-header and integer width.
func TestEncodeVerifiedMarkerPayloadMatchesMsgpack(t *testing.T) {
	hashes := make([]chainhash.Hash, 70000)
	for i := range hashes {
		hashes[i] = chainhash.HashH([]byte{byte(i), byte(i >> 8), byte(i >> 16)})
	}

	seq := func(n int, start uint32) []uint32 {
		out := make([]uint32, n)
		for i := range out {
			out[i] = start + uint32(i)
		}

		return out
	}

	one := func(offsets []uint32) []aeropruner.MarkerChild {
		return []aeropruner.MarkerChild{{Hash: &hashes[0], Offsets: offsets}}
	}

	many := func(n int, offsets []uint32) []aeropruner.MarkerChild {
		out := make([]aeropruner.MarkerChild, n)
		for i := range out {
			out[i] = aeropruner.MarkerChild{Hash: &hashes[i], Offsets: offsets}
		}

		return out
	}

	for _, tc := range []struct {
		name     string
		children []aeropruner.MarkerChild
	}{
		{"no children", nil},
		{"a child with no offsets", one(nil)},
		{"one offset", one([]uint32{0})},
		{"offsets 127 and 128", one([]uint32{127, 128})},
		{"offsets 255 and 256", one([]uint32{255, 256})},
		{"offsets 65535 and 65536", one([]uint32{65535, 65536})},
		{"uint32 values", one([]uint32{1 << 24, 0xffffffff})},
		{"fixarray offsets limit", one(seq(15, 0))},
		{"array16 offsets", one(seq(16, 0))},
		{"array32 offsets", one(seq(65536, 0))},
		{"fixarray children limit", many(15, []uint32{3})},
		{"array16 children", many(16, []uint32{3, 200})},
		{"array32 children", many(65536, nil)},
		{"mixed", []aeropruner.MarkerChild{
			{Hash: &hashes[1], Offsets: []uint32{0, 7, 300}},
			{Hash: &hashes[2]},
			{Hash: &hashes[3], Offsets: seq(20, 120)},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, reflectionVerifiedMarkerPayload(t, tc.children), encodeVerifiedMarkerPayload(tc.children))
		})
	}
}

// BenchmarkEncodeVerifiedMarkerPayload measures the store's own sub-op 15 encoder, the one the
// pruner calls once per parent record. children is how many pruned children name that parent;
// offsets is how many of its outputs each one claims.
func BenchmarkEncodeVerifiedMarkerPayload(b *testing.B) {
	for _, shape := range []struct{ children, offsets int }{{1, 1}, {1, 8}, {4, 1}, {16, 4}} {
		b.Run(fmt.Sprintf("children=%d/offsets=%d", shape.children, shape.offsets), func(b *testing.B) {
			children := make([]aeropruner.MarkerChild, shape.children)

			for i := range children {
				h := chainhash.HashH([]byte{byte(i), 0x55})
				children[i].Hash = &h

				for j := 0; j < shape.offsets; j++ {
					children[i].Offsets = append(children[i].Offsets, uint32(i*shape.offsets+j))
				}
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if len(encodeVerifiedMarkerPayload(children)) == 0 {
					b.Fatal("empty payload")
				}
			}
		})
	}
}

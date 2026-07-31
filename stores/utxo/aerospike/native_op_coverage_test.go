package aerospike

import (
	"testing"

	aerospike "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// allSubOps is every native sub-op id. Kept in one place so a new sub-op that
// forgets its fencing/encoding coverage trips these tests.
var allSubOps = []uint8{
	subOpSpend, subOpSpendMulti, subOpUnspend, subOpSetMined, subOpFreeze,
	subOpUnfreeze, subOpReassign, subOpSetConflicting, subOpPreserveUntil,
	subOpSetLocked, subOpIncrementSpentExtraRec, subOpSetDeleteAtHeight,
	subOpAddDeletedChildren,
}

// TestUseNativeForSubOp_Fencing locks the routing gate: native only when the
// setting is on, and unspend is always fenced to the UDF path (#899).
func TestUseNativeForSubOp_Fencing(t *testing.T) {
	off := &Store{}
	on := &Store{}
	on.useNativeTeranodeOps.Store(true)

	for _, op := range allSubOps {
		require.Falsef(t, off.useNativeForSubOp(op), "sub-op %d must use UDF when native disabled", op)

		want := op != subOpUnspend // unspend is fenced to UDF even when native is on
		require.Equalf(t, want, on.useNativeForSubOp(op), "sub-op %d native routing", op)
	}
}

// TestEncodeNativeOpPayload_ArgShapes exercises encodeNativeOpPayload over the
// concrete argument shapes the production call sites pass (plain Go values, not
// aerospike.Value wrappers), asserting each round-trips as msgpack [subOp, [args]].
func TestEncodeNativeOpPayload_ArgShapes(t *testing.T) {
	hash := chainhash.Hash{0x01}

	cases := []struct {
		name  string
		subOp uint8
		args  []any
	}{
		{"setLocked_bool", subOpSetLocked, []any{true}},
		{"setMined_ints", subOpSetMined, []any{[]int{1, 2, 3}, uint32(700000), 288}},
		{"freeze_bytes", subOpFreeze, []any{hash[:]}},
		{"increment_ints", subOpIncrementSpentExtraRec, []any{5, 700000, 288}},
		{"conflicting_bool", subOpSetConflicting, []any{true}},
		{"preserve_int", subOpPreserveUntil, []any{100}},
		{"spendMulti_mapvalues", subOpSpendMulti, []any{
			[]aerospike.MapValue{{"utxoHash": hash[:], "offset": 0}}, false, false, uint32(1), 288,
		}},
		// A zero-arg sub-op must encode as [id, []] (empty list), never
		// [id, nil]: the pruner follow-up wires subOpSetDeleteAtHeight /
		// subOpAddDeletedChildren and the dispatcher expects an args list.
		{"no_args_nil_slice", subOpSetDeleteAtHeight, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := encodeNativeOpPayload(tc.subOp, tc.args)
			require.NoError(t, err)

			var decoded []any
			require.NoError(t, msgpack.Unmarshal(payload, &decoded))
			require.Len(t, decoded, 2, "payload must be [subOp, [args...]]")
			require.EqualValues(t, tc.subOp, decoded[0])

			args, ok := decoded[1].([]any)
			require.True(t, ok, "args element must be an array, got %T", decoded[1])
			require.Len(t, args, len(tc.args))
		})
	}
}

// TestTeranodeBatchRecord_Routing verifies teranodeBatchRecord returns a native
// BatchWrite (op-200) only when native is enabled and the sub-op isn't fenced,
// and falls back to a UDF BatchUDF otherwise — including carrying a caller
// FilterExpression onto the native write (the PreserveTransactions prune gate).
func TestTeranodeBatchRecord_Routing(t *testing.T) {
	key, _ := aerospike.NewKey("ns", "set", []byte{1, 2, 3})
	require.NotNil(t, key)

	udfPolicy := aerospike.NewBatchUDFPolicy()

	// Native disabled -> UDF record regardless of sub-op.
	off := &Store{logger: ulogger.TestLogger{}}
	require.IsType(t, &aerospike.BatchUDF{}, off.teranodeBatchRecord(udfPolicy, "pkg", key, subOpSetLocked, "setLocked", true))

	on := &Store{
		nativeOpBatchWritePolicy: aerospike.NewBatchWritePolicy(),
		logger:                   ulogger.TestLogger{},
	}
	on.useNativeTeranodeOps.Store(true)

	// Native enabled, non-fenced sub-op -> native BatchWrite.
	require.IsType(t, &aerospike.BatchWrite{}, on.teranodeBatchRecord(udfPolicy, "pkg", key, subOpSetLocked, "setLocked", true))

	// Unspend is fenced -> UDF even when native is enabled.
	require.IsType(t, &aerospike.BatchUDF{}, on.teranodeBatchRecord(udfPolicy, "pkg", key, subOpUnspend, "unspend", []byte{1}))

	// Caller FilterExpression is carried onto the native write (prune gate):
	// still a native record, and the per-call policy copy branch is exercised.
	filtered := aerospike.NewBatchUDFPolicy()
	filtered.FilterExpression = aerospike.ExpBinExists("deleteAtHeight")
	rec := on.teranodeBatchRecord(filtered, "pkg", key, subOpPreserveUntil, "preserveUntil", 100)
	require.IsType(t, &aerospike.BatchWrite{}, rec)
}

// TestParseLuaMapResponse_BothTransports locks the response parser against both
// the UDF path's map[interface{}]interface{} and the native msgpack transport's
// map[string]interface{}. This is the tolerance the native path relies on — a
// concrete-type assertion here would panic on the native shape.
func TestParseLuaMapResponse_BothTransports(t *testing.T) {
	s := &Store{}

	// UDF shape: map[interface{}]interface{}. blockIDs mixes the numeric Go
	// types both transports can produce (int, int64, uint32, float64) to
	// exercise luaResponseInt's type switch; a signal field is included too.
	udf := map[interface{}]interface{}{
		"status":     "OK",
		"signal":     "DELETE",
		"blockIDs":   []interface{}{int(10), int64(20), uint32(30)},
		"childCount": int64(3),
	}
	res, err := s.ParseLuaMapResponse(udf)
	require.NoError(t, err)
	require.Equal(t, LuaStatusOK, res.Status)
	require.Equal(t, LuaSignal("DELETE"), res.Signal)
	require.Equal(t, []int{10, 20, 30}, res.BlockIDs)
	require.Equal(t, 3, res.ChildCount)

	// Native shape: map[string]interface{}. The errors map is keyed by integer
	// offset (msgpack decodes numeric keys to int64, so it lands as a
	// map[interface{}]interface{} nested under the string-keyed outer map).
	native := map[string]interface{}{
		"status":    "ERROR",
		"errorCode": "TX_NOT_FOUND",
		"message":   "parent gone",
		"errors": map[interface{}]interface{}{
			int64(0): map[interface{}]interface{}{"errorCode": "SPENT", "message": "already spent"},
		},
	}
	res2, err := s.ParseLuaMapResponse(native)
	require.NoError(t, err)
	require.Equal(t, LuaStatusError, res2.Status)
	require.Equal(t, LuaErrorCodeTxNotFound, res2.ErrorCode)
	require.Equal(t, "parent gone", res2.Message)
	require.Contains(t, res2.Errors, 0)
	require.Equal(t, "already spent", res2.Errors[0].Message)

	// A non-map response is a parse error, not a panic.
	_, err = s.ParseLuaMapResponse("not a map")
	require.Error(t, err)
}

// TestLuaMapResponse_Reset confirms Reset clears fields and, critically, nils
// BlockIDs (not [:0]) so the "BlockIDs != nil iff present" pool contract holds.
func TestLuaMapResponse_Reset(t *testing.T) {
	r := &LuaMapResponse{
		Status:     LuaStatusOK,
		ErrorCode:  LuaErrorCodeTxNotFound,
		Message:    "m",
		BlockIDs:   []int{1, 2, 3},
		ChildCount: 4,
		Errors:     map[int]LuaErrorInfo{0: {Message: "x"}},
	}

	r.Reset()

	require.Equal(t, LuaStatus(""), r.Status)
	require.Equal(t, LuaErrorCode(""), r.ErrorCode)
	require.Empty(t, r.Message)
	require.Nil(t, r.BlockIDs, "BlockIDs must be nil after Reset, not an empty slice")
	require.Zero(t, r.ChildCount)
	require.Empty(t, r.Errors)
}

// TestCreateSpendError_Branches covers the error-code switch. The nil-item guard
// is covered separately by TestCreateSpendErrorHandlesNilBatchItem.
func TestCreateSpendError_Branches(t *testing.T) {
	s := &Store{}
	txID := &chainhash.Hash{0x02}
	item := &batchSpend{spend: &utxo.Spend{TxID: txID, Vout: 1, UTXOHash: &chainhash.Hash{0x03}}}

	cases := []struct {
		name string
		code LuaErrorCode
	}{
		{"invalid_spend", LuaErrorCodeInvalidSpend},
		{"frozen", LuaErrorCodeFrozen},
		{"frozen_until", LuaErrorCodeFrozenUntil},
		{"utxo_not_found", LuaErrorCodeUtxoNotFound},
		{"hash_mismatch", LuaErrorCodeUtxoHashMismatch},
		{"unknown_default", LuaErrorCode("SOMETHING_ELSE")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := s.createSpendError(LuaErrorInfo{ErrorCode: tc.code, Message: "detail"}, item, txID)
			require.Error(t, err)
		})
	}
}

// TestCreateSpendErrorHandlesNilBatchItem covers the nil-batch-item guard in
// createSpendError. Without it, the LuaErrorCodeSpent arm dereferences a nil
// txID (spend.go, "UTXO already spent but no spending data provided") and the
// process panics instead of returning a diagnosable error. Every other arm
// dereferences batchItem.spend, so the guard has to precede all of them.
func TestCreateSpendErrorHandlesNilBatchItem(t *testing.T) {
	s := &Store{}

	err := s.createSpendError(LuaErrorInfo{
		ErrorCode: LuaErrorCodeSpent,
		Message:   "spent",
	}, nil, nil)

	require.Error(t, err)
	require.Contains(t, err.Error(), "nil batch item")
}

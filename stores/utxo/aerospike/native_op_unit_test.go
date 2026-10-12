package aerospike

import (
	"fmt"
	"testing"

	aerospike "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func aeroErr(code types.ResultCode) *aerospike.AerospikeError {
	return &aerospike.AerospikeError{ResultCode: code}
}

// TestDemoteNativeOnUnsupported locks the runtime-demotion contract: only a
// PARAMETER_ERROR seen while native ops are active flips the store back to
// the UDF path, exactly once, for the rest of the process.
func TestDemoteNativeOnUnsupported(t *testing.T) {
	s := &Store{logger: ulogger.TestLogger{}}
	s.useNativeTeranodeOps.Store(true)

	s.demoteNativeOnUnsupported(nil)
	require.True(t, s.useNativeTeranodeOps.Load(), "nil error must not demote")

	s.demoteNativeOnUnsupported(aeroErr(types.KEY_NOT_FOUND_ERROR))
	require.True(t, s.useNativeTeranodeOps.Load(), "KEY_NOT_FOUND must not demote")

	// Raw and %w-wrapped errors are detected; note teranode's errors.New*
	// wrapping converts foreign errors to *errors.Error and does NOT preserve
	// the aerospike type — the demotion hooks therefore always receive the raw
	// batchRec.Err / opErr, never a teranode-wrapped one. fmt.Errorf is used
	// deliberately: it is the only stdlib way to build a chain that keeps the
	// aerospike type reachable, mirroring the client's own wrapping.
	s.demoteNativeOnUnsupported(fmt.Errorf("op failed: %w", aeroErr(types.PARAMETER_ERROR))) //nolint:forbidigo // std %w chain needed; errors.New* erases the aerospike type
	require.False(t, s.useNativeTeranodeOps.Load(), "wrapped PARAMETER_ERROR must demote")

	// Demoting again is a no-op (CAS already false).
	s.demoteNativeOnUnsupported(aeroErr(types.PARAMETER_ERROR))
	require.False(t, s.useNativeTeranodeOps.Load())

	// With native off from the start, PARAMETER_ERROR is not interpreted as
	// "native unsupported" — the UDF path can surface it for other reasons.
	off := &Store{logger: ulogger.TestLogger{}}
	off.demoteNativeOnUnsupported(aeroErr(types.PARAMETER_ERROR))
	require.False(t, off.useNativeTeranodeOps.Load())
}

func TestIsKeyNotFound(t *testing.T) {
	require.True(t, isKeyNotFound(aeroErr(types.KEY_NOT_FOUND_ERROR)))
	require.True(t, isKeyNotFound(fmt.Errorf("wrapped: %w", aeroErr(types.KEY_NOT_FOUND_ERROR)))) //nolint:forbidigo // std %w chain needed; errors.New* erases the aerospike type
	require.False(t, isKeyNotFound(nil))
	require.False(t, isKeyNotFound(aeroErr(types.PARAMETER_ERROR)))
	require.False(t, isKeyNotFound(errors.NewStorageError("plain")))
}

// TestTeranodeBatchRecordResponse covers every branch of the shared batch
// result checker used by freeze/unfreeze/reassign: nil record, KEY_NOT_FOUND
// (TxNotFound mapping + native demotion trigger), other batch errors, missing
// record/bins, unparsable response, and the happy path.
func TestTeranodeBatchRecordResponse(t *testing.T) {
	key, err := aerospike.NewKey("ns", "set", []byte{1})
	require.NoError(t, err)

	newRec := func() aerospike.BatchRecordIfc {
		return aerospike.NewBatchWrite(nil, key, aerospike.PutOp(aerospike.NewBin("x", 1)))
	}

	t.Run("nil_record", func(t *testing.T) {
		s := &Store{logger: ulogger.TestLogger{}}
		_, err := s.teranodeBatchRecordResponse("[test]", nil)
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrTxNotFound))
	})

	t.Run("key_not_found_maps_to_tx_not_found", func(t *testing.T) {
		s := &Store{logger: ulogger.TestLogger{}}
		rec := newRec()
		rec.BatchRec().Err = aeroErr(types.KEY_NOT_FOUND_ERROR)

		_, err := s.teranodeBatchRecordResponse("[test]", rec)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxNotFound))
	})

	t.Run("parameter_error_demotes_native", func(t *testing.T) {
		s := &Store{logger: ulogger.TestLogger{}}
		s.useNativeTeranodeOps.Store(true)

		rec := newRec()
		rec.BatchRec().Err = aeroErr(types.PARAMETER_ERROR)

		_, err := s.teranodeBatchRecordResponse("[test]", rec)
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrTxNotFound))
		require.False(t, s.useNativeTeranodeOps.Load(), "PARAMETER_ERROR on a batch record must demote")
	})

	t.Run("missing_record", func(t *testing.T) {
		s := &Store{logger: ulogger.TestLogger{}}
		_, err := s.teranodeBatchRecordResponse("[test]", newRec())
		require.Error(t, err)
	})

	t.Run("missing_response_bin", func(t *testing.T) {
		s := &Store{logger: ulogger.TestLogger{}}
		rec := newRec()
		rec.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{"other": 1}}

		_, err := s.teranodeBatchRecordResponse("[test]", rec)
		require.Error(t, err)
	})

	t.Run("unparsable_response", func(t *testing.T) {
		s := &Store{logger: ulogger.TestLogger{}}
		rec := newRec()
		rec.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{LuaSuccess.String(): "not a map"}}

		_, err := s.teranodeBatchRecordResponse("[test]", rec)
		require.Error(t, err)
	})

	t.Run("happy_path", func(t *testing.T) {
		s := &Store{logger: ulogger.TestLogger{}}
		rec := newRec()
		rec.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{
			LuaSuccess.String(): map[interface{}]interface{}{"status": "OK"},
		}}

		res, err := s.teranodeBatchRecordResponse("[test]", rec)
		require.NoError(t, err)
		require.Equal(t, LuaStatusOK, res.Status)
	})
}

// TestHandleBatchErrorMapsKeyNotFound locks the spend-path mapping: under the
// native path's UPDATE_ONLY policy a missing parent arrives as a per-record
// KEY_NOT_FOUND, which must complete each spend with the same TxNotFoundError
// the UDF path's TX_NOT_FOUND status produces.
func TestHandleBatchErrorMapsKeyNotFound(t *testing.T) {
	s := &Store{logger: ulogger.TestLogger{}}
	batchByKey := []aerospike.MapValue{{"idx": 0}}

	item := &batchSpend{spend: &utxo.Spend{TxID: &chainhash.Hash{0x01}}, group: completion.NewGroup(1)}
	s.handleBatchError(batchByKey, []*batchSpend{item}, 100, 1, aeroErr(types.KEY_NOT_FOUND_ERROR))
	require.True(t, errors.Is(item.spend.Err, errors.ErrTxNotFound))

	other := &batchSpend{spend: &utxo.Spend{TxID: &chainhash.Hash{0x02}}, group: completion.NewGroup(1)}
	s.handleBatchError(batchByKey, []*batchSpend{other}, 100, 1, aeroErr(types.SERVER_ERROR))
	require.Error(t, other.spend.Err)
	require.False(t, errors.Is(other.spend.Err, errors.ErrTxNotFound))
}

// TestHandleBatchRecordErrorSetMined covers the setMined per-record error
// mapping, including the nil-hash safety of the KEY_NOT_FOUND branch.
func TestHandleBatchRecordErrorSetMined(t *testing.T) {
	s := &Store{logger: ulogger.TestLogger{}}
	hash := &chainhash.Hash{0x01}
	knf := aeroErr(types.KEY_NOT_FOUND_ERROR)

	require.NoError(t, s.handleBatchRecordError(nil, knf, hash, true), "unsetMined + missing record is a no-op")

	err := s.handleBatchRecordError(nil, knf, hash, false)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))

	require.NotPanics(t, func() {
		err = s.handleBatchRecordError(nil, knf, nil, false)
	})
	require.Error(t, err)

	err = s.handleBatchRecordError(nil, aeroErr(types.SERVER_ERROR), hash, false)
	require.Error(t, err)
	require.False(t, errors.Is(err, errors.ErrTxNotFound))
}

// TestLuaResponseIntSliceRejectsByteSlice locks the blockIDs type guard: a
// msgpack bin-encoded (byte slice) blockIDs must be a parse error, never a
// byte-per-ID success.
func TestLuaResponseIntSliceRejectsByteSlice(t *testing.T) {
	_, err := luaResponseIntSlice([]byte{1, 2, 3})
	require.Error(t, err)

	ids, err := luaResponseIntSlice([]int64{1, 2})
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, ids)

	s := &Store{}
	_, err = s.ParseLuaMapResponse(map[string]interface{}{
		"status":   "OK",
		"blockIDs": []byte{10, 20},
	})
	require.Error(t, err)
}

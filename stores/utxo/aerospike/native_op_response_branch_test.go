package aerospike

import (
	"context"
	"fmt"
	"math"
	"testing"

	aerospike "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestLuaResponseIntWidths covers every numeric width the two transports can
// produce, including the overflow guards on the unsigned types.
func TestLuaResponseIntWidths(t *testing.T) {
	cases := []struct {
		in   interface{}
		want int
		ok   bool
	}{
		{int(1), 1, true},
		{int8(2), 2, true},
		{int16(3), 3, true},
		{int32(4), 4, true},
		{int64(5), 5, true},
		{uint(6), 6, true},
		{uint8(7), 7, true},
		{uint16(8), 8, true},
		{uint32(9), 9, true},
		{uint64(10), 10, true},
		{uint64(math.MaxUint64), 0, false},
		{uint(math.MaxUint), 0, false},
		{"11", 0, false},
		{nil, 0, false},
		{1.5, 0, false},
	}

	for _, tc := range cases {
		got, ok := luaResponseInt(tc.in)
		require.Equal(t, tc.ok, ok, "input %#v (%T)", tc.in, tc.in)
		if tc.ok {
			require.Equal(t, tc.want, got, "input %#v (%T)", tc.in, tc.in)
		}
	}
}

// TestProcessSingleBatchRecordPooledBranches unit-covers the setMined
// per-record result branches: nil record, batch error, missing bin,
// unparsable response, TX_NOT_FOUND with and without unsetMined, generic
// error status, and the happy path.
func TestProcessSingleBatchRecordPooledBranches(t *testing.T) {
	ctx := context.Background()
	s := &Store{logger: ulogger.TestLogger{}}
	hash := &chainhash.Hash{0x0a}

	key, err := aerospike.NewKey("ns", "set", []byte{1})
	require.NoError(t, err)

	newRec := func() aerospike.BatchRecordIfc {
		return aerospike.NewBatchWrite(nil, key, aerospike.PutOp(aerospike.NewBin("x", 1)))
	}

	res := &LuaMapResponse{}

	t.Run("nil_record", func(t *testing.T) {
		ok, _, err := s.processSingleBatchRecordPooled(ctx, nil, hash, utxo.MinedBlockInfo{}, res)
		require.False(t, ok)
		require.Error(t, err)
	})

	t.Run("batch_error_maps_key_not_found", func(t *testing.T) {
		rec := newRec()
		rec.BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}

		ok, _, err := s.processSingleBatchRecordPooled(ctx, rec, hash, utxo.MinedBlockInfo{}, res)
		require.False(t, ok)
		require.True(t, errors.Is(err, errors.ErrTxNotFound))

		// unsetMined: a missing record is a no-op — no error, and the record
		// is not counted as processed (ok=false, err=nil).
		ok, _, err = s.processSingleBatchRecordPooled(ctx, rec, hash, utxo.MinedBlockInfo{UnsetMined: true}, res)
		require.False(t, ok)
		require.NoError(t, err)
	})

	t.Run("missing_response_bin", func(t *testing.T) {
		rec := newRec()
		rec.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{"other": 1}}

		ok, _, err := s.processSingleBatchRecordPooled(ctx, rec, hash, utxo.MinedBlockInfo{}, res)
		require.False(t, ok)
		require.Error(t, err)
	})

	t.Run("unparsable_response", func(t *testing.T) {
		rec := newRec()
		rec.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{LuaSuccess.String(): 42}}

		ok, _, err := s.processSingleBatchRecordPooled(ctx, rec, hash, utxo.MinedBlockInfo{}, res)
		require.False(t, ok)
		require.Error(t, err)
	})

	t.Run("tx_not_found_status", func(t *testing.T) {
		rec := newRec()
		rec.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{
			LuaSuccess.String(): map[interface{}]interface{}{
				"status":    "ERROR",
				"errorCode": string(LuaErrorCodeTxNotFound),
			},
		}}

		ok, _, err := s.processSingleBatchRecordPooled(ctx, rec, hash, utxo.MinedBlockInfo{}, &LuaMapResponse{})
		require.False(t, ok)
		require.True(t, errors.Is(err, errors.ErrTxNotFound))

		ok, _, err = s.processSingleBatchRecordPooled(ctx, rec, hash, utxo.MinedBlockInfo{UnsetMined: true}, &LuaMapResponse{})
		require.True(t, ok)
		require.NoError(t, err)
	})

	t.Run("generic_error_status", func(t *testing.T) {
		rec := newRec()
		rec.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{
			LuaSuccess.String(): map[interface{}]interface{}{
				"status":  "ERROR",
				"message": "boom",
			},
		}}

		ok, _, err := s.processSingleBatchRecordPooled(ctx, rec, hash, utxo.MinedBlockInfo{}, &LuaMapResponse{})
		require.False(t, ok)
		require.Error(t, err)
	})

	t.Run("happy_path", func(t *testing.T) {
		rec := newRec()
		rec.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{
			LuaSuccess.String(): map[interface{}]interface{}{"status": "OK"},
		}}

		ok, out, err := s.processSingleBatchRecordPooled(ctx, rec, hash, utxo.MinedBlockInfo{}, &LuaMapResponse{})
		require.True(t, ok)
		require.NoError(t, err)
		require.Equal(t, LuaStatusOK, out.Status)
	})
}

// TestDescribeAerospikeDiagnosticsBranches covers the nil/empty/limit branches
// of the diagnostics helpers used in the hardened error messages.
func TestDescribeAerospikeDiagnosticsBranches(t *testing.T) {
	require.Equal(t, "batchRecord=<nil>", describeAerospikeBatchRecord(nil))
	require.Equal(t, "record=<nil>", describeAerospikeRecord(nil))
	require.Contains(t, describeAerospikeRecord(&aerospike.Record{}), "bins=<nil>")
	require.Equal(t, "{}", describeAerospikeBins(aerospike.BinMap{}))
	require.Equal(t, "<nil>", describeChainHash(nil))
	require.Equal(t, "<nil>", describeUTXOSpend(nil))

	key, err := aerospike.NewKey("ns", "set", []byte{1})
	require.NoError(t, err)

	rec := aerospike.NewBatchWrite(nil, key, aerospike.PutOp(aerospike.NewBin("x", 1)))
	require.Contains(t, describeAerospikeBatchRecord(rec), "resultCode=")

	// Bin-map cap: exceeding the description limit truncates with a
	// "+N more" marker.
	bins := aerospike.BinMap{}
	for i := 0; i < 40; i++ {
		bins[fmt.Sprintf("bin%02d", i)] = i
	}
	require.Contains(t, describeAerospikeBins(bins), "more")

	// Value shapes used by both transports.
	require.Equal(t, "<nil>", describeAerospikeValue(nil))
	require.Contains(t, describeAerospikeValue([]byte{1, 2}), "len=2")
	require.Contains(t, describeAerospikeValue([]interface{}{1}), "len=1")
	require.Contains(t, describeAerospikeValue(map[interface{}]interface{}{"k": 1}), "len=1")
	require.Contains(t, describeAerospikeValue(map[string]interface{}{"k": 1}), "len=1")
	require.NotEmpty(t, describeAerospikeValue(42))
}

package aerospike

import (
	"context"
	"fmt"
	"testing"

	aerospike "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
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
	s.nativeReplayProtection.Store(true)

	s.demoteNativeOnUnsupported(fmt.Errorf("op failed: %w", aeroErr(types.PARAMETER_ERROR))) //nolint:forbidigo // std %w chain needed; errors.New* erases the aerospike type
	require.False(t, s.useNativeTeranodeOps.Load(), "wrapped PARAMETER_ERROR must demote")
	require.False(t, s.nativeReplayProtection.Load(), "demotion clears the replay-protection verdict, so a stale true cannot re-arm the fenced sub-ops")

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
	_, err := luaResponseIntSlice("blockIDs", []byte{1, 2, 3})
	require.Error(t, err)

	ids, err := luaResponseIntSlice("blockIDs", []int64{1, 2})
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, ids)

	s := &Store{}
	_, err = s.ParseLuaMapResponse(map[string]interface{}{
		"status":   "OK",
		"blockIDs": []byte{10, 20},
	})
	require.Error(t, err)
}

// TestParseLuaMapResponseNamesMalformedIdempotentField pins that a malformed
// idempotent payload is reported as an idempotent problem. The shared integer
// list parser used to hardcode blockIDs in its errors, so this case read as a
// blockIDs fault during diagnosis.
func TestParseLuaMapResponseNamesMalformedIdempotentField(t *testing.T) {
	s := &Store{}

	for name, payload := range map[string]interface{}{
		"wrong type":  "not-a-list",
		"bad element": []interface{}{1, "two"},
		"bin-encoded": []byte{1, 2},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := s.ParseLuaMapResponse(map[string]interface{}{
				"status":     "OK",
				"idempotent": payload,
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), "idempotent")
			require.NotContains(t, err.Error(), "blockID")
		})
	}
}

// TestEncodeMarkLongestChainPayload pins the sub-op 14 wire payload
// [14, [onLongestChain, baseHeight, retention, unminedSinceStamp]] the C module reads positionally.
func TestEncodeMarkLongestChainPayload(t *testing.T) {
	payload, err := encodeMarkLongestChainPayload(true, 467, 6, 458)
	require.NoError(t, err)

	var decoded []any
	require.NoError(t, msgpack.Unmarshal(payload, &decoded))
	require.Len(t, decoded, 2)
	require.EqualValues(t, subOpMarkLongestChain, decoded[0])
	require.EqualValues(t, 14, decoded[0])

	args, ok := decoded[1].([]any)
	require.True(t, ok)
	require.Len(t, args, 4)
	require.Equal(t, true, args[0])
	require.EqualValues(t, 467, args[1])
	require.EqualValues(t, 6, args[2])
	require.EqualValues(t, 458, args[3])
}

// TestParseLuaMapResponseOldMarkFields checks the two fields only sub-op 14 reports are parsed when
// present, absent otherwise, and cleared by Reset.
func TestParseLuaMapResponseOldMarkFields(t *testing.T) {
	s := &Store{logger: ulogger.TestLogger{}}

	var res LuaMapResponse
	require.NoError(t, s.parseLuaMapResponseInto(map[interface{}]interface{}{
		"status": "OK", "signal": "DAHUNSET", "oldDeleteAtHeight": 470, "oldUnminedSince": 450,
	}, &res))
	require.True(t, res.HasOldDeleteAtHeight)
	require.Equal(t, 470, res.OldDeleteAtHeight)
	require.True(t, res.HasOldUnminedSince)
	require.Equal(t, 450, res.OldUnminedSince)

	res.Reset()
	require.False(t, res.HasOldDeleteAtHeight)
	require.False(t, res.HasOldUnminedSince)

	require.NoError(t, s.parseLuaMapResponseInto(map[interface{}]interface{}{"status": "OK"}, &res))
	require.False(t, res.HasOldDeleteAtHeight)
	require.False(t, res.HasOldUnminedSince)
}

// levelLogger records what a store logs at Warn and at Error.
type levelLogger struct {
	ulogger.TestLogger
	warns, errs []string
}

func (l *levelLogger) Warnf(format string, args ...interface{}) {
	l.warns = append(l.warns, fmt.Sprintf(format, args...))
}

func (l *levelLogger) Errorf(format string, args ...interface{}) {
	l.errs = append(l.errs, fmt.Sprintf(format, args...))
}

func nativeRoutingGauge(route string) float64 {
	return testutil.ToFloat64(prometheusAerospikeNativeRouting.WithLabelValues(route))
}

// TestRecordReplayProtection: a native-ops deployment whose server fails the replay-protection
// probe runs spendMulti, spend and addDeletedChildren on the UDF path for the life of the
// process, and nothing but a log line used to say so. The verdict decides the routing, and the
// log level says which failure it was: a server that answered wrongly is an Error, because the
// build has to be replaced, and a probe that could not finish is a Warn. The routing gauge is
// published by initNativeTeranodeOps, not here: see TestInitNativeTeranodeOpsPublishesRouting.
func TestRecordReplayProtection(t *testing.T) {
	for _, tc := range []struct {
		name      string
		verdict   replayProbeVerdict
		wantNativ bool
		wantErrs  int
		wantWarns int
	}{
		{name: "proven", verdict: replayProbeProven, wantNativ: true},
		{name: "the server answered wrongly", verdict: replayProbeMismatch, wantErrs: 1},
		{name: "the probe could not finish", verdict: replayProbeUnavailable, wantWarns: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger := &levelLogger{}
			s := &Store{logger: logger}

			s.recordReplayProtection(tc.verdict)

			require.Equal(t, tc.wantNativ, s.nativeReplayProtection.Load())
			require.Len(t, logger.errs, tc.wantErrs)
			require.Len(t, logger.warns, tc.wantWarns)
		})
	}
}

// TestInitNativeTeranodeOpsPublishesRouting: the replay probe records its verdict from inside
// detectNativeTeranodeOpSupport, before initNativeTeranodeOps stores useNativeTeranodeOps and
// useNativeVerifiedMarker, so only the publish at the end of initNativeTeranodeOps sees the whole
// decision. The probes are stubbed in the order production runs them. A healthy native cluster
// reports 1 for both routes, a server that fails a probe reports 0 for that route, and native
// ops off reports 0 for both, whatever the gauge held before.
func TestInitNativeTeranodeOpsPublishesRouting(t *testing.T) {
	InitPrometheusMetrics()

	yes := func(context.Context) bool { return true }
	no := func(context.Context) bool { return false }

	for _, tc := range []struct {
		name       string
		ops        bool
		replay     replayProbeVerdict
		marker     bool
		wantReplay float64
		wantMarker float64
	}{
		{name: "native ops and both probes proven", ops: true, replay: replayProbeProven, marker: true, wantReplay: 1, wantMarker: 1},
		{name: "replay protection not proven", ops: true, replay: replayProbeMismatch, marker: true, wantReplay: 0, wantMarker: 1},
		{name: "sub-op 15 not proven", ops: true, replay: replayProbeProven, marker: false, wantReplay: 1, wantMarker: 0},
		{name: "native ops off", ops: false, replay: replayProbeProven, marker: true, wantReplay: 0, wantMarker: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Store{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t)}

			// The opposite of what the run should publish, so a missing publish shows.
			prometheusAerospikeNativeRouting.WithLabelValues(nativeRouteReplayProtection).Set(1 - tc.wantReplay)
			prometheusAerospikeNativeRouting.WithLabelValues(nativeRouteVerifiedMarker).Set(1 - tc.wantMarker)

			opsProbe := no
			if tc.ops {
				opsProbe = func(context.Context) bool {
					s.recordReplayProtection(tc.replay)

					return true
				}
			}

			markerProbe := no
			if tc.marker {
				markerProbe = yes
			}

			s.nativeProbes = &nativeSupportProbes{ops: opsProbe, markLongestChain: yes, verifiedMarker: markerProbe}

			s.initNativeTeranodeOps(context.Background())

			require.Equal(t, tc.wantReplay, nativeRoutingGauge(nativeRouteReplayProtection))
			require.Equal(t, tc.wantMarker, nativeRoutingGauge(nativeRouteVerifiedMarker))
		})
	}
}

// TestNativeRoutingGaugeFollowsDemotion: a runtime PARAMETER_ERROR turns native ops off, and
// a sub-op 15 rejection turns the native marker write off on its own. The gauge follows both.
func TestNativeRoutingGaugeFollowsDemotion(t *testing.T) {
	InitPrometheusMetrics()

	s := &Store{logger: ulogger.TestLogger{}}
	s.useNativeTeranodeOps.Store(true)
	s.nativeReplayProtection.Store(true)
	s.useNativeVerifiedMarker.Store(true)
	s.publishNativeRouting()

	require.Equal(t, 1.0, nativeRoutingGauge(nativeRouteReplayProtection))
	require.Equal(t, 1.0, nativeRoutingGauge(nativeRouteVerifiedMarker))

	s.observePrunerNativeOpError(aeroErr(types.PARAMETER_ERROR))

	require.Equal(t, 1.0, nativeRoutingGauge(nativeRouteReplayProtection), "sub-op 15 is separate from replay protection")
	require.Equal(t, 0.0, nativeRoutingGauge(nativeRouteVerifiedMarker), "the marker write is back on the filtered path")

	s.demoteNativeOnUnsupported(aeroErr(types.PARAMETER_ERROR))

	require.Equal(t, 0.0, nativeRoutingGauge(nativeRouteReplayProtection), "native ops are off")
	require.Equal(t, 0.0, nativeRoutingGauge(nativeRouteVerifiedMarker))
}

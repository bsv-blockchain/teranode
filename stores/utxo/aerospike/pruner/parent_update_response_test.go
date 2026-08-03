package pruner

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/stretchr/testify/require"
)

// TestAddDeletedChildrenStatus covers the response shapes the addDeletedChildren
// mod-teranode op may return — the UDF path's map[interface{}]interface{}, a
// string-keyed map, and the client's ordered-map []aerospike.MapPair — and the
// field vocabulary the pruner keys on.
func TestAddDeletedChildrenStatus(t *testing.T) {
	t.Run("interface-keyed OK", func(t *testing.T) {
		status, errCode, errMsg, ok := addDeletedChildrenStatus(map[interface{}]interface{}{
			"status":    "OK",
			"errorCode": "",
		})
		require.True(t, ok)
		require.Equal(t, "OK", status)
		require.Empty(t, errCode)
		require.Empty(t, errMsg)
	})

	t.Run("string-keyed OK", func(t *testing.T) {
		status, errCode, _, ok := addDeletedChildrenStatus(map[string]interface{}{
			"status":    "OK",
			"errorCode": "",
		})
		require.True(t, ok)
		require.Equal(t, "OK", status)
		require.Empty(t, errCode)
	})

	t.Run("ordered-map (MapPair) OK", func(t *testing.T) {
		status, _, _, ok := addDeletedChildrenStatus([]aerospike.MapPair{
			{Key: "status", Value: "OK"},
		})
		require.True(t, ok)
		require.Equal(t, "OK", status)
	})

	t.Run("interface-keyed ERROR TX_NOT_FOUND", func(t *testing.T) {
		status, errCode, _, ok := addDeletedChildrenStatus(map[interface{}]interface{}{
			"status":    "ERROR",
			"errorCode": "TX_NOT_FOUND",
		})
		require.True(t, ok)
		require.Equal(t, "ERROR", status)
		require.Equal(t, "TX_NOT_FOUND", errCode)
	})

	t.Run("string-keyed ERROR TX_NOT_FOUND", func(t *testing.T) {
		status, errCode, _, ok := addDeletedChildrenStatus(map[string]interface{}{
			"status":    "ERROR",
			"errorCode": "TX_NOT_FOUND",
		})
		require.True(t, ok)
		require.Equal(t, "ERROR", status)
		require.Equal(t, "TX_NOT_FOUND", errCode)
	})

	t.Run("ordered-map ERROR TX_NOT_FOUND", func(t *testing.T) {
		status, errCode, _, ok := addDeletedChildrenStatus([]aerospike.MapPair{
			{Key: "status", Value: "ERROR"},
			{Key: "errorCode", Value: "TX_NOT_FOUND"},
		})
		require.True(t, ok)
		require.Equal(t, "ERROR", status)
		require.Equal(t, "TX_NOT_FOUND", errCode)
	})

	// The producers emit "message" — teranode.lua FIELD_MESSAGE, set by
	// addDeletedChildren on its not-found branch, and the key the store's own
	// ParseLuaMapResponse reads. Reading "errorMessage" instead silently dropped
	// every server-supplied detail from the synthesised error.
	t.Run("reads the message key the producers actually emit", func(t *testing.T) {
		shapes := map[string]interface{}{
			"interface-keyed": map[interface{}]interface{}{
				"status": "ERROR", "errorCode": "SOME_FAILURE", "message": "boom",
			},
			"string-keyed": map[string]interface{}{
				"status": "ERROR", "errorCode": "SOME_FAILURE", "message": "boom",
			},
			"ordered-map": []aerospike.MapPair{
				{Key: "status", Value: "ERROR"},
				{Key: "errorCode", Value: "SOME_FAILURE"},
				{Key: "message", Value: "boom"},
			},
		}

		for name, resp := range shapes {
			t.Run(name, func(t *testing.T) {
				status, errCode, errMsg, ok := addDeletedChildrenStatus(resp)
				require.True(t, ok)
				require.Equal(t, "ERROR", status)
				require.Equal(t, "SOME_FAILURE", errCode)
				require.Equal(t, "boom", errMsg, "the server-supplied message must reach the caller")
			})
		}
	})

	// errorMessage is kept as a tolerated alias for a hypothetical future
	// producer, but must not win over the key the producers really use.
	t.Run("errorMessage alias is tolerated but message wins", func(t *testing.T) {
		_, _, errMsg, ok := addDeletedChildrenStatus(map[string]interface{}{
			"status": "ERROR", "errorMessage": "alias-only",
		})
		require.True(t, ok)
		require.Equal(t, "alias-only", errMsg)

		_, _, errMsg, ok = addDeletedChildrenStatus(map[string]interface{}{
			"status": "ERROR", "message": "real", "errorMessage": "alias",
		})
		require.True(t, ok)
		require.Equal(t, "real", errMsg)
	})

	t.Run("map without a string status reports not-ok", func(t *testing.T) {
		_, _, _, ok := addDeletedChildrenStatus(map[interface{}]interface{}{"errorCode": "X"})
		require.False(t, ok)

		_, _, _, ok = addDeletedChildrenStatus(map[string]interface{}{"status": 42})
		require.False(t, ok)

		_, _, _, ok = addDeletedChildrenStatus([]aerospike.MapPair{{Key: "errorCode", Value: "X"}})
		require.False(t, ok)
	})

	t.Run("unrecognised shape returns false", func(t *testing.T) {
		_, _, _, ok := addDeletedChildrenStatus("not-a-map")
		require.False(t, ok)

		_, _, _, ok = addDeletedChildrenStatus(nil)
		require.False(t, ok)

		_, _, _, ok = addDeletedChildrenStatus(42)
		require.False(t, ok)
	})
}

// batchRecordWithBins builds a BatchRecord carrying a response bin, as the
// mod-teranode paths return it.
func batchRecordWithBins(t *testing.T, bins aerospike.BinMap) *aerospike.BatchRecord {
	t.Helper()

	key, err := aerospike.NewKey("test", "utxo", []byte("parent"))
	require.NoError(t, err)

	return &aerospike.BatchRecord{
		Key:    key,
		Record: &aerospike.Record{Bins: bins},
	}
}

// batchRecordWithErr builds a BatchRecord that failed with the given error.
func batchRecordWithErr(t *testing.T, batchErr aerospike.Error) *aerospike.BatchRecord {
	t.Helper()

	key, err := aerospike.NewKey("test", "utxo", []byte("parent"))
	require.NoError(t, err)

	return &aerospike.BatchRecord{Key: key, Err: batchErr}
}

// TestClassifyParentUpdateResult is the fail-closed contract. Every outcome the
// pruner cannot positively read as a success must be classified as a failure:
// the caller deletes the child records in the same batch on the strength of this
// classification, so counting an unreadable response as success would delete
// children whose parent's deletedChildren map was never updated — the exact
// referential-integrity break the parent-update phase exists to prevent.
func TestClassifyParentUpdateResult(t *testing.T) {
	t.Run("mod-teranode OK", func(t *testing.T) {
		outcome, err := classifyParentUpdateResult(
			batchRecordWithBins(t, aerospike.BinMap{"SUCCESS": map[interface{}]interface{}{"status": "OK"}}), true)
		require.NoError(t, err)
		require.Equal(t, parentUpdateOK, outcome)
	})

	t.Run("mod-teranode ERROR TX_NOT_FOUND is a skip, not a failure", func(t *testing.T) {
		outcome, err := classifyParentUpdateResult(
			batchRecordWithBins(t, aerospike.BinMap{"SUCCESS": map[interface{}]interface{}{
				"status": "ERROR", "errorCode": "TX_NOT_FOUND",
			}}), true)
		require.NoError(t, err)
		require.Equal(t, parentUpdateNotFound, outcome)
	})

	t.Run("mod-teranode ERROR carries code and message into the error", func(t *testing.T) {
		outcome, err := classifyParentUpdateResult(
			batchRecordWithBins(t, aerospike.BinMap{"SUCCESS": map[interface{}]interface{}{
				"status": "ERROR", "errorCode": "BOOM", "message": "detail from server",
			}}), true)
		require.Equal(t, parentUpdateFailed, outcome)
		require.ErrorContains(t, err, "BOOM")
		require.ErrorContains(t, err, "detail from server")
	})

	// KEY_NOT_FOUND is how the native path's UPDATE_ONLY policy and the plain
	// MapPutItems path both report an already-deleted parent. Benign on every
	// path — no path treats a missing parent as an error.
	t.Run("KEY_NOT_FOUND is a skip on both contracts", func(t *testing.T) {
		for _, usesModTeranode := range []bool{true, false} {
			outcome, err := classifyParentUpdateResult(batchRecordWithErr(t, aerospike.ErrKeyNotFound), usesModTeranode)
			require.NoError(t, err)
			require.Equalf(t, parentUpdateNotFound, outcome, "usesModTeranode=%v", usesModTeranode)
		}
	})

	t.Run("other batch errors are failures", func(t *testing.T) {
		outcome, err := classifyParentUpdateResult(batchRecordWithErr(t, aerospike.ErrTimeout), true)
		require.Equal(t, parentUpdateFailed, outcome)
		require.Error(t, err)
	})

	// The fail-closed cases. Each of these previously resolved to success.
	t.Run("missing SUCCESS bin fails closed", func(t *testing.T) {
		outcome, err := classifyParentUpdateResult(
			batchRecordWithBins(t, aerospike.BinMap{"someOtherBin": 1}), true)
		require.Equal(t, parentUpdateFailed, outcome)
		require.ErrorContains(t, err, "SUCCESS")
	})

	t.Run("nil record fails closed", func(t *testing.T) {
		key, err := aerospike.NewKey("test", "utxo", []byte("parent"))
		require.NoError(t, err)

		outcome, classifyErr := classifyParentUpdateResult(&aerospike.BatchRecord{Key: key}, true)
		require.Equal(t, parentUpdateFailed, outcome)
		require.ErrorContains(t, classifyErr, "no record")
	})

	t.Run("unreadable response shape fails closed", func(t *testing.T) {
		outcome, err := classifyParentUpdateResult(
			batchRecordWithBins(t, aerospike.BinMap{"SUCCESS": "not-a-map"}), true)
		require.Equal(t, parentUpdateFailed, outcome)
		require.ErrorContains(t, err, "unreadable")
	})

	t.Run("unknown status fails closed", func(t *testing.T) {
		outcome, err := classifyParentUpdateResult(
			batchRecordWithBins(t, aerospike.BinMap{"SUCCESS": map[interface{}]interface{}{"status": "MAYBE"}}), true)
		require.Equal(t, parentUpdateFailed, outcome)
		require.ErrorContains(t, err, "MAYBE")
	})

	// The plain MapPutItems BatchWrite has no response map at all — a nil Err is
	// the whole of its success signal, so the SUCCESS-bin requirement must not
	// apply to it.
	t.Run("plain BatchWrite path succeeds without a response map", func(t *testing.T) {
		key, err := aerospike.NewKey("test", "utxo", []byte("parent"))
		require.NoError(t, err)

		outcome, classifyErr := classifyParentUpdateResult(&aerospike.BatchRecord{Key: key}, false)
		require.NoError(t, classifyErr)
		require.Equal(t, parentUpdateOK, outcome)
	})
}

// okRecord / notFoundRecord / brokenRecord build the three parent-update results
// the tally has to tell apart.
func okRecord(t *testing.T) aerospike.BatchRecordIfc {
	t.Helper()
	return &aerospike.BatchWrite{BatchRecord: *batchRecordWithBins(t,
		aerospike.BinMap{"SUCCESS": map[interface{}]interface{}{"status": "OK"}})}
}

func notFoundRecord(t *testing.T) aerospike.BatchRecordIfc {
	t.Helper()
	return &aerospike.BatchWrite{BatchRecord: *batchRecordWithErr(t, aerospike.ErrKeyNotFound)}
}

func brokenRecord(t *testing.T) aerospike.BatchRecordIfc {
	t.Helper()
	return &aerospike.BatchWrite{BatchRecord: *batchRecordWithBins(t,
		aerospike.BinMap{"SUCCESS": "not-a-map"})}
}

// TestTallyParentUpdateResults covers the aggregation the combined and two-call
// cleanup paths both run over their parent-update region — previously inline in
// executeBatchCleanupCombined and therefore only reachable with a live client.
func TestTallyParentUpdateResults(t *testing.T) {
	t.Run("counts each outcome independently", func(t *testing.T) {
		records := []aerospike.BatchRecordIfc{
			okRecord(t), okRecord(t), notFoundRecord(t), brokenRecord(t),
		}

		tally := tallyParentUpdateResults(records, true, nil)

		require.Equal(t, 2, tally.success)
		require.Equal(t, 1, tally.notFound)
		require.Equal(t, 1, tally.failed)
		require.ErrorContains(t, tally.firstErr, "unreadable")
	})

	// A batch of purely skipped parents is a clean run, not a failure — this is
	// the routine shape when the pruner already deleted the parents.
	t.Run("all-skipped is not a failure", func(t *testing.T) {
		tally := tallyParentUpdateResults(
			[]aerospike.BatchRecordIfc{notFoundRecord(t), notFoundRecord(t)}, true, nil)

		require.Zero(t, tally.failed)
		require.Equal(t, 2, tally.notFound)
		require.NoError(t, tally.firstErr)
	})

	t.Run("firstErr keeps the first failure only", func(t *testing.T) {
		unknownStatus := &aerospike.BatchWrite{BatchRecord: *batchRecordWithBins(t,
			aerospike.BinMap{"SUCCESS": map[interface{}]interface{}{"status": "FIRST_FAILURE"}})}

		tally := tallyParentUpdateResults(
			[]aerospike.BatchRecordIfc{unknownStatus, brokenRecord(t)}, true, nil)

		require.Equal(t, 2, tally.failed)
		require.ErrorContains(t, tally.firstErr, "FIRST_FAILURE")
	})

	t.Run("empty region tallies nothing", func(t *testing.T) {
		tally := tallyParentUpdateResults(nil, true, nil)

		require.Zero(t, tally.success)
		require.Zero(t, tally.notFound)
		require.Zero(t, tally.failed)
		require.NoError(t, tally.firstErr)
	})
}

// TestTallyChildDeletionResults locks the idempotency rule: a child that is
// already gone is the outcome the deletion asked for, not an error.
func TestTallyChildDeletionResults(t *testing.T) {
	t.Run("KEY_NOT_FOUND is success", func(t *testing.T) {
		deleteErrors, firstErr := tallyChildDeletionResults(
			[]aerospike.BatchRecordIfc{notFoundRecord(t), notFoundRecord(t)})

		require.Zero(t, deleteErrors)
		require.Nil(t, firstErr)
	})

	t.Run("real errors are counted", func(t *testing.T) {
		failing := &aerospike.BatchWrite{BatchRecord: *batchRecordWithErr(t, aerospike.ErrTimeout)}

		deleteErrors, firstErr := tallyChildDeletionResults(
			[]aerospike.BatchRecordIfc{notFoundRecord(t), failing, failing})

		require.Equal(t, 2, deleteErrors)
		require.NotNil(t, firstErr)
	})

	t.Run("clean batch reports nothing", func(t *testing.T) {
		deleteErrors, firstErr := tallyChildDeletionResults(
			[]aerospike.BatchRecordIfc{okRecord(t)})

		require.Zero(t, deleteErrors)
		require.Nil(t, firstErr)
	})
}

// TestTallyParentUpdateResultsObservesErrors covers the hook that lets the store
// demote itself off the native path.
//
// The pruner is the only native call site that executes its own BatchOperate, so
// without this it could never trigger demoteNativeOnUnsupported. It runs every
// block, making it a likely first victim of a mixed-version cluster that passed
// the single-partition startup probe and then rejects native writes.
func TestTallyParentUpdateResultsObservesErrors(t *testing.T) {
	t.Run("per-record errors reach the observer", func(t *testing.T) {
		var seen []error

		records := []aerospike.BatchRecordIfc{
			okRecord(t),
			&aerospike.BatchWrite{BatchRecord: *batchRecordWithErr(t, aerospike.ErrInvalidParam)},
			notFoundRecord(t),
		}

		tally := tallyParentUpdateResults(records, true, func(err error) { seen = append(seen, err) })

		// Both failing records are reported; the successful one is not. Filtering
		// by result code is the store's job, so KEY_NOT_FOUND is passed on too.
		require.Len(t, seen, 2)
		require.ErrorIs(t, seen[0], aerospike.ErrInvalidParam)
		require.Equal(t, 1, tally.success)
	})

	// The plain MapPutItems fallback never travelled the native path, so its
	// failures carry no signal about native-op support and must not demote.
	t.Run("plain BatchWrite path is never observed", func(t *testing.T) {
		called := false

		records := []aerospike.BatchRecordIfc{
			&aerospike.BatchWrite{BatchRecord: *batchRecordWithErr(t, aerospike.ErrInvalidParam)},
		}

		tallyParentUpdateResults(records, false, func(error) { called = true })

		require.False(t, called, "non-mod-teranode records must not reach the native-op observer")
	})

	t.Run("nil observer is safe", func(t *testing.T) {
		records := []aerospike.BatchRecordIfc{
			&aerospike.BatchWrite{BatchRecord: *batchRecordWithErr(t, aerospike.ErrInvalidParam)},
		}

		require.NotPanics(t, func() { tallyParentUpdateResults(records, true, nil) })
	})

	t.Run("successful batches report nothing", func(t *testing.T) {
		called := false

		tallyParentUpdateResults([]aerospike.BatchRecordIfc{okRecord(t)}, true, func(error) { called = true })

		require.False(t, called)
	})
}

// TestReportNativeOpError covers the Service-level wrapper's nil guards — every
// pruner test constructs a Service without an observer.
func TestReportNativeOpError(t *testing.T) {
	t.Run("no observer configured", func(t *testing.T) {
		s := &Service{}
		require.NotPanics(t, func() { s.reportNativeOpError(aerospike.ErrInvalidParam) })
	})

	t.Run("nil error is not forwarded", func(t *testing.T) {
		called := false
		s := &Service{observeNativeOpError: func(error) { called = true }}

		s.reportNativeOpError(nil)
		require.False(t, called)
	})

	t.Run("error is forwarded verbatim", func(t *testing.T) {
		var got error
		s := &Service{observeNativeOpError: func(err error) { got = err }}

		s.reportNativeOpError(aerospike.ErrInvalidParam)
		require.ErrorIs(t, got, aerospike.ErrInvalidParam)
	})
}

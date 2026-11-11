package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestNativeMarkResult covers how one sub-op 14 batch record is classified, and that a rejection of
// sub-op 14 never demotes the store's other native ops.
func TestNativeMarkResult(t *testing.T) {
	key, err := aerospike.NewKey("ns", "set", []byte{1})
	require.NoError(t, err)

	record := func(bins aerospike.BinMap, recErr aerospike.Error) *aerospike.BatchRecord {
		br := aerospike.NewBatchWrite(nil, key, aerospike.PutOp(aerospike.NewBin("x", 1))).BatchRec()
		br.Err = recErr

		if bins != nil {
			br.Record = &aerospike.Record{Bins: bins}
		}

		return br
	}

	ok := aerospike.BinMap{nativeOpResultBin: map[interface{}]interface{}{"status": "OK"}}
	notFound := aerospike.BinMap{nativeOpResultBin: map[interface{}]interface{}{"status": "ERROR", "errorCode": "TX_NOT_FOUND", "message": "x"}}
	failed := aerospike.BinMap{nativeOpResultBin: map[interface{}]interface{}{"status": "ERROR", "errorCode": "UPDATE_FAILED", "message": "x"}}

	cases := []struct {
		name string
		br   *aerospike.BatchRecord
		want nativeMarkOutcome
	}{
		{"ok", record(ok, nil), nativeMarkOK},
		{"key not found is missing", record(nil, aeroErr(types.KEY_NOT_FOUND_ERROR)), nativeMarkMissing},
		{"TX_NOT_FOUND status is missing", record(notFound, nil), nativeMarkMissing},
		{"parameter error is unsupported", record(nil, aeroErr(types.PARAMETER_ERROR)), nativeMarkUnsupported},
		{"other record error fails", record(nil, aeroErr(types.TIMEOUT)), nativeMarkFailed},
		{"error status fails", record(failed, nil), nativeMarkFailed},
		{"no record fails", record(nil, nil), nativeMarkFailed},
		{"unparsable response fails", record(aerospike.BinMap{nativeOpResultBin: "junk"}, nil), nativeMarkFailed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Store{logger: ulogger.TestLogger{}}
			s.useNativeTeranodeOps.Store(true)
			s.useNativeMarkLongestChain.Store(true)

			var res LuaMapResponse

			got, _ := s.nativeMarkResult(tc.br, &res)
			require.Equal(t, tc.want, got)
			require.True(t, s.useNativeTeranodeOps.Load(), "a sub-op 14 result must never demote the other native ops")
		})
	}
}

// TestCollectNativeMarkFollowUp checks a sub-op 14 response queues the same page follow-up and
// restore state the expression path builds from its bins.
func TestCollectNativeMarkFollowUp(t *testing.T) {
	hash := chainhash.HashH([]byte("native-mark-follow-up"))

	t.Run("DAHSET copies the new height to the pages", func(t *testing.T) {
		var f childDAHFollowUps

		states := map[chainhash.Hash]masterState{}
		res := &LuaMapResponse{Status: LuaStatusOK, Signal: LuaSignalDAHSet, ChildCount: 2,
			DeleteAtHeight: 473, HasDeleteAtHeight: true, DAHVersion: 1, HasDAHVersion: true,
			OldUnminedSince: 450, HasOldUnminedSince: true}

		collectNativeMarkFollowUp(&f, states, &hash, res)

		sets := f.sets()
		require.Len(t, sets, 1)
		require.Equal(t, hash, *sets[0].TxID)
		require.Equal(t, 2, sets[0].ChildCount)
		require.Equal(t, uint32(473), sets[0].DeleteAtHeight)
		require.Equal(t, dahVersion{value: 1, known: true}, sets[0].Version)
		require.Empty(t, f.unsets())

		st := states[hash]
		require.NotNil(t, st.newDAH)
		require.Equal(t, uint32(473), *st.newDAH)
		require.Nil(t, st.oldDAH)
		require.NotNil(t, st.oldUnminedSince)
		require.Equal(t, 450, *st.oldUnminedSince)
	})

	t.Run("DAHUNSET clears the pages", func(t *testing.T) {
		var f childDAHFollowUps

		states := map[chainhash.Hash]masterState{}
		res := &LuaMapResponse{Status: LuaStatusOK, Signal: LuaSignalDAHUnset, ChildCount: 2,
			DAHVersion: 1, HasDAHVersion: true, OldDeleteAtHeight: 470, HasOldDeleteAtHeight: true}

		collectNativeMarkFollowUp(&f, states, &hash, res)

		unsets := f.unsets()
		require.Len(t, unsets, 1)
		require.Equal(t, 2, unsets[0].ChildCount)
		require.Empty(t, f.sets())

		st := states[hash]
		require.Nil(t, st.newDAH)
		require.NotNil(t, st.oldDAH)
		require.Equal(t, uint32(470), *st.oldDAH)
		require.Nil(t, st.oldUnminedSince)
	})

	t.Run("no signal queues nothing", func(t *testing.T) {
		var f childDAHFollowUps

		states := map[chainhash.Hash]masterState{}
		collectNativeMarkFollowUp(&f, states, &hash, &LuaMapResponse{Status: LuaStatusOK})

		require.Empty(t, f.sets())
		require.Empty(t, f.unsets())
		require.Empty(t, states)
	})
}

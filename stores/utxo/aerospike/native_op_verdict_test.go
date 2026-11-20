package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// The native-op gate lets spendMulti onto the server fork's dispatcher only if
// that dispatcher demonstrably enforces the pruner's replay marker and reports
// idempotent matches. A stock container refuses the opcode outright, so the
// probe demotes before either verdict is reached and no container test can
// exercise them. These pin the verdicts themselves, so a dispatcher that
// answers the wrong thing cannot be read as compliant.
func TestProbeVerdicts(t *testing.T) {
	t.Run("a pruned-child spend counts as rejected only on the marker code", func(t *testing.T) {
		require.True(t, rejectsWith(&LuaMapResponse{Status: LuaStatusError, ErrorCode: LuaErrorCodeInvalidSpend}, LuaErrorCodeInvalidSpend))
		require.True(t, rejectsWith(&LuaMapResponse{
			Status: LuaStatusError,
			Errors: map[int]LuaErrorInfo{0: {ErrorCode: LuaErrorCodeInvalidSpend}},
		}, LuaErrorCodeInvalidSpend), "a per-spend rejection counts")

		require.False(t, rejectsWith(&LuaMapResponse{Status: LuaStatusOK}, LuaErrorCodeInvalidSpend),
			"accepting the replay is the failure this probe exists to catch")
		require.False(t, rejectsWith(&LuaMapResponse{Status: LuaStatusError, ErrorCode: LuaErrorCodeSpent}, LuaErrorCodeInvalidSpend),
			"rejecting as already-spent is not proof the marker was consulted")
		require.False(t, rejectsWith(nil, LuaErrorCodeInvalidSpend))
	})

	t.Run("an idempotent match must be reported for the spend that made it", func(t *testing.T) {
		require.True(t, reportsIdempotent(&LuaMapResponse{Status: LuaStatusOK, Idempotent: []int{0}}, 0))
		require.True(t, reportsIdempotent(&LuaMapResponse{Status: LuaStatusOK, Idempotent: []int{2, 0}}, 0))

		require.False(t, reportsIdempotent(&LuaMapResponse{Status: LuaStatusOK}, 0),
			"a dispatcher that omits the list would have the store unspend a confirmed output")
		require.False(t, reportsIdempotent(&LuaMapResponse{Status: LuaStatusOK, Idempotent: []int{1}}, 0),
			"another spend being idempotent says nothing about this one")
		require.False(t, reportsIdempotent(&LuaMapResponse{Status: LuaStatusError, Idempotent: []int{0}}, 0),
			"a rejection is not an idempotent match")
		require.False(t, reportsIdempotent(nil, 0))
	})

	t.Run("a match must carry the height the first spend recorded, under the spend's idx", func(t *testing.T) {
		withHeight := func(heights map[int]uint32) *LuaMapResponse {
			return &LuaMapResponse{Status: LuaStatusOK, Idempotent: []int{probeSpendIdx}, IdempotentHeights: heights}
		}

		require.True(t, probeReportsHeight(withHeight(map[int]uint32{probeSpendIdx: 1}), 1))

		require.False(t, probeReportsHeight(withHeight(nil), 1),
			"a native answer without the recorded height fails the probe, so spends stay on the UDF")
		require.False(t, probeReportsHeight(withHeight(map[int]uint32{}), 1))
		require.False(t, probeReportsHeight(withHeight(map[int]uint32{probeSpendIdx: 5}), 1),
			"echoing the incoming height is not reporting the recorded one")
		require.False(t, probeReportsHeight(withHeight(map[int]uint32{0: 1}), 1), "a height keyed by offset or vout is not keyed by idx")
		require.False(t, probeReportsHeight(&LuaMapResponse{Status: LuaStatusError, IdempotentHeights: map[int]uint32{probeSpendIdx: 1}}, 1))
		require.False(t, probeReportsHeight(nil, 1))
	})

	t.Run("the probe spend's idx differs from its offset and vout", func(t *testing.T) {
		store := &Store{utxoBatchSize: 128}
		item := store.probeSpendItem(&chainhash.Hash{0x01}, spendpkg.NewSpendingData(&chainhash.Hash{0x02}, 0), 0)

		require.Equal(t, probeSpendIdx, item["idx"])
		require.NotEqual(t, item["idx"], item["offset"], "a dispatcher keyed by offset must not pass")
		require.NotEqual(t, item["idx"], item["vOut"], "a dispatcher keyed by vout must not pass")
	})

	t.Run("a dispatcher that keys its report by offset or vout fails the probe", func(t *testing.T) {
		// The probe record's offset and vout are both 0.
		keyedByOffset := &LuaMapResponse{Status: LuaStatusOK, Idempotent: []int{0}}
		require.False(t, probeReportsIdempotent(keyedByOffset))
		require.True(t, probeReportsIdempotent(&LuaMapResponse{Status: LuaStatusOK, Idempotent: []int{probeSpendIdx}}))

		wrongKey := &LuaMapResponse{Status: LuaStatusError, Errors: map[int]LuaErrorInfo{0: {ErrorCode: LuaErrorCodeInvalidSpend}}}
		require.False(t, probeReplayRejected(wrongKey), "an error keyed by offset or vout is not keyed by idx")

		wholeResponse := &LuaMapResponse{Status: LuaStatusError, ErrorCode: LuaErrorCodeInvalidSpend}
		require.False(t, probeReplayRejected(wholeResponse), "the store reads rejections per spend")

		right := &LuaMapResponse{Status: LuaStatusError, Errors: map[int]LuaErrorInfo{probeSpendIdx: {ErrorCode: LuaErrorCodeInvalidSpend}}}
		require.True(t, probeReplayRejected(right))
		require.False(t, rejectsItemWith(right, probeSpendIdx, LuaErrorCodeSpent), "the code has to match")
		require.False(t, probeReplayRejected(&LuaMapResponse{Status: LuaStatusOK, Errors: right.Errors}))
		require.False(t, probeReplayRejected(nil))
	})
}

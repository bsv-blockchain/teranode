package aerospike

import (
	"testing"

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
}

package validator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOptionsFromValidateRequest_SkipScriptAndOutpointOnlyRoundTrip pins the
// Client → Server gRPC propagation of the two below-checkpoint fast-path
// options: SkipScriptValidation and OutpointOnlySpend.
//
// Both options must survive the gRPC boundary intact so the fast path is not
// silently limited to in-process validators. The test builds a request via
// buildValidateTxRequest (the same function the gRPC client uses) and
// reconstructs Options via optionsFromValidateRequest (the same function the
// gRPC server uses), then asserts that both options are preserved.
func TestOptionsFromValidateRequest_SkipScriptAndOutpointOnlyRoundTrip(t *testing.T) {
	src := &Options{
		SkipScriptValidation: true,
		OutpointOnlySpend:    true,
	}

	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 620000, src)
	got, err := optionsFromValidateRequest(req)
	require.NoError(t, err)

	require.True(t, got.SkipScriptValidation, "SkipScriptValidation must survive gRPC round-trip")
	require.True(t, got.OutpointOnlySpend, "OutpointOnlySpend must survive gRPC round-trip")
}

// TestOptionsFromValidateRequest_SkipScriptAndOutpointOnlyDefaultFalse
// confirms both options default to false when not set by the caller — i.e. a
// request with neither flag reconstructs with both false (old behaviour,
// full validation).
func TestOptionsFromValidateRequest_SkipScriptAndOutpointOnlyDefaultFalse(t *testing.T) {
	src := &Options{
		SkipPolicyChecks: true, // unrelated option to ensure defaults are independent
	}

	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 620000, src)
	got, err := optionsFromValidateRequest(req)
	require.NoError(t, err)

	require.False(t, got.SkipScriptValidation, "SkipScriptValidation must default to false")
	require.False(t, got.OutpointOnlySpend, "OutpointOnlySpend must default to false")
}

// The HTTP leg of this pair is deliberately gone: the /tx endpoint no longer
// carries validation options on any body shape, so there is no HTTP round trip
// left to pin. The gRPC round trips above are the surviving wire. The refusal
// itself is pinned by http_trust_flags_test.go.

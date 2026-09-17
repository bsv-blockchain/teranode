package validator

import (
	"testing"

	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestIgnorePolicyFreeze_WireRoundTrip pins the client-build → wire →
// server-reconstruction path for ignore_policy_freeze (field 15) on both transports.
//
// The flag marks a block-context spend, on which only the height-anchored consensus
// tier of an alert-system freeze may reject. A silent drop on either transport would
// make a remote validator enforce the wall-clock policy tier during block validation
// and reintroduce the fleet split issue #1422 removes — so true AND false must survive.
func TestIgnorePolicyFreeze_WireRoundTrip(t *testing.T) {
	t.Run("true survives gRPC build, marshal, and reconstruction", func(t *testing.T) {
		opts := ProcessOptions(WithIgnorePolicyFreeze(true))

		req := buildValidateTxRequest([]byte{1, 2, 3}, 500000, opts)
		require.NotNil(t, req.IgnorePolicyFreeze)
		require.True(t, *req.IgnorePolicyFreeze)

		bytesOut, err := proto.Marshal(req)
		require.NoError(t, err)

		got := &validator_api.ValidateTransactionRequest{}
		require.NoError(t, proto.Unmarshal(bytesOut, got))

		reconstructed, err := optionsFromValidateRequest(got)
		require.NoError(t, err)
		require.True(t, reconstructed.IgnorePolicyFreeze, "IgnorePolicyFreeze must survive gRPC round-trip")
	})

	t.Run("false survives gRPC round-trip and is the default", func(t *testing.T) {
		opts := ProcessOptions(WithSkipPolicyChecks(true)) // unrelated option; the default must be independent

		req := buildValidateTxRequest([]byte{1, 2, 3}, 500000, opts)

		bytesOut, err := proto.Marshal(req)
		require.NoError(t, err)

		got := &validator_api.ValidateTransactionRequest{}
		require.NoError(t, proto.Unmarshal(bytesOut, got))

		reconstructed, err := optionsFromValidateRequest(got)
		require.NoError(t, err)
		require.False(t, reconstructed.IgnorePolicyFreeze, "IgnorePolicyFreeze must default to false")
	})

	t.Run("absent field reconstructs as false (old sender, new validator)", func(t *testing.T) {
		reconstructed, err := optionsFromValidateRequest(&validator_api.ValidateTransactionRequest{
			TransactionData: []byte{1, 2, 3},
			BlockHeight:     500000,
		})
		require.NoError(t, err)
		require.False(t, reconstructed.IgnorePolicyFreeze)
	})

	t.Run("true survives the HTTP fallback query string", func(t *testing.T) {
		q := buildValidateTxHTTPQuery(&Options{IgnorePolicyFreeze: true}, 500000)

		e := echo.New()
		ctx, err := echoRequestWithQuery(e, q.Encode())
		require.NoError(t, err)

		_, opts := extractValidationParams(ctx)
		require.True(t, opts.IgnorePolicyFreeze, "IgnorePolicyFreeze must survive HTTP fallback round-trip")
	})

	t.Run("false is not emitted on the HTTP fallback query string", func(t *testing.T) {
		q := buildValidateTxHTTPQuery(&Options{}, 500000)
		require.Empty(t, q.Get("ignorePolicyFreeze"))

		e := echo.New()
		ctx, err := echoRequestWithQuery(e, q.Encode())
		require.NoError(t, err)

		_, opts := extractValidationParams(ctx)
		require.False(t, opts.IgnorePolicyFreeze)
	})
}

package validator

import (
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// TestOptionsFromValidateRequest_InBlockRoundTrip pins the Client → Server
// gRPC propagation of the InBlock provenance option. InBlock marks a
// transaction that arrived as part of a block or announced subtree (block
// validation, subtree validation, legacy sync) rather than via mempool
// submission; the validator publishes it on the txmeta topic so relay
// consumers never announce such transactions. It must be an explicit option —
// deriving it from SkipPolicyChecks is wrong because that flag also arrives
// from external gRPC/Kafka submitters for genuinely fresh transactions.
func TestOptionsFromValidateRequest_InBlockRoundTrip(t *testing.T) {
	src := &Options{InBlock: true}

	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 420000, src)
	got, err := optionsFromValidateRequest(req)
	require.NoError(t, err)

	require.True(t, got.InBlock)
}

// TestOptionsFromValidateRequest_InBlockDefaultsFalse confirms the option is
// off unless a block-context caller sets it explicitly — in particular it must
// NOT be inferred from SkipPolicyChecks.
func TestOptionsFromValidateRequest_InBlockDefaultsFalse(t *testing.T) {
	src := &Options{SkipPolicyChecks: true}

	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 420000, src)
	got, err := optionsFromValidateRequest(req)
	require.NoError(t, err)

	require.False(t, got.InBlock)
}

// TestHTTPHandlerPath_InBlock_EndToEnd mirrors the /tx HTTP fallback handler:
// client builds the query, server parses it back into Options.
func TestHTTPHandlerPath_InBlock_EndToEnd(t *testing.T) {
	q := buildValidateTxHTTPQuery(&Options{InBlock: true}, 420000)

	e := echo.New()
	ctx, err := echoRequestWithQuery(e, q.Encode())
	require.NoError(t, err)

	_, opts := extractValidationParams(ctx)
	require.True(t, opts.InBlock)
}

// TestWithInBlock pins the functional option setter.
func TestWithInBlock(t *testing.T) {
	opts := ProcessOptions(WithInBlock(true))
	require.True(t, opts.InBlock)
}

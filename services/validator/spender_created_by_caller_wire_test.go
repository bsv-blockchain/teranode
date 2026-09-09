package validator

import (
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// The legacy block path can run against a remote validator. The flag that
// switches the missing-parent bless off has to survive both transports, or a
// remote validator blesses a replay of a fully pruned chain on the strength of
// the record the block path just wrote.
func TestOptionsFromValidateRequest_SpenderCreatedByCallerRoundTrip(t *testing.T) {
	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 620000, &Options{SpenderCreatedByCaller: true})
	got, err := optionsFromValidateRequest(req)
	require.NoError(t, err)
	require.True(t, got.SpenderCreatedByCaller, "SpenderCreatedByCaller must survive the gRPC round-trip")

	req = buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 620000, &Options{SkipPolicyChecks: true})
	got, err = optionsFromValidateRequest(req)
	require.NoError(t, err)
	require.False(t, got.SpenderCreatedByCaller, "SpenderCreatedByCaller must default to false")
}

func TestHTTPHandlerPath_SpenderCreatedByCaller(t *testing.T) {
	q := buildValidateTxHTTPQuery(&Options{SpenderCreatedByCaller: true}, 620000)

	e := echo.New()
	ctx, err := echoRequestWithQuery(e, q.Encode())
	require.NoError(t, err)

	_, opts := extractValidationParams(ctx)
	require.True(t, opts.SpenderCreatedByCaller, "SpenderCreatedByCaller must survive the HTTP query string")
}

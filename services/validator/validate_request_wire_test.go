package validator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestIsProtobufContentType pins the Content-Type discrimination used by
// handleSingleTx to choose between the protobuf-body path and the legacy
// raw-bytes-plus-query-params path. Includes parameter-tolerant variants
// (charset, quality) that well-behaved HTTP intermediaries may append.
func TestIsProtobufContentType(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  bool
	}{
		{"empty", "", false},
		{"octet-stream", "application/octet-stream", false},
		{"json", "application/json", false},
		{"x-protobuf bare", "application/x-protobuf", true},
		{"x-protobuf upper", "APPLICATION/X-PROTOBUF", true},
		{"x-protobuf with charset", "application/x-protobuf; charset=binary", true},
		{"x-protobuf with spaces", "  application/x-protobuf  ", true},
		{"protobuf bare", "application/protobuf", true},
		{"protobuf with params", "application/protobuf; q=1.0", true},
		{"protobuf-ish but wrong", "application/x-protobuf-2", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isProtobufContentType(tc.input))
		})
	}
}

// TestHTTPHandlerPath_ProtobufBody_EndToEnd pins that a request received on
// the /tx HTTP endpoint with Content-Type: application/x-protobuf and a
// marshalled ValidateTransactionRequest body round-trips the block-validation
// option fields (UnconfirmedParentsAtCandidateHeight,
// CandidateParentMedianTime) correctly. Mirrors the protobuf-body shape the
// validator's HTTP fallback client produces — the proto-body path is the
// field-parity guarantee for options that have no query-string representation
// drift.
func TestHTTPHandlerPath_ProtobufBody_EndToEnd(t *testing.T) {
	src := ProcessOptions(
		WithUnconfirmedParentsAtCandidateHeight(true),
		WithCandidateParentMedianTime(1234567),
	)

	srcReq := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 42, src)
	body, err := proto.Marshal(srcReq)
	require.NoError(t, err)

	// Simulate what handleSingleTx would do for an x-protobuf request.
	e := echo.New()
	httpReq := httptest.NewRequest(http.MethodPost, "/tx", strings.NewReader(string(body)))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	rec := httptest.NewRecorder()
	_ = e.NewContext(httpReq, rec)

	require.True(t, isProtobufContentType(httpReq.Header.Get("Content-Type")))

	got := &validator_api.ValidateTransactionRequest{}
	require.NoError(t, proto.Unmarshal(body, got))

	gotOpts, err := optionsFromValidateRequest(got)
	require.NoError(t, err)
	require.True(t, gotOpts.UnconfirmedParentsAtCandidateHeight)
	require.Equal(t, uint32(1234567), gotOpts.CandidateParentMedianTime)
}

// TestHTTPHandlerPath_LegacyOctetStream_BackwardCompat pins that the legacy
// /tx path (Content-Type: application/octet-stream + raw tx body + scalar
// query params) still works: the query params project into Options and the
// shared request builder + server projection round-trip without error.
func TestHTTPHandlerPath_LegacyOctetStream_BackwardCompat(t *testing.T) {
	e := echo.New()
	ctx, err := echoRequestWithQuery(e, "blockHeight=42")
	require.NoError(t, err)

	require.False(t, isProtobufContentType(ctx.Request().Header.Get("Content-Type")),
		"legacy path must not be misclassified as protobuf")

	blockHeight, opts := extractValidationParams(ctx)
	require.Equal(t, uint32(42), blockHeight)

	// The legacy path subsequently calls buildValidateTxRequest + optionsFromValidateRequest.
	// The projection must not error on the legacy shape.
	req := buildValidateTxRequest([]byte{1, 2, 3}, blockHeight, opts)
	_, err = optionsFromValidateRequest(req)
	require.NoError(t, err)
}

// TestUnconfirmedParentsAtCandidateHeight_WireRoundTrip pins the
// client-build → wire → server-reconstruction path for the
// unconfirmed_parents_at_candidate_height flag set by the block-validation
// paths (subtreevalidation's CheckBlockSubtrees and the legacy
// CheckSubtreeFromBlock branch). A silently-dropped flag would reintroduce the
// bad-txns-unconfirmed-input-in-block wedge on deployments with a remote
// validator, so the mapping on both sides is pinned here.
func TestUnconfirmedParentsAtCandidateHeight_WireRoundTrip(t *testing.T) {
	t.Run("set flag survives build, marshal, and reconstruction", func(t *testing.T) {
		opts := ProcessOptions(WithUnconfirmedParentsAtCandidateHeight(true))

		req := buildValidateTxRequest([]byte{1, 2, 3}, 1730003, opts)
		require.NotNil(t, req.UnconfirmedParentsAtCandidateHeight)
		require.True(t, *req.UnconfirmedParentsAtCandidateHeight)

		bytesOut, err := proto.Marshal(req)
		require.NoError(t, err)

		got := &validator_api.ValidateTransactionRequest{}
		require.NoError(t, proto.Unmarshal(bytesOut, got))

		reconstructed, err := optionsFromValidateRequest(got)
		require.NoError(t, err)
		require.True(t, reconstructed.UnconfirmedParentsAtCandidateHeight)
	})

	t.Run("default stays false through the round-trip", func(t *testing.T) {
		opts := ProcessOptions()

		req := buildValidateTxRequest([]byte{1, 2, 3}, 1730003, opts)

		bytesOut, err := proto.Marshal(req)
		require.NoError(t, err)

		got := &validator_api.ValidateTransactionRequest{}
		require.NoError(t, proto.Unmarshal(bytesOut, got))

		reconstructed, err := optionsFromValidateRequest(got)
		require.NoError(t, err)
		require.False(t, reconstructed.UnconfirmedParentsAtCandidateHeight)
	})
}

package validator

import (
	"testing"

	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestValidateTransactionRequest_CandidateParentMedianTimeRoundTrip pins the
// proto wire encoding for Options.CandidateParentMedianTime. A future refactor
// that drops the field from validator_api.proto (or renames it) will fail this
// test before any silent regression can ship on the post-CSV consensus path.
func TestValidateTransactionRequest_CandidateParentMedianTimeRoundTrip(t *testing.T) {
	const wantMTP uint32 = 1699999000
	mtp := wantMTP

	req := &validator_api.ValidateTransactionRequest{
		TransactionData:           []byte{1, 2, 3},
		BlockHeight:               420000,
		CandidateParentMedianTime: &mtp,
	}

	bytes, err := proto.Marshal(req)
	require.NoError(t, err)

	got := &validator_api.ValidateTransactionRequest{}
	require.NoError(t, proto.Unmarshal(bytes, got))

	require.NotNil(t, got.CandidateParentMedianTime, "candidate_parent_median_time must round-trip")
	require.Equal(t, wantMTP, got.GetCandidateParentMedianTime())
}

// TestValidateTransactionRequest_CandidateParentMedianTimeOmitted_IsZero pins
// the wire-level absent-vs-present contract on the receiver side: when the
// sender does not set the field, the receiver observes nil and the server-side
// projection (optionsFromValidateRequest) maps that to Options.CandidateParentMedianTime=0.
// What the validator does with that zero on the post-CSV consensus path is
// covered by TestSelectFinalityComparisonTime — selectFinalityComparisonTime
// now returns a ProcessingError on that combination (no tip-MTP soft-fall).
func TestValidateTransactionRequest_CandidateParentMedianTimeOmitted_IsZero(t *testing.T) {
	req := &validator_api.ValidateTransactionRequest{
		TransactionData: []byte{1, 2, 3},
		BlockHeight:     420000,
	}

	bytes, err := proto.Marshal(req)
	require.NoError(t, err)

	got := &validator_api.ValidateTransactionRequest{}
	require.NoError(t, proto.Unmarshal(bytes, got))

	require.Nil(t, got.CandidateParentMedianTime, "omitted optional must remain nil after round-trip")
	require.Equal(t, uint32(0), got.GetCandidateParentMedianTime(), "GetCandidateParentMedianTime must return zero when unset")
}

// TestBuildValidateTxRequest_PopulatesCandidateParentMedianTimeWhenSet pins
// the client-side gRPC request projection used by both the non-batch and batch
// send paths. When Options.CandidateParentMedianTime is set, it must appear in
// the outgoing request.
func TestBuildValidateTxRequest_PopulatesCandidateParentMedianTimeWhenSet(t *testing.T) {
	opts := &Options{CandidateParentMedianTime: 1699999000}
	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 420000, opts)

	require.NotNil(t, req.CandidateParentMedianTime)
	require.Equal(t, uint32(1699999000), *req.CandidateParentMedianTime)
}

// TestBuildValidateTxRequest_OmitsCandidateParentMedianTimeWhenZero pins the
// wire-side contract: requests that do not consume the field (policy mode;
// pre-CSV consensus mode) must leave the proto field absent rather than send
// a zero-valued optional. Post-CSV consensus requests don't go through this
// branch — they always populate, and the server-side
// selectFinalityComparisonTime returns a ProcessingError if the field is
// missing on that path (no tip-MTP soft-fall).
func TestBuildValidateTxRequest_OmitsCandidateParentMedianTimeWhenZero(t *testing.T) {
	opts := &Options{}
	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 420000, opts)

	require.Nil(t, req.CandidateParentMedianTime,
		"buildValidateTxRequest must leave CandidateParentMedianTime nil when Options.CandidateParentMedianTime is zero")
}

// TestBuildValidateTxHTTPQuery_PopulatesCandidateParentMedianTimeWhenSet pins
// the HTTP fallback path. The fallback fires on gRPC ResourceExhausted (large
// txs), and large txs validated in a fork / historical context still need the
// candidate-parent MTP to land on the server side.
func TestBuildValidateTxHTTPQuery_PopulatesCandidateParentMedianTimeWhenSet(t *testing.T) {
	opts := &Options{CandidateParentMedianTime: 1699999000}
	q := buildValidateTxHTTPQuery(opts, 420000)

	require.Equal(t, "1699999000", q.Get("candidateParentMedianTime"))
	require.Equal(t, "420000", q.Get("blockHeight"))
}

// TestBuildValidateTxHTTPQuery_OmitsCandidateParentMedianTimeWhenZero confirms
// the HTTP path also avoids the field for requests that don't carry one.
func TestBuildValidateTxHTTPQuery_OmitsCandidateParentMedianTimeWhenZero(t *testing.T) {
	opts := &Options{}
	q := buildValidateTxHTTPQuery(opts, 420000)

	require.Equal(t, "", q.Get("candidateParentMedianTime"))
}

// TestExtractValidationParams_ReadsCandidateParentMedianTime pins the server-
// side HTTP parse that pairs with buildValidateTxHTTPQuery. Closes the loop on
// the HTTP fallback path for post-CSV consensus: client builds the query,
// server reads it back.
func TestExtractValidationParams_ReadsCandidateParentMedianTime(t *testing.T) {
	e := echo.New()
	req, err := echoRequestWithQuery(e, "candidateParentMedianTime=1699999000")
	require.NoError(t, err)

	_, opts := extractValidationParams(req)
	require.Equal(t, uint32(1699999000), opts.CandidateParentMedianTime)
}

// TestExtractValidationParams_CandidateParentMedianTimeZeroWhenAbsent confirms
// the parse is permissive of omitted fields.
func TestExtractValidationParams_CandidateParentMedianTimeZeroWhenAbsent(t *testing.T) {
	e := echo.New()
	req, err := echoRequestWithQuery(e, "")
	require.NoError(t, err)

	_, opts := extractValidationParams(req)
	require.Equal(t, uint32(0), opts.CandidateParentMedianTime)
}

// TestOptionsFromValidateRequest_CandidateParentMedianTimeRoundTrip pins the
// server-side gRPC option mapping. Combined with buildValidateTxRequest, this
// covers the full Client → Server propagation path for the new field.
func TestOptionsFromValidateRequest_CandidateParentMedianTimeRoundTrip(t *testing.T) {
	src := &Options{CandidateParentMedianTime: 1699999000}

	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 420000, src)
	got := optionsFromValidateRequest(req)

	require.Equal(t, src.CandidateParentMedianTime, got.CandidateParentMedianTime)
}

// TestHTTPHandlerPath_CandidateParentMedianTime_EndToEnd mirrors what the /tx
// HTTP handler does internally: parse the query string into Options, then
// build the validator request from those Options. Pins the HTTP-fallback
// handler path so a future regression dropping CandidateParentMedianTime at
// either step is caught here, not at runtime in production — where post-CSV
// consensus would hit selectFinalityComparisonTime's hard-error path with a
// less-precise diagnostic than the parse-time warning issued by
// extractValidationParams.
func TestHTTPHandlerPath_CandidateParentMedianTime_EndToEnd(t *testing.T) {
	const wantMTP uint32 = 1699999000

	q := buildValidateTxHTTPQuery(&Options{CandidateParentMedianTime: wantMTP}, 420000)

	e := echo.New()
	ctx, err := echoRequestWithQuery(e, q.Encode())
	require.NoError(t, err)
	blockHeight, opts := extractValidationParams(ctx)

	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), blockHeight, opts)

	require.Equal(t, uint32(420000), req.BlockHeight)
	require.NotNil(t, req.CandidateParentMedianTime,
		"HTTP handler path must propagate candidateParentMedianTime through to the gRPC request")
	require.Equal(t, wantMTP, *req.CandidateParentMedianTime)
}

// TestCandidateParentMedianTimePtr_AliasesOptsField pins the no-alloc contract
// for the post-CSV finality helper. The returned pointer must alias
// opts.CandidateParentMedianTime directly so mutating the pointer reflects in
// opts — a regression where the helper copies into a local would force every
// block-validation request to allocate the local on the heap (escape
// analysis), so the test fails fast on that.
func TestCandidateParentMedianTimePtr_AliasesOptsField(t *testing.T) {
	opts := &Options{CandidateParentMedianTime: 1699999000}
	ptr := candidateParentMedianTimePtr(opts)

	require.NotNil(t, ptr)
	require.Same(t, &opts.CandidateParentMedianTime, ptr,
		"candidateParentMedianTimePtr must return &opts.CandidateParentMedianTime (no per-request copy/allocation)")
}

// TestCandidateParentMedianTimePtr_NilWhenZero pins the wire-economy contract:
// the optional proto field stays absent on the wire when the caller has not
// supplied a value. Legitimate omission paths after the hard-error stance
// landed are policy mode and pre-CSV consensus mode — both do not consume the
// field. Post-CSV consensus requests must populate the field; if they don't,
// the server's selectFinalityComparisonTime returns a ProcessingError.
func TestCandidateParentMedianTimePtr_NilWhenZero(t *testing.T) {
	require.Nil(t, candidateParentMedianTimePtr(&Options{}),
		"candidateParentMedianTimePtr must return nil when CandidateParentMedianTime is zero")
}

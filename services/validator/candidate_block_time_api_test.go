package validator

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestValidateTransactionRequest_CandidateBlockTimeRoundTrip pins the proto
// wire encoding for Options.CandidateBlockTime. A future refactor that drops
// the field from validator_api.proto (or renames it) will fail this test
// before any silent regression can ship.
func TestValidateTransactionRequest_CandidateBlockTimeRoundTrip(t *testing.T) {
	const wantCBT uint32 = 1234567890
	cbt := wantCBT

	req := &validator_api.ValidateTransactionRequest{
		TransactionData:    []byte{1, 2, 3},
		BlockHeight:        42,
		CandidateBlockTime: &cbt,
	}

	bytes, err := proto.Marshal(req)
	require.NoError(t, err)

	got := &validator_api.ValidateTransactionRequest{}
	require.NoError(t, proto.Unmarshal(bytes, got))

	require.NotNil(t, got.CandidateBlockTime, "candidate_block_time must round-trip")
	require.Equal(t, wantCBT, got.GetCandidateBlockTime())
}

// TestValidateTransactionRequest_CandidateBlockTimeOmitted_IsZero pins the
// soft-fall contract on the receiver side: when the sender does not set the
// field, the receiver observes nil (which the server maps to 0, which the
// validator skips on the pre-CSV consensus path).
func TestValidateTransactionRequest_CandidateBlockTimeOmitted_IsZero(t *testing.T) {
	req := &validator_api.ValidateTransactionRequest{
		TransactionData: []byte{1, 2, 3},
		BlockHeight:     42,
	}

	bytes, err := proto.Marshal(req)
	require.NoError(t, err)

	got := &validator_api.ValidateTransactionRequest{}
	require.NoError(t, proto.Unmarshal(bytes, got))

	require.Nil(t, got.CandidateBlockTime, "omitted optional must remain nil after round-trip")
	require.Equal(t, uint32(0), got.GetCandidateBlockTime(), "GetCandidateBlockTime must return zero when unset")
}

// minimal helper to build a tx for the request-builder tests. We don't care
// about the content, just that buildValidateTxRequest projects the fields.
func newTinyTx(t *testing.T) *bt.Tx {
	t.Helper()
	return bt.NewTx()
}

// TestBuildValidateTxRequest_PopulatesCandidateBlockTimeWhenSet pins the
// client-side gRPC request projection used by both the non-batch and batch
// send paths. When Options.CandidateBlockTime is set, it must appear in the
// outgoing request.
func TestBuildValidateTxRequest_PopulatesCandidateBlockTimeWhenSet(t *testing.T) {
	opts := &Options{CandidateBlockTime: 1700000000}
	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 42, opts)

	require.NotNil(t, req.CandidateBlockTime)
	require.Equal(t, uint32(1700000000), *req.CandidateBlockTime)
}

// TestBuildValidateTxRequest_OmitsCandidateBlockTimeWhenZero pins the wire
// economy: policy-mode requests (which never carry a candidate block time)
// must leave the proto field absent rather than send a zero-valued optional.
func TestBuildValidateTxRequest_OmitsCandidateBlockTimeWhenZero(t *testing.T) {
	opts := &Options{}
	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 42, opts)

	require.Nil(t, req.CandidateBlockTime,
		"buildValidateTxRequest must leave CandidateBlockTime nil when Options.CandidateBlockTime is zero")
}

// TestBuildValidateTxHTTPQuery_PopulatesCandidateBlockTimeWhenSet pins the
// HTTP fallback path. The fallback fires on gRPC ResourceExhausted (large
// txs), and these are exactly the transactions most likely to need
// block-validation finality. The query string must carry candidateBlockTime.
func TestBuildValidateTxHTTPQuery_PopulatesCandidateBlockTimeWhenSet(t *testing.T) {
	opts := &Options{CandidateBlockTime: 1700000000}
	q := buildValidateTxHTTPQuery(opts, 42)

	require.Equal(t, "1700000000", q.Get("candidateBlockTime"))
	require.Equal(t, "42", q.Get("blockHeight"))
}

// TestBuildValidateTxHTTPQuery_OmitsCandidateBlockTimeWhenZero confirms the
// HTTP path also avoids the field for policy-mode requests.
func TestBuildValidateTxHTTPQuery_OmitsCandidateBlockTimeWhenZero(t *testing.T) {
	opts := &Options{}
	q := buildValidateTxHTTPQuery(opts, 42)

	require.Equal(t, "", q.Get("candidateBlockTime"))
}

// TestExtractValidationParams_ReadsCandidateBlockTime pins the server-side
// HTTP parse that pairs with buildValidateTxHTTPQuery. Closes the loop on the
// HTTP fallback path: client builds the query, server reads it back.
func TestExtractValidationParams_ReadsCandidateBlockTime(t *testing.T) {
	e := echo.New()
	req, err := echoRequestWithQuery(e, "candidateBlockTime=1700000000")
	require.NoError(t, err)

	_, opts := extractValidationParams(req)
	require.Equal(t, uint32(1700000000), opts.CandidateBlockTime)
}

// TestExtractValidationParams_ZeroWhenAbsent confirms the parse is
// permissive of omitted fields.
func TestExtractValidationParams_ZeroWhenAbsent(t *testing.T) {
	e := echo.New()
	req, err := echoRequestWithQuery(e, "")
	require.NoError(t, err)

	_, opts := extractValidationParams(req)
	require.Equal(t, uint32(0), opts.CandidateBlockTime)
}

// TestOptionsFromValidateRequest_RoundTrip pins the server-side gRPC option
// mapping. Combined with buildValidateTxRequest, it covers the full Client →
// Server propagation path: build the request from Options, project the
// request back into Options, expect equality on every field we care about
// — including CandidateBlockTime.
func TestOptionsFromValidateRequest_RoundTrip(t *testing.T) {
	src := &Options{
		SkipUtxoCreation:          true,
		AddTXToBlockAssembly:      false,
		SkipPolicyChecks:          true,
		CreateConflicting:         true,
		SkipTxMetaPublishing:      true,
		CandidateBlockTime:        1700000000,
		CandidateParentMedianTime: 1699999000,
	}

	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 42, src)
	got, err := optionsFromValidateRequest(req)
	require.NoError(t, err)

	require.Equal(t, src.SkipUtxoCreation, got.SkipUtxoCreation)
	require.Equal(t, src.AddTXToBlockAssembly, got.AddTXToBlockAssembly)
	require.Equal(t, src.SkipPolicyChecks, got.SkipPolicyChecks)
	require.Equal(t, src.CreateConflicting, got.CreateConflicting)
	require.Equal(t, src.SkipTxMetaPublishing, got.SkipTxMetaPublishing)
	require.Equal(t, src.CandidateBlockTime, got.CandidateBlockTime)
	require.Equal(t, src.CandidateParentMedianTime, got.CandidateParentMedianTime)
}

// TestOptionsFromValidateRequest_OmittedFieldsStayZero pins that omitted
// proto fields project to zero on the server side. What the validator then
// does with those zeros depends on the era / mode and is covered by
// TestSelectFinalityComparisonTime: policy mode uses tip MTP; pre-CSV
// consensus with CandidateBlockTime=0 returns skipFinality; post-CSV
// consensus with CandidateParentMedianTime=0 hard-errors (no tip-MTP
// soft-fall).
func TestOptionsFromValidateRequest_OmittedFieldsStayZero(t *testing.T) {
	src := &Options{}

	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 42, src)
	require.Nil(t, req.CandidateBlockTime)
	require.Nil(t, req.CandidateParentMedianTime)

	got, err := optionsFromValidateRequest(req)
	require.NoError(t, err)
	require.Equal(t, uint32(0), got.CandidateBlockTime)
	require.Equal(t, uint32(0), got.CandidateParentMedianTime)
}

// TestHTTPHandlerPath_CandidateBlockTime_EndToEnd mirrors what the /tx HTTP
// handler does internally: parse the query string into Options, then build
// the validator request from those Options. This pins the *handler* path
// (handleSingleTx / handleMultipleTx) — a regression where one of those
// handlers reverts to inline struct-literal request building (dropping
// CandidateBlockTime as a recent regression did) is caught here, not at
// runtime in production where pre-CSV finality would silently skip.
func TestHTTPHandlerPath_CandidateBlockTime_EndToEnd(t *testing.T) {
	const wantCBT uint32 = 1700000000

	// Build the same query the HTTP fallback client emits.
	q := buildValidateTxHTTPQuery(&Options{CandidateBlockTime: wantCBT}, 42)

	// Hand it to the same parser handleSingleTx/handleMultipleTx use.
	e := echo.New()
	ctx, err := echoRequestWithQuery(e, q.Encode())
	require.NoError(t, err)
	blockHeight, opts := extractValidationParams(ctx)

	// Then build the same gRPC request the handlers feed into the validator.
	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), blockHeight, opts)

	require.Equal(t, uint32(42), req.BlockHeight)
	require.NotNil(t, req.CandidateBlockTime,
		"HTTP handler path must propagate candidateBlockTime through to the gRPC request")
	require.Equal(t, wantCBT, *req.CandidateBlockTime)
}

// TestCandidateBlockTimePtr_AliasesOptsField pins the no-alloc contract: the
// returned pointer must alias opts.CandidateBlockTime directly (not a copy),
// so mutating the pointer reflects in opts. A regression where the helper
// copies into a local would force every block-validation request to allocate
// the local on the heap (escape analysis), so the test fails fast on that.
func TestCandidateBlockTimePtr_AliasesOptsField(t *testing.T) {
	opts := &Options{CandidateBlockTime: 1700000000}
	ptr := candidateBlockTimePtr(opts)

	require.NotNil(t, ptr)
	require.Same(t, &opts.CandidateBlockTime, ptr,
		"candidateBlockTimePtr must return &opts.CandidateBlockTime (no per-request copy/allocation)")
}

// echoRequestWithQuery builds a minimal echo.Context backed by an HTTP
// request with the given query string. Used to drive extractValidationParams
// without standing up a full HTTP server.
func echoRequestWithQuery(e *echo.Echo, query string) (echo.Context, error) {
	u, err := url.Parse("/tx?" + query)
	if err != nil {
		return nil, err
	}

	httpReq := httptest.NewRequest(http.MethodPost, u.String(), strings.NewReader(""))
	rec := httptest.NewRecorder()
	return e.NewContext(httpReq, rec), nil
}

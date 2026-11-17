package validator

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
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

// The query-build and query-parse cases that used to live here are gone: the
// /tx endpoint no longer reads validation options from the query string, so
// there is no HTTP projection of candidateBlockTime to pin on either side. The
// gRPC projection below is the surviving wire.

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

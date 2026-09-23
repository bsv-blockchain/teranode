package validator

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// recordingValidator captures the block height and Options the server hands to
// the validator. MockValidator.ValidateFunc does not receive either, and the
// server calls ValidateWithOptions, so the recorder overrides that method and
// delegates to the embedded mock for the return value: validateTransaction
// serialises the metadata before replying 200, so a nil return would make every
// expected-200 assertion in this file unreachable for the wrong reason.
type recordingValidator struct {
	MockValidator

	calls       int
	lastHeight  uint32
	lastOptions *Options
	lastTx      *bt.Tx
}

func (r *recordingValidator) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, o *Options) (*meta.Data, error) {
	r.calls++
	r.lastHeight = blockHeight
	r.lastOptions = o
	r.lastTx = tx

	return r.MockValidator.ValidateWithOptions(ctx, tx, blockHeight, o)
}

// newTrustFlagServer builds a server through the real constructor, which is what
// populates settings and stats — a zero-value Server{} panics in the handlers —
// and injects the recorder in place of the validator.
//
// The assignment is direct, NOT via SetValidatorForTesting: that helper sets the
// field through reflection, and reflect.Value.CanSet is false for a value reached
// through an unexported field whatever package the caller is in, so its guarded
// Set never runs and the field stays nil. These tests are in-package, so plain
// assignment works — the same thing every other test in this package does.
func newTrustFlagServer(t *testing.T) (*Server, *recordingValidator) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	srv := NewServer(ulogger.TestLogger{}, tSettings, nil, nil, nil, nil, nil, nil, nil)

	rec := &recordingValidator{}
	srv.validator = rec

	return srv, rec
}

// postToHandler drives an echo handler with a POST carrying the given path (with
// query string), Content-Type and body, and returns the recorder.
func postToHandler(t *testing.T, handler echo.HandlerFunc, target, contentType string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	httpReq := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(body))
	if contentType != "" {
		httpReq.Header.Set("Content-Type", contentType)
	}

	rec := httptest.NewRecorder()
	require.NoError(t, handler(echo.New().NewContext(httpReq, rec)))

	return rec
}

// attackQueryString is the query string from step 3 of the reported attack, plus
// addTxToBlockAssembly=false for completeness. Every parameter in it used to be
// honoured; none of them is read any more.
const attackQueryString = "blockHeight=620000&skipScriptValidation=true&outpointOnlySpend=true" +
	"&skipPolicyChecks=true&addTxToBlockAssembly=false"

// TestHandleSingleTx_QueryStringFlagsAreNotHonoured pins the core of the fix: a
// POST to /tx carrying the attack's query string on the legacy octet-stream body
// shape is validated at height 0 with default options. Height 0 is what forces
// the validator to derive the height from its own chain state, which is what makes
// the existing above-checkpoint guard bite instead of being satisfied by a
// caller-chosen value.
func TestHandleSingleTx_QueryStringFlagsAreNotHonoured(t *testing.T) {
	srv, rec := newTrustFlagServer(t)
	handler := srv.HTTP().HandleSingleTx(context.Background())

	res := postToHandler(t, handler, "/tx?"+attackQueryString,
		"application/octet-stream", newTinyTx(t).SerializeBytes())

	require.Equal(t, http.StatusOK, res.Code, "body: %s", res.Body.String())
	require.Equal(t, 1, rec.calls)
	require.Equal(t, uint32(0), rec.lastHeight,
		"the query string must not be able to nominate a block height")
	require.Equal(t, NewDefaultOptions(), rec.lastOptions,
		"the query string must not be able to select validation options")
}

// TestHandleMultipleTx_QueryStringFlagsAreNotHonoured is the same assertion on the
// second handler: /txs also stops reading the query string, for every transaction
// in the stream.
func TestHandleMultipleTx_QueryStringFlagsAreNotHonoured(t *testing.T) {
	srv, rec := newTrustFlagServer(t)
	handler := srv.HTTP().HandleMultipleTx(context.Background())

	txBytes := newTinyTx(t).SerializeBytes()

	var body bytes.Buffer
	body.Write(txBytes)
	body.Write(txBytes)

	res := postToHandler(t, handler, "/txs?"+attackQueryString,
		"application/octet-stream", body.Bytes())

	require.Equal(t, http.StatusOK, res.Code, "body: %s", res.Body.String())
	require.Equal(t, 2, rec.calls, "both transactions in the stream must be validated")
	require.Equal(t, uint32(0), rec.lastHeight)
	require.Equal(t, NewDefaultOptions(), rec.lastOptions)
}

// uint32Ptr is the uint32 twin of Client_test.go's boolPtr, for the optional
// candidate-time fields.
func uint32Ptr(v uint32) *uint32 { return &v }

// TestHandleSingleTx_ProtobufNonDefaultOptionsRejected covers the body shape the
// query-string deletion does not reach: a full protobuf ValidateTransactionRequest
// posted straight at the unauthenticated endpoint. Every field that can change
// validation behaviour gets its own row, so an omitted branch in
// nonDefaultValidationOptions fails a specific row rather than hiding behind a
// sibling field.
func TestHandleSingleTx_ProtobufNonDefaultOptionsRejected(t *testing.T) {
	txBytes := newTinyTx(t).SerializeBytes()

	cases := []struct {
		name   string
		mutate func(*validator_api.ValidateTransactionRequest)
		reason string
	}{
		{"blockHeight", func(r *validator_api.ValidateTransactionRequest) { r.BlockHeight = 620000 }, "blockHeight"},
		{"skipUtxoCreation", func(r *validator_api.ValidateTransactionRequest) { r.SkipUtxoCreation = boolPtr(true) }, "skipUtxoCreation"},
		{"addTxToBlockAssembly", func(r *validator_api.ValidateTransactionRequest) { r.AddTxToBlockAssembly = boolPtr(false) }, "addTxToBlockAssembly=false"},
		{"skipPolicyChecks", func(r *validator_api.ValidateTransactionRequest) { r.SkipPolicyChecks = boolPtr(true) }, "skipPolicyChecks"},
		{"createConflicting", func(r *validator_api.ValidateTransactionRequest) { r.CreateConflicting = boolPtr(true) }, "createConflicting"},
		{"skipTxmetaPublishing", func(r *validator_api.ValidateTransactionRequest) { r.SkipTxmetaPublishing = boolPtr(true) }, "skipTxmetaPublishing"},
		{"inBlock", func(r *validator_api.ValidateTransactionRequest) { r.InBlock = boolPtr(true) }, "inBlock"},
		{"candidateBlockTime", func(r *validator_api.ValidateTransactionRequest) { r.CandidateBlockTime = uint32Ptr(1700000000) }, "candidateBlockTime"},
		{"candidateParentMedianTime", func(r *validator_api.ValidateTransactionRequest) { r.CandidateParentMedianTime = uint32Ptr(1700000000) }, "candidateParentMedianTime"},
		{"unconfirmedParentsAtCandidateHeight", func(r *validator_api.ValidateTransactionRequest) {
			r.UnconfirmedParentsAtCandidateHeight = boolPtr(true)
		}, "unconfirmedParentsAtCandidateHeight"},
		{"skipScriptValidation", func(r *validator_api.ValidateTransactionRequest) { r.SkipScriptValidation = boolPtr(true) }, "skipScriptValidation"},
		{"outpointOnlySpend", func(r *validator_api.ValidateTransactionRequest) { r.OutpointOnlySpend = boolPtr(true) }, "outpointOnlySpend"},
		{"ignorePolicyFreeze", func(r *validator_api.ValidateTransactionRequest) { r.IgnorePolicyFreeze = boolPtr(true) }, "ignorePolicyFreeze"},
		{"ignoreConsensusFreeze", func(r *validator_api.ValidateTransactionRequest) { r.IgnoreConsensusFreeze = boolPtr(true) }, "ignoreConsensusFreeze"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, rec := newTrustFlagServer(t)
			handler := srv.HTTP().HandleSingleTx(context.Background())

			req := &validator_api.ValidateTransactionRequest{TransactionData: txBytes}
			tc.mutate(req)

			body, err := proto.Marshal(req)
			require.NoError(t, err)

			res := postToHandler(t, handler, "/tx", "application/x-protobuf", body)

			require.Equal(t, http.StatusBadRequest, res.Code,
				"a non-default %s must be refused, not honoured and not stripped", tc.name)
			require.Contains(t, res.Body.String(), tc.reason,
				"the 400 must name the offending field so the caller can act on it")
			require.Equal(t, 0, rec.calls,
				"the transaction must never reach the validator once the request is refused")
		})
	}
}

// TestHandleSingleTx_ProtobufDefaultOptionsAccepted is the other half of the
// contract: the ordinary large-transaction fallback still works. It matters because
// buildValidateTxRequest emits a non-nil pointer for every bool field, so a guard
// that filtered on field presence instead of field value would reject this.
func TestHandleSingleTx_ProtobufDefaultOptionsAccepted(t *testing.T) {
	srv, rec := newTrustFlagServer(t)
	handler := srv.HTTP().HandleSingleTx(context.Background())

	body, err := proto.Marshal(buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 0, NewDefaultOptions()))
	require.NoError(t, err)

	res := postToHandler(t, handler, "/tx", "application/x-protobuf", body)

	require.Equal(t, http.StatusOK, res.Code, "body: %s", res.Body.String())
	require.Equal(t, 1, rec.calls)
	require.Equal(t, uint32(0), rec.lastHeight)
	require.Equal(t, NewDefaultOptions(), rec.lastOptions)
}

// TestHandleMultipleTx_HasNoProtobufShape is the structural counterpart to the
// guard on /tx. handleMultipleTx never calls nonDefaultValidationOptions, which is
// correct only because /txs has no body shape that could carry an option at all.
// Nothing in the code says so, so these two subtests do: the handler reads no
// Content-Type, and a protobuf body delivers no option and no asserted height.
func TestHandleMultipleTx_HasNoProtobufShape(t *testing.T) {
	t.Run("content type does not change the outcome", func(t *testing.T) {
		txBytes := newTinyTx(t).SerializeBytes()

		var body bytes.Buffer
		body.Write(txBytes)
		body.Write(txBytes)

		// One body, two independently built servers, two Content-Types. Any
		// divergence means /txs has grown a body shape discriminated on the header.
		post := func(contentType string) (*httptest.ResponseRecorder, *recordingValidator) {
			srv, rec := newTrustFlagServer(t)

			return postToHandler(t, srv.HTTP().HandleMultipleTx(context.Background()),
				"/txs", contentType, body.Bytes()), rec
		}

		plainRes, plainRec := post("application/octet-stream")
		protoRes, protoRec := post("application/x-protobuf")

		require.Equal(t, plainRes.Code, protoRes.Code,
			"handleMultipleTx must not branch on Content-Type; plain body: %s, protobuf body: %s",
			plainRes.Body.String(), protoRes.Body.String())
		require.Equal(t, plainRec.calls, protoRec.calls)
		require.Equal(t, plainRec.lastHeight, protoRec.lastHeight)
		require.Equal(t, plainRec.lastOptions, protoRec.lastOptions)
	})

	t.Run("a protobuf body cannot deliver options", func(t *testing.T) {
		srv, rec := newTrustFlagServer(t)

		opts := NewDefaultOptions()
		opts.SkipPolicyChecks = true
		opts.SkipScriptValidation = true

		body, err := proto.Marshal(buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 620000, opts))
		require.NoError(t, err)

		postToHandler(t, srv.HTTP().HandleMultipleTx(context.Background()),
			"/txs", "application/x-protobuf", body)

		// Deliberately no status-code or call-count assertion. handleMultipleTx
		// hands the body straight to bt.Tx.ReadFrom, which reads the leading
		// protobuf framing as transaction fields: the field-1 tag 0x0a and a length
		// varint, then the transaction's own version bytes, after which a 0x00 is
		// taken as the input count, a second 0x00 as the output count, and four more
		// bytes as a lock time that is not the 0xEF extended-format marker. The read
		// SUCCEEDS with a zero-input, zero-output transaction, so the handler
		// validates it and the final status depends on where the remaining bytes stop
		// parsing. Pinning that would pin an accident. What matters is the projection.
		if rec.calls > 0 {
			require.Equal(t, uint32(0), rec.lastHeight,
				"a protobuf body must not be able to nominate a block height on /txs")
			require.Equal(t, NewDefaultOptions(), rec.lastOptions,
				"a protobuf body must not be able to select validation options on /txs")
		}
	})
}

package propagation

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// The public rejection-reason contract for POST /tx and POST /txs.
//
// A client that submits a transaction and is told it failed must be able to
// answer two questions from the response alone: WHICH transaction failed, and
// WHY. Neither is guaranteed by construction.
//
// "Which" is not, because errors.UserMessage surfaces the innermost cause on
// publicCauseCodes and discards everything outside it — including the
// "[ProcessTransaction][<txid>]" wrapper this package adds. Precisely the
// failures with the most useful messages therefore arrive anonymous, and a
// client batching transactions cannot tell which of them the verdict is about.
// It has to guess, requeue everything, or — as observed downstream — attribute
// the verdict to the wrong transaction.
//
// "Why" is not, because a cause that is not a *errors.Error (every bdkscript
// failure) can never be selected, and a cause whose code is off the allowlist
// collapses to the outermost PROCESSING wrapper.
//
// These tests pin both, per failure class, through the real handlers.

// foreignError is an error from outside the errors package — a stdlib or
// third-party one, which is what reaches the boundary when a dependency fails
// before Teranode has wrapped it. errors.PublicError has no code to report for
// one of these.
type foreignError string

func (e foreignError) Error() string { return string(e) }

// failureClass is one validation outcome as the validator would return it.
type failureClass struct {
	name string
	// err is what validator.Validate returns; processTransactionInternal wraps
	// it exactly as it does in production.
	err error
	// wantStatus is the HTTP status the handler must derive from it.
	wantStatus int
	// wantReason is a cause-specific token the response body must carry, or
	// empty for the classes that are deliberately opaque (node faults).
	//
	// No class here names a transaction in its own message, so the txid can
	// only be present because the handler put it there.
	wantReason string
}

func failureClasses() []failureClass {
	return []failureClass{
		{
			name:       "go-side consensus check",
			err:        errors.NewTxInvalidError("bad-txns-in-belowout"),
			wantStatus: http.StatusBadRequest,
			wantReason: "bad-txns-in-belowout",
		},
		{
			name:       "policy check",
			err:        errors.NewTxPolicyError("bad-txns-inputs-too-large"),
			wantStatus: http.StatusBadRequest,
			wantReason: "bad-txns-inputs-too-large",
		},
		{
			// The chain ScriptVerifierGoBDK builds: this node's constant on the
			// outside, the engine's own verdict as the innermost typed link.
			name: "script evaluation failure",
			err: errors.NewTxInvalidError("GoBDK fail to ValidateTransaction",
				errors.NewTxInvalidError("Script evaluated without error but finished with a false/empty top stack element")),
			wantStatus: http.StatusBadRequest,
			wantReason: "false/empty top stack element",
		},
		{
			name: "fee below the floor",
			err: errors.NewTxInvalidError("GoBDK fail to ValidateTransaction",
				errors.NewTxPolicyError("transaction fee is too low",
					errors.NewTxPolicyError("insufficient-fee"))),
			wantStatus: http.StatusBadRequest,
			wantReason: "insufficient-fee",
		},
		{
			name:       "non-final",
			err:        errors.NewUtxoNonFinalError("transaction is not final", errors.NewTxLockTimeError("lock time (1783806110) is not less than median block time (1783805456)")),
			wantStatus: http.StatusBadRequest,
			wantReason: "median block time",
		},
		{
			name:       "missing parent",
			err:        errors.NewTxMissingParentError("error getting parent transaction %s", strings.Repeat("a", 64), errors.ErrTxNotFound),
			wantStatus: http.StatusUnprocessableEntity,
			wantReason: "error getting parent transaction",
		},
		{
			name:       "conflicting",
			err:        errors.NewTxConflictingError("tx is conflicting"),
			wantStatus: http.StatusConflict,
			wantReason: "tx is conflicting",
		},
		{
			name: "node fault",
			err:  errors.NewStorageError("utxo store unavailable"),
			// A fault in this node, not a verdict: no detail is surfaced, and
			// the 5xx is what tells the client to retry rather than give up.
			wantStatus: http.StatusInternalServerError,
		},
	}
}

// TestTxsFailureLinesNameTheirTransaction is the "which" half. Every failure
// line in a POST /txs body must carry the id of the transaction it is about,
// whatever the public error boundary did to the message.
func TestTxsFailureLinesNameTheirTransaction(t *testing.T) {
	for _, fc := range failureClasses() {
		t.Run(fc.name, func(t *testing.T) {
			tx := createRobustTestTx(t)
			ps, _ := setupPropagationServer(t, NewMockValidatorForTxTest(fc.err), nil)

			body := bytes.NewBuffer(tx.ExtendedBytes())
			rec := httptest.NewRecorder()
			c := echo.New().NewContext(httptest.NewRequest(http.MethodPost, "/txs", body), rec)
			require.NoError(t, ps.handleMultipleTx(context.Background())(c))

			require.Equal(t, fc.wantStatus, rec.Code, "status derived from the error class")
			got := rec.Body.String()

			require.Contains(t, got, tx.TxID(),
				"failure line does not name the transaction it is about; a client batching "+
					"transactions cannot tell which one this verdict belongs to\nbody: %s", got)

			if fc.wantReason != "" {
				require.Contains(t, got, fc.wantReason,
					"the cause did not survive the public error boundary\nbody: %s", got)
			}

			// The code must stay at the head of the line: consumers read it
			// there to classify the verdict before reading anything else.
			for _, line := range failureBodyLines(t, got) {
				require.Regexp(t, `^[A-Z_]+ \(\d+\): `, line,
					"failure line must start with the Teranode code")
			}
		})
	}
}

// TestSingleTxFailureNamesItsTransaction is the same guarantee on POST /tx.
// One transaction per request makes the id less critical, but a client
// correlating asynchronous responses, or reading logs, still needs it — and
// the two surfaces should not disagree about what a failure looks like.
func TestSingleTxFailureNamesItsTransaction(t *testing.T) {
	for _, fc := range failureClasses() {
		t.Run(fc.name, func(t *testing.T) {
			tx := createRobustTestTx(t)
			ps, _ := setupPropagationServer(t, NewMockValidatorForTxTest(fc.err), nil)

			body := bytes.NewBuffer(tx.ExtendedBytes())
			rec := httptest.NewRecorder()
			c := echo.New().NewContext(httptest.NewRequest(http.MethodPost, "/tx", body), rec)
			require.NoError(t, ps.handleSingleTx(context.Background())(c))

			require.Equal(t, fc.wantStatus, rec.Code)
			require.Contains(t, rec.Body.String(), tx.TxID(),
				"single-tx failure must name its transaction\nbody: %s", rec.Body.String())
		})
	}
}

// TestSingleTxUnparseableBodyIsAnswered pins the one path where there is no
// transaction to name: a body the parser rejects.
//
// The handler must answer it rather than fall over. Naming the transaction in
// a failure needs the parsed transaction, and the obvious way to get one in
// the handler — parsing the body again on the failure path — would re-run the
// parser on exactly the input that just failed it, unguarded. bt's parser is
// recovered from a panic everywhere else in this file precisely because
// adversarial input can panic it, and this echo server registers no
// middleware.Recover, so such a panic would unwind into net/http and drop the
// connection instead of returning a status. processTransaction hands the
// parsed transaction back instead, so the body is parsed exactly once, inside
// the existing guard, and a nil result simply means "nothing to name".
func TestSingleTxUnparseableBodyIsAnswered(t *testing.T) {
	ps, _ := setupPropagationServer(t, NewMockValidatorForTxTest(nil), nil)

	rec := httptest.NewRecorder()
	c := echo.New().NewContext(
		httptest.NewRequest(http.MethodPost, "/tx", bytes.NewReader([]byte{0x01, 0x02, 0x03})), rec)

	require.NotPanics(t, func() {
		require.NoError(t, ps.handleSingleTx(context.Background())(c))
	})
	require.Contains(t, rec.Body.String(), "failed to parse transaction from bytes")
}

// TestFailureLine covers the renderer directly, including the cases the
// handler tests cannot reach.
func TestFailureLine(t *testing.T) {
	tx := createRobustTestTx(t)
	txid := tx.TxID()

	t.Run("injects the txid after the code", func(t *testing.T) {
		got := failureLine(tx, errors.NewTxInvalidError("bad-txns-in-belowout"))
		require.Equal(t, "TX_INVALID (31): [ProcessTransaction]["+txid+"] bad-txns-in-belowout", got)
	})

	t.Run("does not name it twice", func(t *testing.T) {
		err := errors.NewTxInvalidError("[ProcessTransaction][%s] received transaction with no outputs", txid)
		got := failureLine(tx, err)
		require.Equal(t, 1, strings.Count(got, txid), "txid must appear exactly once: %s", got)
	})

	t.Run("nil transaction is rendered unchanged", func(t *testing.T) {
		err := errors.NewProcessingError("[ProcessTransaction] failed to parse transaction from bytes")
		require.Equal(t, errors.UserMessage(err), failureLine(nil, err))
	})

	t.Run("a foreign error still leads with a code", func(t *testing.T) {
		// errors.PublicError has no code to report for an error that is not a
		// *errors.Error, and falls back to the bare literal "internal error".
		// Scanning the message for the "CODE (n): " prefix and inserting the
		// txid after it produced a line with no code on it at all, breaking the
		// invariant every other failure line holds. Rendering through the
		// errors package instead makes the prefix structural.
		got := failureLine(tx, foreignError("connection reset by peer"))
		require.Regexp(t, `^[A-Z_]+ \(\d+\): `, got)
		require.Contains(t, got, txid, "the transaction must still be named: %s", got)
	})

	t.Run("nil error renders nothing", func(t *testing.T) {
		require.Empty(t, failureLine(tx, nil))
	})
}

// TestPublicCauseAllowlist_MissingParent pins the allowlist decision that makes
// an out-of-order child distinguishable from a permanent rejection. Collapsing
// it to PROCESSING made the two identical on the wire, and a client that
// broadcasts a transaction chain across several requests cannot tell "your
// parent has not arrived yet, resubmit" from "these bytes will never be
// accepted".
func TestPublicCauseAllowlist_MissingParent(t *testing.T) {
	const txid = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	chain := errors.NewProcessingError("[ProcessTransaction][%s] failed to validate transaction", txid,
		errors.NewProcessingError("[Validate][%s] error getting transaction input block heights", txid,
			errors.NewTxMissingParentError("[Validate][%s] error getting parent transaction %s", txid, strings.Repeat("a", 64))))

	got := errors.UserMessage(chain)
	require.Contains(t, got, "TX_MISSING_PARENT", "the missing-parent cause must reach the client: %s", got)
	require.Contains(t, got, "error getting parent transaction", "the parent must be named: %s", got)
}

// failureBodyLines strips the header from a /txs failure body and returns the
// per-transaction lines.
func failureBodyLines(t *testing.T, body string) []string {
	t.Helper()
	trimmed := strings.TrimRight(body, "\n")
	lines := strings.Split(trimmed, "\n")
	require.NotEmpty(t, lines)
	require.Equal(t, "Failed to process transactions:", lines[0], "unexpected body shape: %s", body)
	return lines[1:]
}

// TestLargeTxFallbackSurfacesTheVerdict covers the one path on which a
// Kafka-wired node — every .operator deployment — answers a submitter with a
// real verdict.
//
// Everything else in this file exercises the Kafka-less path. With Kafka
// configured, processTransactionInternal publishes and returns nil, so a normal
// transaction is answered 200 OK before the validator has looked at it; only a
// transaction above KafkaMaxMessageBytes is validated synchronously, over HTTP.
// That fallback used to wrap whatever came back as SERVICE_ERROR, which is off
// publicCauseCodes: a permanently invalid transaction was reported as a
// retryable 500, and the validator's rendered error chain went out in the body.
func TestLargeTxFallbackSurfacesTheVerdict(t *testing.T) {
	// A verdict as the validator produces it: the client-safe reason buried
	// under a wrapper that names node-internal state.
	verdict := errors.NewProcessingError("[Validate][deadbeef] /home/build/services/validator/Validator.go:812 failed",
		errors.NewTxPolicyError("insufficient-fee"))

	t.Run("verdict header is preferred over the body", func(t *testing.T) {
		tx, rec := runLargeTxFallback(t, func(w http.ResponseWriter) {
			errors.AttachHTTPError(w.Header(), verdict)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("[handleSingleTx] Failed to process transaction: " + verdict.Error()))
		})

		got := rec.Body.String()
		require.Equal(t, http.StatusBadRequest, rec.Code,
			"a permanently invalid transaction must not be reported as retryable\nbody: %s", got)
		require.Contains(t, got, "insufficient-fee", "the verdict must reach the client\nbody: %s", got)
		require.Contains(t, got, tx.TxID(), "the failure must name its transaction\nbody: %s", got)
		require.Contains(t, got, "TX_POLICY", "the code must survive the hop\nbody: %s", got)
		require.NotContains(t, got, "Validator.go:812",
			"the validator's internal chain must not reach a client\nbody: %s", got)
	})

	t.Run("without the header the previous wrapping stands", func(t *testing.T) {
		_, rec := runLargeTxFallback(t, func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("[handleSingleTx] Failed to process transaction: " + verdict.Error()))
		})

		require.Equal(t, http.StatusInternalServerError, rec.Code,
			"an older validator must degrade to the previous behaviour, not lose the failure")
		require.Contains(t, rec.Body.String(), "non-OK status")
	})
}

// runLargeTxFallback drives POST /tx through the > KafkaMaxMessageBytes HTTP
// fallback against a stub validator, and returns the transaction submitted and
// the recorded response. The limit is set to one byte rather than building a
// megabyte transaction: the branch under test is a size comparison.
func runLargeTxFallback(t *testing.T, respond func(w http.ResponseWriter)) (*bt.Tx, *httptest.ResponseRecorder) {
	t.Helper()

	validatorStub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		respond(w)
	}))
	t.Cleanup(validatorStub.Close)

	validatorURL, err := url.Parse(validatorStub.URL)
	require.NoError(t, err)

	tx := createRobustTestTx(t)
	ps, _ := setupPropagationServer(t, NewMockValidatorForTxTest(nil), nil)
	ps.validatorKafkaProducerClient = kafka.NewKafkaAsyncProducerMock()
	ps.validatorHTTPAddr = validatorURL
	ps.settings.Validator.KafkaMaxMessageBytes = 1

	rec := httptest.NewRecorder()
	c := echo.New().NewContext(
		httptest.NewRequest(http.MethodPost, "/tx", bytes.NewReader(tx.ExtendedBytes())), rec)
	require.NoError(t, ps.handleSingleTx(context.Background())(c))

	return tx, rec
}

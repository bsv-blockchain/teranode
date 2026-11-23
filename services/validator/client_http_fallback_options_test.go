package validator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// countingValidatorStub stands in for the validator's /tx endpoint and counts the
// requests it actually receives, plus the Content-Type of the last one.
func countingValidatorStub(t *testing.T) (*url.URL, *atomic.Int64, *atomic.Value) {
	t.Helper()

	var (
		calls       atomic.Int64
		contentType atomic.Value
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		contentType.Store(r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	addr, err := url.Parse(server.URL)
	require.NoError(t, err)

	return addr, &calls, &contentType
}

// TestValidateTransactionViaHTTP_RefusesNonDefaultOptions pins that the refusal
// happens BEFORE anything is transmitted. Refusing client-side rather than relying
// on the server's 400 makes the guarantee independent of the peer's version: during
// a rolling upgrade an un-upgraded validator would still honour these flags, which
// is the vulnerability itself.
func TestValidateTransactionViaHTTP_RefusesNonDefaultOptions(t *testing.T) {
	addr, calls, _ := countingValidatorStub(t)

	client := &Client{validatorHTTPAddr: addr, logger: &testLogger{t: t}}

	// Start from the defaults and set only the two flags under test. A raw
	// &Options{...} literal would leave AddTXToBlockAssembly at Go's zero value
	// false while its default is true, so the guard would correctly report
	// addTxToBlockAssembly as the first non-default field and this test would be
	// asserting the wrong one.
	opts := NewDefaultOptions()
	opts.SkipScriptValidation = true
	opts.OutpointOnlySpend = true

	err := client.validateTransactionViaHTTP(context.Background(), createTestTransaction(t), 0, opts)

	require.Error(t, err)
	require.Contains(t, err.Error(), "skipScriptValidation",
		"the error must name the field that cannot be carried")
	require.Contains(t, err.Error(), "fixed at 1 GiB",
		"the error must state why the limit cannot simply be raised, not name a setting that does not exist")
	require.Equal(t, int64(0), calls.Load(),
		"the refusal must happen before the request is sent, not after the server rejects it")
}

// TestValidateTransactionViaHTTP_DefaultOptionsStillSent is the non-regression
// twin: the mempool/RPC fallback (default options, height 0) must still reach the
// endpoint, as a protobuf body.
func TestValidateTransactionViaHTTP_DefaultOptionsStillSent(t *testing.T) {
	addr, calls, contentType := countingValidatorStub(t)

	client := &Client{validatorHTTPAddr: addr, logger: &testLogger{t: t}}

	err := client.validateTransactionViaHTTP(context.Background(), createTestTransaction(t), 0, NewDefaultOptions())

	require.NoError(t, err)
	require.Equal(t, int64(1), calls.Load(), "the default-options fallback must still be sent, exactly once")
	require.Equal(t, "application/x-protobuf", contentType.Load())
}

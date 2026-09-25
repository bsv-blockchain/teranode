package subtreevalidation

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/stretchr/testify/require"
)

// redirectTestTxHex is a single well-formed transaction, used as the peer's answer in the
// positive control so the helper's success path is proven to work before anything asserts
// a refusal.
const redirectTestTxHex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"

// redirectRefusal is the message util's redirect policy returns when it refuses a redirect of
// a POST. Asserting on it is what makes this test about the policy rather than about the
// transport happening to give up: without the refusal the request does not fail here at all,
// and with any other failure (a bad status, a closed connection, a count mismatch) this
// substring is absent. The message survives two layers of wrapping - ExternalError ->
// ServiceError -> *url.Error - because errors.Error renders the whole chain and falls back to
// Error() on the non-*Error tail, which is how util/http_rebind_test.go already asserts on it.
const redirectRefusal = "refusing to follow a redirect of a POST"

// assertRefusalReason is attached to every redirectRefusal assertion so the reason survives
// anyone tempted to reduce these checks to "an error came back".
const assertRefusalReason = "the redirect policy must be what rejected this, not an incidental transport failure"

// status307 is the substring util's buildHTTPError produces when net/http hands back the
// original 307 response instead of following it: every branch of that function formats
// "http request [%s] returned status code [%d]".
const status307 = "returned status code [307]"

// notReplayableReason explains what the 307 subtests actually pin, so they are not
// "simplified" into duplicates of the 302 ones.
const notReplayableReason = "a 307 must fail on the unreplayable body before CheckRedirect is reached, not via the redirect policy"

// newRedirectTestServer builds the Server fixture the missing-transaction helper needs.
// invalidSubtreeKafkaProducer is required because the failure path publishes an
// invalid-subtree message.
func newRedirectTestServer(t *testing.T) *Server {
	t.Helper()

	server := &Server{
		logger:                       ulogger.TestLogger{},
		settings:                     test.CreateBaseTestSettings(t),
		subtreeStore:                 memory.New(),
		invalidSubtreeKafkaProducer:  &mockKafkaProducer{},
		invalidSubtreeDeDuplicateMap: expiringmap.New[string, struct{}](time.Minute),
	}
	t.Cleanup(server.invalidSubtreeDeDuplicateMap.Stop)

	return server
}

// TestGetMissingTransactionsBatch_DoesNotFollowRedirectToAnotherOrigin covers issue 4841 at
// the helper that builds the POST: a peer answering the missing-transaction request with a
// redirect must not have the body - which is peer-chosen transaction hashes we assembled -
// delivered to an origin of its choosing. Real httptest servers are used rather than a mock
// transport because redirect handling is the thing under test.
//
// Two statuses are covered because they exercise two independent defences:
//
//   - 302 exercises ssrfCheckRedirect. The default client follows a 302, converting the POST
//     to a GET, so CheckRedirect is consulted and its POST refusal is what stops the hop. A
//     307 would prove nothing about the redirect policy here. Delete that refusal and the
//     302 subtests fail three ways: the victim is hit, no error comes back from the
//     transport, and the helper ends on a tx-count mismatch (a processing error, not
//     ErrExternal).
//   - 307 exercises the no-GetBody defence. executeHTTPRequestWithClient builds the POST body
//     with req.Body = io.NopCloser(...) and never sets GetBody, and net/http's
//     redirectBehavior declines a 307 outright when GetBody == nil && outgoingLength() != 0
//     (net/http/client.go, "case 307, 308"), returning the 307 response rather than
//     consulting CheckRedirect at all. The 307 subtests assert that shape - the status-code
//     error, and NOT the redirect-policy message - so they fail if anything ever reinstalls
//     GetBody on this path.
func TestGetMissingTransactionsBatch_DoesNotFollowRedirectToAnotherOrigin(t *testing.T) {
	var victimHits atomic.Int64

	victim := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		victimHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer victim.Close()

	tx, err := bt.NewTxFromString(redirectTestTxHex)
	require.NoError(t, err)

	answering := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(tx.Bytes())
	}))
	defer answering.Close()

	subtreeHash := chainhash.HashH([]byte("subtree-4841"))

	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, victim.URL+"/subtree/"+subtreeHash.String()+"/txs", http.StatusFound)
	}))
	defer redirecting.Close()

	redirecting307 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, victim.URL+"/subtree/"+subtreeHash.String()+"/txs", http.StatusTemporaryRedirect)
	}))
	defer redirecting307.Close()

	origProtection := util.SSRFProtectionEnabled()

	// Both servers are on loopback, which the dial policy always refuses; running with SSRF
	// protection off is also what proves the POST redirect refusal is unconditional.
	util.SetSSRFProtection(false)

	t.Cleanup(func() {
		util.SetSSRFProtection(origProtection)

		// The package-level signer is an atomic.Value: it cannot be stored back to nil and it
		// panics on a differently typed value, so a key-less Ed25519 signer is the only safe
		// reset. SignRequest returns before it touches the request.
		util.SetHTTPRequestSigner(util.NewEd25519RequestSigner(nil))
	})

	missing := []utxo.UnresolvedMetaData{{Hash: *tx.TxIDChainHash(), Idx: 0}}

	t.Run("positive control", func(t *testing.T) {
		server := newRedirectTestServer(t)

		txs, err := server.getMissingTransactionsBatch(context.Background(), subtreeHash, missing, answering.URL, "")
		require.NoError(t, err)
		require.Len(t, txs, 1, "the harness must reach the helper's success path")
	})

	t.Run("302 without signer", func(t *testing.T) {
		util.SetHTTPRequestSigner(util.NewEd25519RequestSigner(nil))

		server := newRedirectTestServer(t)
		victimHits.Store(0)

		_, err := server.getMissingTransactionsBatch(context.Background(), subtreeHash, missing, redirecting.URL, "")
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrExternal))
		require.Contains(t, err.Error(), redirectRefusal, assertRefusalReason)
		require.Zero(t, victimHits.Load(), "the redirect target must never see the POST")
	})

	t.Run("307 without signer", func(t *testing.T) {
		util.SetHTTPRequestSigner(util.NewEd25519RequestSigner(nil))

		server := newRedirectTestServer(t)
		victimHits.Store(0)

		_, err := server.getMissingTransactionsBatch(context.Background(), subtreeHash, missing, redirecting307.URL, "")
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrExternal))
		require.Contains(t, err.Error(), status307, notReplayableReason)
		require.NotContains(t, err.Error(), redirectRefusal, notReplayableReason)
		require.Zero(t, victimHits.Load(), "the redirect target must never see the POST")
	})

	t.Run("302 with signer", func(t *testing.T) {
		privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
		require.NoError(t, err)

		util.SetHTTPRequestSigner(util.NewEd25519RequestSigner(privKey))

		server := newRedirectTestServer(t)
		victimHits.Store(0)

		_, err = server.getMissingTransactionsBatch(context.Background(), subtreeHash, missing, redirecting.URL, "")
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrExternal))
		require.Contains(t, err.Error(), redirectRefusal, assertRefusalReason)
		require.Zero(t, victimHits.Load(), "signing must not change redirect semantics")
	})

	t.Run("307 with signer", func(t *testing.T) {
		privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
		require.NoError(t, err)

		util.SetHTTPRequestSigner(util.NewEd25519RequestSigner(privKey))

		server := newRedirectTestServer(t)
		victimHits.Store(0)

		_, err = server.getMissingTransactionsBatch(context.Background(), subtreeHash, missing, redirecting307.URL, "")
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrExternal))
		require.Contains(t, err.Error(), status307, notReplayableReason)
		require.NotContains(t, err.Error(), redirectRefusal, notReplayableReason)
		require.Zero(t, victimHits.Load(), "the redirect target must never see the POST")
	})
}

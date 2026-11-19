package subtreevalidation

import (
	"context"
	"crypto/rand"
	"fmt"
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
// executeHTTPRequestWithClient sets GetBody on the POST, so net/http consults CheckRedirect for
// 301, 302 and 303 and also for 307 and 308 (net/http/client.go, redirectBehavior). Every
// status is therefore stopped by ssrfCheckRedirect's refusal of a redirect of a request that
// is not a plain read. The 307 and 308 subtests are the ones that would replay the body
// verbatim to the redirect target if that refusal were removed.
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

	redirecting308 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, victim.URL+"/subtree/"+subtreeHash.String()+"/txs", http.StatusPermanentRedirect)
	}))
	defer redirecting308.Close()

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

	redirectors := []struct {
		status int
		server *httptest.Server
	}{
		{status: http.StatusFound, server: redirecting},
		{status: http.StatusTemporaryRedirect, server: redirecting307},
		{status: http.StatusPermanentRedirect, server: redirecting308},
	}

	for _, redirect := range redirectors {
		for _, withSigner := range []bool{false, true} {
			name := fmt.Sprintf("%d without signer", redirect.status)
			if withSigner {
				name = fmt.Sprintf("%d with signer", redirect.status)
			}

			t.Run(name, func(t *testing.T) {
				if withSigner {
					privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
					require.NoError(t, err)

					util.SetHTTPRequestSigner(util.NewEd25519RequestSigner(privKey))
				} else {
					util.SetHTTPRequestSigner(util.NewEd25519RequestSigner(nil))
				}

				server := newRedirectTestServer(t)
				victimHits.Store(0)

				_, err := server.getMissingTransactionsBatch(context.Background(), subtreeHash, missing, redirect.server.URL, "")
				require.Error(t, err)
				require.True(t, errors.Is(err, errors.ErrExternal))
				require.Contains(t, err.Error(), redirectRefusal, assertRefusalReason)
				require.Zero(t, victimHits.Load(), "the redirect target must never see the POST")
			})
		}
	}
}

package util

import (
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/stretchr/testify/require"
)

// redirectRefusal is the message ssrfCheckRedirect returns when it refuses a redirect of a
// POST. Asserting on it is what makes this test about the policy rather than about the
// transport happening to give up: with the refusal deleted the redirect is followed and no
// error comes back at all, and any other failure mode carries a different message.
const redirectRefusal = "refusing to follow a redirect of a POST"

// status307 is the substring buildHTTPError produces when net/http hands back the original
// 307 response instead of following it. Every branch of buildHTTPError formats
// "http request [%s] returned status code [%d]", so this is the whole status-code clause.
const status307 = "returned status code [307]"

// notReplayableReason explains what the 307 subtests below actually pin, so they are not
// "simplified" into duplicates of the 302 ones.
const notReplayableReason = "a 307 must fail on the unreplayable body before CheckRedirect is reached, not via the redirect policy"

// TestDoHTTPRequestBodyReader_POSTRedirectNotReplayedToOtherOrigin is the regression test
// issue 4841 asks for: the missing-transaction helper posts a body built from peer-supplied
// data, and a peer answering with a redirect must not get that body delivered to an origin of
// its choosing. That has to hold with and without the package-level signer installed -
// signing must not change what the client does with the request - and with SSRF protection
// turned off, which is how test topologies reach loopback.
//
// Two statuses are covered because they exercise two independent defences, and neither
// substitutes for the other:
//
//   - 302 exercises ssrfCheckRedirect. The default client follows a 302, converting the POST
//     to a GET, so CheckRedirect is consulted and its POST refusal is what stops the hop.
//     A 307 would prove nothing about the redirect policy here.
//   - 307 exercises the no-GetBody defence. This path builds the body with
//     req.Body = io.NopCloser(...) and leaves GetBody nil, and net/http's redirectBehavior
//     declines a 307 outright when GetBody == nil && outgoingLength() != 0
//     (net/http/client.go, "case 307, 308"), returning the 307 response rather than
//     consulting CheckRedirect at all. The 307 subtests assert exactly that shape - the
//     status-code error, and NOT the redirect-policy message - so they fail if anything ever
//     reinstalls GetBody on this path, which is the regression issue 4841 closes.
func TestDoHTTPRequestBodyReader_POSTRedirectNotReplayedToOtherOrigin(t *testing.T) {
	var victimHits atomic.Int64

	var victimBody atomic.Value // stores []byte

	victim := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		victimHits.Add(1)

		seen, _ := io.ReadAll(r.Body)
		victimBody.Store(seen)

		w.WriteHeader(http.StatusOK)
	}))
	defer victim.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, victim.URL+"/blob/x", http.StatusFound)
	}))
	defer redirector.Close()

	redirector307 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, victim.URL+"/blob/x", http.StatusTemporaryRedirect)
	}))
	defer redirector307.Close()

	const directBody = "direct-answer"

	direct := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte(directBody))
	}))
	defer direct.Close()

	origProtection := SSRFProtectionEnabled()
	origSigner := loadHTTPRequestSigner()

	t.Cleanup(func() {
		SetSSRFProtection(origProtection)

		if origSigner != nil {
			SetHTTPRequestSigner(origSigner)
			return
		}

		// The package signer is an atomic.Value: it cannot be stored back to nil, and it
		// panics if a different concrete type is stored. A key-less Ed25519 signer is the
		// only safe reset - SignRequest returns before it touches the request.
		SetHTTPRequestSigner(NewEd25519RequestSigner(nil))
	})

	// Both servers are on loopback, which the dial policy always refuses. Running with SSRF
	// protection off is also the point of the test: the POST refusal must not be conditional
	// on that toggle.
	SetSSRFProtection(false)

	body := make([]byte, 32)

	t.Run("positive control", func(t *testing.T) {
		reader, err := DoHTTPRequestBodyReader(context.Background(), direct.URL+"/txs", body)
		require.NoError(t, err)

		defer func() { _ = reader.Close() }()

		read, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, directBody, string(read), "the harness must reach a POST handler at all")
	})

	t.Run("302 without signer", func(t *testing.T) {
		// Installed explicitly rather than assuming a clean start: other tests in this
		// package install a real signer permanently.
		SetHTTPRequestSigner(NewEd25519RequestSigner(nil))

		victimHits.Store(0)

		reader, err := DoHTTPRequestBodyReader(context.Background(), redirector.URL+"/txs", body)
		if reader != nil {
			_ = reader.Close()
		}

		require.Error(t, err)
		require.Contains(t, err.Error(), redirectRefusal, "the redirect policy must be what rejected this, not an incidental transport failure")
		require.Zero(t, victimHits.Load(), "the redirect target must never be contacted")
		require.Nil(t, victimBody.Load(), "the body must never reach the redirect target")
	})

	t.Run("307 without signer", func(t *testing.T) {
		SetHTTPRequestSigner(NewEd25519RequestSigner(nil))

		victimHits.Store(0)

		reader, err := DoHTTPRequestBodyReader(context.Background(), redirector307.URL+"/txs", body)
		if reader != nil {
			_ = reader.Close()
		}

		require.Error(t, err)
		require.Contains(t, err.Error(), status307, notReplayableReason)
		require.NotContains(t, err.Error(), redirectRefusal, notReplayableReason)
		require.Zero(t, victimHits.Load(), "the redirect target must never be contacted")
		require.Nil(t, victimBody.Load(), "the body must never reach the redirect target")
	})

	t.Run("302 with signer", func(t *testing.T) {
		privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
		require.NoError(t, err)

		SetHTTPRequestSigner(NewEd25519RequestSigner(privKey))

		victimHits.Store(0)

		reader, err := DoHTTPRequestBodyReader(context.Background(), redirector.URL+"/txs", body)
		if reader != nil {
			_ = reader.Close()
		}

		require.Error(t, err)
		require.Contains(t, err.Error(), redirectRefusal, "the redirect policy must be what rejected this, not an incidental transport failure")
		require.Zero(t, victimHits.Load(), "installing a signer must not change redirect semantics")
		require.Nil(t, victimBody.Load(), "the signed body must never reach the redirect target")
	})

	t.Run("307 with signer", func(t *testing.T) {
		privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
		require.NoError(t, err)

		SetHTTPRequestSigner(NewEd25519RequestSigner(privKey))

		victimHits.Store(0)

		reader, err := DoHTTPRequestBodyReader(context.Background(), redirector307.URL+"/txs", body)
		if reader != nil {
			_ = reader.Close()
		}

		require.Error(t, err)
		require.Contains(t, err.Error(), status307, notReplayableReason)
		require.NotContains(t, err.Error(), redirectRefusal, notReplayableReason)
		require.Zero(t, victimHits.Load(), "the redirect target must never be contacted")
		require.Nil(t, victimBody.Load(), "the signed body must never reach the redirect target")
	})
}

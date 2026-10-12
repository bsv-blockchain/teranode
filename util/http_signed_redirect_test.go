package util

import (
	"context"
	"crypto/rand"
	"fmt"
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

// TestDoHTTPRequestBodyReader_POSTRedirectNotReplayedToOtherOrigin is the regression test
// issue 4841 asks for: the missing-transaction helper posts a body built from peer-supplied
// data, and a peer answering with a redirect must not get that body delivered to an origin of
// its choosing. That has to hold with and without the package-level signer installed -
// signing must not change what the client does with the request - and with SSRF protection
// turned off, which is how test topologies reach loopback.
//
// This path sets GetBody on the request, so net/http consults CheckRedirect for 301, 302 and
// 303 and also for 307 and 308 (net/http/client.go, redirectBehavior). Every status is
// therefore stopped by ssrfCheckRedirect's refusal of a redirect of a request that is not a
// plain read. The 307 and 308 subtests are the ones that would replay the body verbatim to the
// redirect target if that refusal were removed.
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

	redirector308 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, victim.URL+"/blob/x", http.StatusPermanentRedirect)
	}))
	defer redirector308.Close()

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

	redirectors := []struct {
		status int
		server *httptest.Server
	}{
		{status: http.StatusFound, server: redirector},
		{status: http.StatusTemporaryRedirect, server: redirector307},
		{status: http.StatusPermanentRedirect, server: redirector308},
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

					SetHTTPRequestSigner(NewEd25519RequestSigner(privKey))
				} else {
					// Installed explicitly rather than assuming a clean start: other tests in
					// this package install a real signer permanently.
					SetHTTPRequestSigner(NewEd25519RequestSigner(nil))
				}

				victimHits.Store(0)

				reader, err := DoHTTPRequestBodyReader(context.Background(), redirect.server.URL+"/txs", body)
				if reader != nil {
					_ = reader.Close()
				}

				require.Error(t, err)
				require.Contains(t, err.Error(), redirectRefusal, "the redirect policy must be what rejected this, not an incidental transport failure")
				require.Zero(t, victimHits.Load(), "the redirect target must never be contacted")
				require.Nil(t, victimBody.Load(), "the body must never reach the redirect target")
			})
		}
	}
}

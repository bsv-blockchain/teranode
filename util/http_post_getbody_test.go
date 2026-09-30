package util

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/stretchr/testify/require"
)

// roundTripperFunc adapts a function to http.RoundTripper.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// capturingClient returns a client whose transport records the request it is handed and
// answers 200 without touching the network.
func capturingClient() (*http.Client, func() *http.Request) {
	var (
		mu       sync.Mutex
		captured *http.Request
	)

	client := &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			captured = req
			mu.Unlock()

			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"application/octet-stream"}},
				Body:       io.NopCloser(strings.NewReader("ok")),
				Request:    req,
			}, nil
		}),
	}

	return client, func() *http.Request {
		mu.Lock()
		defer mu.Unlock()

		return captured
	}
}

// readGetBody calls req.GetBody once and returns everything it yields.
func readGetBody(t *testing.T, req *http.Request) []byte {
	t.Helper()

	rc, err := req.GetBody()
	require.NoError(t, err)

	defer func() { _ = rc.Close() }()

	read, err := io.ReadAll(rc)
	require.NoError(t, err)

	return read
}

// TestExecuteHTTPRequestWithClient_POSTBodyIsRewindable pins that the helper building the
// missing-transaction POST body sets GetBody, so net/http can resend the body on a fresh
// connection after an HTTP/2 refusal or a reused HTTP/1.1 connection that failed before
// writing, instead of the failure being charged to the peer.
func TestExecuteHTTPRequestWithClient_POSTBodyIsRewindable(t *testing.T) {
	const rawURL = "http://peer.test/subtree/x/txs"

	body := []byte("packed-tx-hashes-for-the-peer")

	origSigner := loadHTTPRequestSigner()

	t.Cleanup(func() {
		if origSigner != nil {
			SetHTTPRequestSigner(origSigner)
			return
		}

		// The package signer is an atomic.Value: it cannot be stored back to nil, and it
		// panics if a different concrete type is stored. A key-less Ed25519 signer is the
		// only safe reset - SignRequest returns before it touches the request.
		SetHTTPRequestSigner(NewEd25519RequestSigner(nil))
	})

	t.Run("without signer", func(t *testing.T) {
		SetHTTPRequestSigner(NewEd25519RequestSigner(nil))

		client, captured := capturingClient()

		reader, _, err := executeHTTPRequestWithClient(context.Background(), func() {}, client, rawURL, body)
		require.NoError(t, err)
		require.NoError(t, reader.Close())

		req := captured()
		require.NotNil(t, req)
		require.Equal(t, http.MethodPost, req.Method)
		require.NotNil(t, req.GetBody, "the POST body must be rewindable for transport retries")

		require.Equal(t, body, readGetBody(t, req))
		require.Equal(t, body, readGetBody(t, req), "GetBody must yield the whole body on every call")
	})

	t.Run("with signer", func(t *testing.T) {
		privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
		require.NoError(t, err)

		SetHTTPRequestSigner(NewEd25519RequestSigner(privKey))

		client, captured := capturingClient()

		reader, _, err := executeHTTPRequestWithClient(context.Background(), func() {}, client, rawURL, body)
		require.NoError(t, err)
		require.NoError(t, reader.Close())

		req := captured()
		require.NotNil(t, req)
		require.NotNil(t, req.GetBody, "the signed POST body must be rewindable for transport retries")

		resent := readGetBody(t, req)
		require.Equal(t, body, resent)

		digest := sha256.Sum256(resent)
		require.Equal(t, hex.EncodeToString(digest[:]), req.Header.Get(PeerAuthBodyDigestHeader),
			"a retried request must carry the same bytes the signature covers")
	})

	t.Run("GET carries no GetBody", func(t *testing.T) {
		SetHTTPRequestSigner(NewEd25519RequestSigner(nil))

		client, captured := capturingClient()

		reader, _, err := executeHTTPRequestWithClient(context.Background(), func() {}, client, rawURL)
		require.NoError(t, err)
		require.NoError(t, reader.Close())

		req := captured()
		require.NotNil(t, req)
		require.Equal(t, http.MethodGet, req.Method)
		require.Nil(t, req.GetBody)
	})
}

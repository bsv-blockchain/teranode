package util

import (
	"bytes"
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

func TestEd25519RequestSigner_SignsGetRequest(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	signer := NewEd25519RequestSigner(privKey)

	req, err := http.NewRequest(http.MethodGet, "http://example.test/api/v1/test?from=10&to=20", nil)
	require.NoError(t, err)

	require.NoError(t, signer.SignRequest(req))

	pubKeyHex := req.Header.Get("X-Peer-PubKey")
	tsStr := req.Header.Get("X-Peer-Timestamp")
	digest := req.Header.Get(PeerAuthBodyDigestHeader)
	sigHex := req.Header.Get("X-Peer-Signature")

	require.NotEmpty(t, pubKeyHex)
	require.NotEmpty(t, tsStr)
	require.Equal(t, EmptyBodySHA256Hex, digest, "GET with no body must carry empty-body digest")
	require.NotEmpty(t, sigHex)

	// Reconstruct the canonical payload exactly as a verifier would.
	pubKeyBytes, err := hex.DecodeString(pubKeyHex)
	require.NoError(t, err)
	pubKey, err := crypto.UnmarshalEd25519PublicKey(pubKeyBytes)
	require.NoError(t, err)

	expected := buildSignedPayload(tsStr, req.Host, req.Method, req.URL.RequestURI(), digest)
	require.True(t, strings.HasPrefix(expected, "v2:"), "payload must be versioned")

	sigBytes, err := hex.DecodeString(sigHex)
	require.NoError(t, err)
	ok, err := pubKey.Verify([]byte(expected), sigBytes)
	require.NoError(t, err)
	require.True(t, ok, "signature must verify")
}

func TestEd25519RequestSigner_SignsPostRequestWithBody(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	signer := NewEd25519RequestSigner(privKey)

	body := []byte(`{"txids":["a","b","c"]}`)
	req, err := http.NewRequest(http.MethodPost, "http://example.test/api/v1/subtree/abc/txs", bytes.NewReader(body))
	require.NoError(t, err)

	require.NoError(t, signer.SignRequest(req))

	// Body must still be readable by the transport.
	readBack, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	require.Equal(t, body, readBack, "signer must leave req.Body re-readable")
	require.Equal(t, int64(len(body)), req.ContentLength)
	require.NotNil(t, req.GetBody, "GetBody must be set so HTTP/2 retries replay the body")

	digest := req.Header.Get(PeerAuthBodyDigestHeader)
	want := sha256.Sum256(body)
	require.Equal(t, hex.EncodeToString(want[:]), digest)
}

func TestEd25519RequestSigner_NilKey(t *testing.T) {
	signer := NewEd25519RequestSigner(nil)

	req, err := http.NewRequest(http.MethodPost, "http://localhost/submit", nil)
	require.NoError(t, err)

	require.NoError(t, signer.SignRequest(req))

	require.Empty(t, req.Header.Get("X-Peer-PubKey"))
	require.Empty(t, req.Header.Get("X-Peer-Timestamp"))
	require.Empty(t, req.Header.Get("X-Peer-Signature"))
}

// TestSetHTTPRequestSigner_ConcurrentRace exercises the package-level signer
// from multiple goroutines under -race to confirm reads and writes don't tear.
// (The earlier non-atomic interface variable produced a data race that the
// existing test suite never tripped on because no test was concurrent.)
func TestSetHTTPRequestSigner_ConcurrentRace(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	signer := NewEd25519RequestSigner(privKey)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			SetHTTPRequestSigner(signer)
		}()
		go func() {
			defer wg.Done()
			_ = loadHTTPRequestSigner()
		}()
	}
	wg.Wait()

	require.NotNil(t, loadHTTPRequestSigner())
}

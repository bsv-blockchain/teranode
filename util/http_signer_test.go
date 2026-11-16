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

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
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

	expected := PeerAuthSignedPayload(tsStr, req.Host, req.Method, req.URL.RequestURI(), digest)
	require.True(t, strings.HasPrefix(expected, "teranode-http-peer-auth/v3:"), "payload must carry the domain tag and version")

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

	// The body is wrapped in a NopCloser deliberately, and must stay wrapped. Handing
	// http.NewRequest a bare *bytes.Reader makes net/http install GetBody and ContentLength
	// itself (net/http/request.go, the *bytes.Reader case), so the assertions below would be
	// about what NewRequest did rather than about the signer. A NopCloser falls to
	// NewRequest's default case, which leaves both zero. The real caller,
	// executeHTTPRequestWithClient, sets GetBody itself where it builds the body; this test pins
	// that the signer adds none of its own.
	req, err := http.NewRequest(http.MethodPost, "http://example.test/api/v1/subtree/abc/txs", io.NopCloser(bytes.NewReader(body)))
	require.NoError(t, err)
	require.Nil(t, req.GetBody, "precondition: the request must reach the signer with no GetBody")

	require.NoError(t, signer.SignRequest(req))

	// Body must still be readable by the transport.
	readBack, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	require.Equal(t, body, readBack, "signer must leave req.Body re-readable")
	// NewRequest left ContentLength at 0 for a NopCloser, so this is the signer's own
	// assignment - without it the transport would send the body with an unknown length.
	require.Equal(t, int64(len(body)), req.ContentLength)
	require.Nil(t, req.GetBody, "signing must not install GetBody - replaying a body across a redirect is the caller's decision, not the signer's")

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

// TestPeerAuthSignedPayload_DomainSeparatedFromLibp2p pins that the bytes the HTTP signer
// signs with the node's libp2p identity key can never be mistaken for bytes a libp2p
// protocol signs with that same key. Each libp2p protocol prefixes what it signs with its
// own domain string; the HTTP payload carries its own tag, and no tag may be a prefix of
// another, or a signature made for one could be presented as valid for the other.
//
// The noise and TLS prefixes are unexported in go-libp2p, so they are written out here
// (go-libp2p p2p/security/noise/handshake.go payloadSigPrefix, p2p/security/tls/crypto.go
// certificatePrefix). A signed envelope (peer records) signs uvarint(len(domain)) || domain
// || ..., so its first bytes are the length byte followed by the domain.
func TestPeerAuthSignedPayload_DomainSeparatedFromLibp2p(t *testing.T) {
	payload := PeerAuthSignedPayload("1700000000", "peer.example:8090", http.MethodPost, "/api/v1/subtree/abc/txs?x=1", EmptyBodySHA256Hex)

	require.True(t, strings.HasPrefix(payload, "teranode-http-peer-auth/v3:"), "the signed payload must open with the HTTP peer-auth domain tag")

	envelopeDomain := peer.PeerRecordEnvelopeDomain

	libp2pPrefixes := map[string]string{
		"pubsub message signing":    pubsub.SignPrefix,
		"noise static key":          "noise-libp2p-static-key:",
		"tls handshake certificate": "libp2p-tls-handshake:",
		"peer record envelope":      string([]byte{byte(len(envelopeDomain))}) + envelopeDomain,
	}

	for name, prefix := range libp2pPrefixes {
		require.False(t, strings.HasPrefix(payload, prefix), "HTTP payload must not open with the %s domain", name)
		require.False(t, strings.HasPrefix(prefix, PeerAuthDomain), "the %s domain must not open with the HTTP peer-auth tag", name)
	}
}

// TestPeerAuthLegacyV2Payload pins the pre-domain-tag format byte for byte. Verifiers still
// accept it so peers running the previous signer keep their tier while the network upgrades,
// and the verifier's own test signs with this function, so a change here would pass that test
// while breaking every real v2 peer.
func TestPeerAuthLegacyV2Payload(t *testing.T) {
	require.Equal(t, "v2:1700000000:h:GET:/p?q=1:"+EmptyBodySHA256Hex,
		PeerAuthLegacyV2Payload("1700000000", "h", http.MethodGet, "/p?q=1", EmptyBodySHA256Hex))
}

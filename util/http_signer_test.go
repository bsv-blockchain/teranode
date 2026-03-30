package util

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/stretchr/testify/require"
)

func TestEd25519RequestSigner_SignsRequest(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	signer := NewEd25519RequestSigner(privKey)

	req, err := http.NewRequest(http.MethodGet, "http://localhost/api/v1/test", nil)
	require.NoError(t, err)

	err = signer.SignRequest(req)
	require.NoError(t, err)

	// All three headers must be set.
	pubKeyHex := req.Header.Get("X-Peer-PubKey")
	tsStr := req.Header.Get("X-Peer-Timestamp")
	sigHex := req.Header.Get("X-Peer-Signature")

	require.NotEmpty(t, pubKeyHex, "X-Peer-PubKey header should be set")
	require.NotEmpty(t, tsStr, "X-Peer-Timestamp header should be set")
	require.NotEmpty(t, sigHex, "X-Peer-Signature header should be set")

	// Decode the public key and verify the signature manually.
	pubKeyBytes, err := hex.DecodeString(pubKeyHex)
	require.NoError(t, err)

	pubKey, err := crypto.UnmarshalEd25519PublicKey(pubKeyBytes)
	require.NoError(t, err)

	payload := fmt.Sprintf("%s:%s:%s", tsStr, req.Method, req.URL.Path)

	sigBytes, err := hex.DecodeString(sigHex)
	require.NoError(t, err)

	ok, err := pubKey.Verify([]byte(payload), sigBytes)
	require.NoError(t, err)
	require.True(t, ok, "signature verification should succeed")
}

func TestEd25519RequestSigner_NilKey(t *testing.T) {
	signer := NewEd25519RequestSigner(nil)

	req, err := http.NewRequest(http.MethodPost, "http://localhost/submit", nil)
	require.NoError(t, err)

	err = signer.SignRequest(req)
	require.NoError(t, err, "nil key should not produce an error")

	require.Empty(t, req.Header.Get("X-Peer-PubKey"), "no headers should be set with nil key")
	require.Empty(t, req.Header.Get("X-Peer-Timestamp"), "no headers should be set with nil key")
	require.Empty(t, req.Header.Get("X-Peer-Signature"), "no headers should be set with nil key")
}

func TestSetHTTPRequestSigner(t *testing.T) {
	// Reset to nil after test to avoid polluting other tests.
	original := httpRequestSigner
	defer func() { httpRequestSigner = original }()

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	signer := NewEd25519RequestSigner(privKey)
	SetHTTPRequestSigner(signer)

	require.Equal(t, signer, httpRequestSigner, "package-level signer should be set")
}

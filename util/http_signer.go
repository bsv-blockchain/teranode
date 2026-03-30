package util

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
)

// HTTPRequestSigner signs outgoing HTTP requests with identity and timestamp headers.
type HTTPRequestSigner interface {
	SignRequest(req *http.Request) error
}

// httpRequestSigner is the package-level signer used by executeHTTPRequest.
var httpRequestSigner HTTPRequestSigner

// SetHTTPRequestSigner sets the package-level HTTP request signer.
func SetHTTPRequestSigner(signer HTTPRequestSigner) {
	httpRequestSigner = signer
}

// Ed25519RequestSigner signs HTTP requests using an Ed25519 private key.
type Ed25519RequestSigner struct {
	privKey   crypto.PrivKey
	pubKeyHex string
}

// NewEd25519RequestSigner creates a new Ed25519RequestSigner from a libp2p Ed25519 private key.
func NewEd25519RequestSigner(privKey crypto.PrivKey) *Ed25519RequestSigner {
	pubKeyHex := ""
	if privKey != nil {
		raw, err := privKey.GetPublic().Raw()
		if err == nil {
			pubKeyHex = hex.EncodeToString(raw)
		}
	}

	return &Ed25519RequestSigner{
		privKey:   privKey,
		pubKeyHex: pubKeyHex,
	}
}

// SignRequest signs the HTTP request by setting X-Peer-PubKey, X-Peer-Timestamp,
// and X-Peer-Signature headers. The signature covers "timestamp:METHOD:/path".
func (s *Ed25519RequestSigner) SignRequest(req *http.Request) error {
	if s.privKey == nil {
		return nil
	}

	ts := strconv.FormatInt(time.Now().Unix(), 10)
	payload := fmt.Sprintf("%s:%s:%s", ts, req.Method, req.URL.Path)

	sig, err := s.privKey.Sign([]byte(payload))
	if err != nil {
		return err
	}

	req.Header.Set("X-Peer-PubKey", s.pubKeyHex)
	req.Header.Set("X-Peer-Timestamp", ts)
	req.Header.Set("X-Peer-Signature", hex.EncodeToString(sig))

	return nil
}

package util

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
)

// PeerAuthVersion is the wire version of the signed-payload format. The payload
// format is:
//
//	v<PeerAuthVersion>:<unix_ts>:<host>:<method>:<request_uri>:<sha256_body_hex>
//
// Bumping this constant (and the matching verifier prefix) is the upgrade
// mechanism for any future change to the signed payload.
const PeerAuthVersion = 2

// PeerAuthBodyDigestHeader carries the lowercase hex SHA-256 of the request
// body. Empty bodies use the SHA-256 of the empty string. Verifiers MUST
// recompute the digest from the actual body and reject on mismatch.
const PeerAuthBodyDigestHeader = "X-Peer-Body-Digest"

// EmptyBodySHA256Hex is the lowercase hex SHA-256 of an empty body.
const EmptyBodySHA256Hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// HTTPRequestSigner signs outgoing HTTP requests with identity, timestamp, and
// body-digest headers. Implementations MUST be safe for concurrent use because
// SignRequest is invoked from arbitrary request goroutines.
type HTTPRequestSigner interface {
	SignRequest(req *http.Request) error
}

// httpRequestSigner is the package-level signer used by executeHTTPRequest.
// It is read on every outbound request and written rarely (typically once at
// startup). atomic.Value gives lock-free, race-free reads.
var httpRequestSigner atomic.Value // stores HTTPRequestSigner

// SetHTTPRequestSigner sets the package-level HTTP request signer.
func SetHTTPRequestSigner(signer HTTPRequestSigner) {
	if signer == nil {
		return
	}
	httpRequestSigner.Store(signer)
}

// loadHTTPRequestSigner returns the current signer, or nil if none is set.
func loadHTTPRequestSigner() HTTPRequestSigner {
	v := httpRequestSigner.Load()
	if v == nil {
		return nil
	}
	return v.(HTTPRequestSigner)
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
// X-Peer-Body-Digest, and X-Peer-Signature headers.
//
// The signature covers:
//
//	v<PeerAuthVersion>:<unix_ts>:<host>:<method>:<request_uri>:<sha256_body_hex>
//
// where host is req.Host, request_uri is req.URL.RequestURI() (path + raw
// query), and sha256_body_hex is the lowercase hex SHA-256 of the request
// body. For requests with a body, SignRequest reads the body into memory,
// hashes it, and replaces req.Body so downstream transports still see it.
func (s *Ed25519RequestSigner) SignRequest(req *http.Request) error {
	if s.privKey == nil {
		return nil
	}

	bodyDigestHex, err := digestRequestBody(req)
	if err != nil {
		return err
	}

	ts := strconv.FormatInt(time.Now().Unix(), 10)
	payload := buildSignedPayload(ts, req.Host, req.Method, req.URL.RequestURI(), bodyDigestHex)

	sig, err := s.privKey.Sign([]byte(payload))
	if err != nil {
		return err
	}

	req.Header.Set("X-Peer-PubKey", s.pubKeyHex)
	req.Header.Set("X-Peer-Timestamp", ts)
	req.Header.Set(PeerAuthBodyDigestHeader, bodyDigestHex)
	req.Header.Set("X-Peer-Signature", hex.EncodeToString(sig))

	return nil
}

// digestRequestBody reads the request body (if any), returns its lowercase hex
// SHA-256, and replaces req.Body with a fresh reader over the buffered bytes
// so the request can still be sent. For empty bodies returns the SHA-256 of
// the empty string.
func digestRequestBody(req *http.Request) (string, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return EmptyBodySHA256Hex, nil
	}

	buf, err := io.ReadAll(req.Body)
	if err != nil {
		return "", err
	}
	_ = req.Body.Close()

	if len(buf) == 0 {
		req.Body = http.NoBody
		return EmptyBodySHA256Hex, nil
	}

	sum := sha256.Sum256(buf)
	req.Body = io.NopCloser(bytes.NewReader(buf))
	req.ContentLength = int64(len(buf))
	// GetBody is consulted by net/http on redirects and HTTP/2 retries; provide
	// a clone so signed retries don't lose the body.
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(buf)), nil
	}
	return hex.EncodeToString(sum[:]), nil
}

// buildSignedPayload is the canonical form covered by the signature. Verifiers
// MUST construct an identical payload from the request being verified.
func buildSignedPayload(ts, host, method, requestURI, bodyDigestHex string) string {
	return "v" + strconv.Itoa(PeerAuthVersion) + ":" + ts + ":" + host + ":" + method + ":" + requestURI + ":" + bodyDigestHex
}

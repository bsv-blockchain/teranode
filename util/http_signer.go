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

// PeerAuthDomain opens every payload the HTTP signer signs. The signing key is
// the node's libp2p identity key, which libp2p protocols also sign with; each of
// those prefixes what it signs with its own domain string (pubsub, noise, TLS,
// signed envelopes). This tag keeps an HTTP signature from ever being valid for
// one of those payloads, or theirs for this one. A new protocol that signs with
// the identity key needs its own tag, and must not share this one.
const PeerAuthDomain = "teranode-http-peer-auth"

// PeerAuthVersion is the wire version of the signed-payload format. The payload
// format is:
//
//	teranode-http-peer-auth/v<PeerAuthVersion>:<unix_ts>:<host>:<method>:<request_uri>:<sha256_body_hex>
//
// Bumping this constant is the upgrade mechanism for any future change to the
// signed payload. Verifiers build the payload with PeerAuthSignedPayload, so the
// signer and the verifier cannot drift apart.
const PeerAuthVersion = 3

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

// signerSignsRequests reports whether signer actually adds a signature. An
// Ed25519RequestSigner without a private key installs cleanly but signs
// nothing, so its requests carry no timestamp that a retry could replay.
func signerSignsRequests(signer HTTPRequestSigner) bool {
	if signer == nil {
		return false
	}

	if s, ok := signer.(*Ed25519RequestSigner); ok && s.privKey == nil {
		return false
	}

	return true
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
//	teranode-http-peer-auth/v<PeerAuthVersion>:<unix_ts>:<host>:<method>:<request_uri>:<sha256_body_hex>
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
	payload := PeerAuthSignedPayload(ts, req.Host, req.Method, req.URL.RequestURI(), bodyDigestHex)

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
	// Signing must not change what the client will do with this request, so it does not install
	// GetBody: that would let net/http replay the body across a 307 or 308 on the signer's say-so.
	// executeHTTPRequestWithClient, which builds the body, sets GetBody itself for transport
	// retries and relies on ssrfCheckRedirect to refuse any redirect of a POST.

	return hex.EncodeToString(sum[:]), nil
}

// PeerAuthSignedPayload is the canonical form covered by the signature. Verifiers
// MUST construct an identical payload from the request being verified.
func PeerAuthSignedPayload(ts, host, method, requestURI, bodyDigestHex string) string {
	return PeerAuthDomain + "/v" + strconv.Itoa(PeerAuthVersion) + ":" + ts + ":" + host + ":" + method + ":" + requestURI + ":" + bodyDigestHex
}

// PeerAuthLegacyV2Payload is the payload signed before PeerAuthDomain was added,
// with no domain tag. Verifiers accept it for one release so peers that have not
// upgraded keep their tier; the signer never produces it. Remove it, and the
// verifier's fallback to it, in the release after the domain tag ships.
func PeerAuthLegacyV2Payload(ts, host, method, requestURI, bodyDigestHex string) string {
	return "v2:" + ts + ":" + host + ":" + method + ":" + requestURI + ":" + bodyDigestHex
}

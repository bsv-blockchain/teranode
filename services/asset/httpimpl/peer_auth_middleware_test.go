package httpimpl

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// signTestRequest signs an HTTP request with the v2 payload format and sets
// all required peer-auth headers. Body (if any) is consumed and replaced.
func signTestRequest(t *testing.T, req *http.Request, privKey crypto.PrivKey) {
	t.Helper()

	bodyDigest := util.EmptyBodySHA256Hex
	if req.Body != nil && req.Body != http.NoBody {
		buf, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		_ = req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(buf))
		req.ContentLength = int64(len(buf))
		if len(buf) > 0 {
			sum := sha256.Sum256(buf)
			bodyDigest = hex.EncodeToString(sum[:])
		}
	}

	ts := strconv.FormatInt(time.Now().Unix(), 10)
	payload := "v2:" + ts + ":" + req.Host + ":" + req.Method + ":" + req.URL.RequestURI() + ":" + bodyDigest

	sig, err := privKey.Sign([]byte(payload))
	require.NoError(t, err)

	pubBytes, err := privKey.GetPublic().Raw()
	require.NoError(t, err)

	req.Header.Set(peerAuthHeaderPubKey, hex.EncodeToString(pubBytes))
	req.Header.Set(peerAuthHeaderTimestamp, ts)
	req.Header.Set(util.PeerAuthBodyDigestHeader, bodyDigest)
	req.Header.Set(peerAuthHeaderSignature, hex.EncodeToString(sig))
}

// newAuthEcho builds an Echo server with the peer-auth verifier mounted. The
// allowlist parameter is the set of peer IDs eligible for tier elevation; pass
// allowAll() in tests that don't care about gating, or pass a specific set to
// exercise allowlist behaviour. Pass nil for the empty-allowlist case (every
// authenticated peer drops to tierUnverified).
func newAuthEcho(t *testing.T, cache *peerTierCache, allowlist map[peer.ID]struct{}) (*echo.Echo, *peerTier) {
	t.Helper()
	logger := ulogger.TestLogger{}
	e := echo.New()
	captured := new(peerTier)
	e.Use(newPeerAuthVerifier(logger, cache, allowlist).Middleware())
	e.Any("/test", func(c echo.Context) error {
		*captured = c.Get("peer_tier").(peerTier)
		return c.NoContent(http.StatusOK)
	})
	return e, captured
}

// allowAll returns an allowlist containing every peer ID present in the given
// tier cache — used by tests that exercise the verify path rather than the
// gating path.
func allowAll(cache *peerTierCache) map[peer.ID]struct{} {
	out := make(map[peer.ID]struct{}, len(cache.tiers))
	for id := range cache.tiers {
		out[id] = struct{}{}
	}
	return out
}

func TestPeerAuthMiddleware_NoHeaders(t *testing.T) {
	cache := &peerTierCache{tiers: map[peer.ID]peerTier{}, logger: ulogger.TestLogger{}}
	e, captured := newAuthEcho(t, cache, allowAll(cache))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierUnverified, *captured)
}

func TestPeerAuthMiddleware_ValidMinerSignature(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierMiner}, logger: ulogger.TestLogger{}}
	e, captured := newAuthEcho(t, cache, allowAll(cache))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	signTestRequest(t, req, privKey)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierMiner, *captured)
}

func TestPeerAuthMiddleware_ValidPeerSignature(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierPeer}, logger: ulogger.TestLogger{}}
	e, captured := newAuthEcho(t, cache, allowAll(cache))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	signTestRequest(t, req, privKey)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierPeer, *captured)
}

func TestPeerAuthMiddleware_InvalidSignature(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierMiner}, logger: ulogger.TestLogger{}}
	e, captured := newAuthEcho(t, cache, allowAll(cache))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	signTestRequest(t, req, privKey)
	req.Header.Set(peerAuthHeaderSignature, hex.EncodeToString([]byte("invalidsignaturedata")))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierUnverified, *captured)
}

func TestPeerAuthMiddleware_ExpiredTimestamp(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierMiner}, logger: ulogger.TestLogger{}}
	e, captured := newAuthEcho(t, cache, allowAll(cache))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	// Build headers manually with a timestamp 30s in the past (outside the 10s window).
	expiredTs := strconv.FormatInt(time.Now().Unix()-30, 10)
	digest := util.EmptyBodySHA256Hex
	payload := "v2:" + expiredTs + ":" + req.Host + ":" + req.Method + ":" + req.URL.RequestURI() + ":" + digest
	sig, err := privKey.Sign([]byte(payload))
	require.NoError(t, err)
	pubBytes, err := privKey.GetPublic().Raw()
	require.NoError(t, err)
	req.Header.Set(peerAuthHeaderPubKey, hex.EncodeToString(pubBytes))
	req.Header.Set(peerAuthHeaderTimestamp, expiredTs)
	req.Header.Set(util.PeerAuthBodyDigestHeader, digest)
	req.Header.Set(peerAuthHeaderSignature, hex.EncodeToString(sig))

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierUnverified, *captured)
}

func TestPeerAuthMiddleware_UnknownPeer(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{}, logger: ulogger.TestLogger{}}
	e, captured := newAuthEcho(t, cache, allowAll(cache))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	signTestRequest(t, req, privKey)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierUnverified, *captured)
}

// TestPeerAuthMiddleware_RejectV1Payload — the old `ts:METHOD:Path` payload
// from before this hardening must not verify under the new v2 verifier.
func TestPeerAuthMiddleware_RejectV1Payload(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierMiner}, logger: ulogger.TestLogger{}}
	e, captured := newAuthEcho(t, cache, allowAll(cache))

	req := httptest.NewRequest(http.MethodGet, "/test?from=1&to=99", nil)
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	// Old v1 payload format.
	v1Payload := ts + ":" + req.Method + ":" + req.URL.Path
	sig, err := privKey.Sign([]byte(v1Payload))
	require.NoError(t, err)
	pubBytes, err := privKey.GetPublic().Raw()
	require.NoError(t, err)
	req.Header.Set(peerAuthHeaderPubKey, hex.EncodeToString(pubBytes))
	req.Header.Set(peerAuthHeaderTimestamp, ts)
	req.Header.Set(util.PeerAuthBodyDigestHeader, util.EmptyBodySHA256Hex)
	req.Header.Set(peerAuthHeaderSignature, hex.EncodeToString(sig))

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierUnverified, *captured, "v1 payload signatures must not be accepted by the v2 verifier")
}

// TestPeerAuthMiddleware_BodyDigestMismatch — a signature whose digest header
// disagrees with the actual body must not verify, even if the cryptographic
// signature over the declared digest is correct.
func TestPeerAuthMiddleware_BodyDigestMismatch(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierMiner}, logger: ulogger.TestLogger{}}
	e, captured := newAuthEcho(t, cache, allowAll(cache))

	realBody := []byte(`{"x":1}`)
	req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader(realBody))

	// Sign over a *different* body's digest.
	fakeBody := []byte(`{"x":2}`)
	fakeDigest := sha256.Sum256(fakeBody)
	fakeDigestHex := hex.EncodeToString(fakeDigest[:])
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	payload := "v2:" + ts + ":" + req.Host + ":" + req.Method + ":" + req.URL.RequestURI() + ":" + fakeDigestHex
	sig, err := privKey.Sign([]byte(payload))
	require.NoError(t, err)
	pubBytes, err := privKey.GetPublic().Raw()
	require.NoError(t, err)
	req.Header.Set(peerAuthHeaderPubKey, hex.EncodeToString(pubBytes))
	req.Header.Set(peerAuthHeaderTimestamp, ts)
	req.Header.Set(util.PeerAuthBodyDigestHeader, fakeDigestHex)
	req.Header.Set(peerAuthHeaderSignature, hex.EncodeToString(sig))

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierUnverified, *captured, "digest header that disagrees with body must be rejected")
}

// TestPeerAuthMiddleware_QueryStringBound — same path with different query
// string must require a fresh signature.
func TestPeerAuthMiddleware_QueryStringBound(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierMiner}, logger: ulogger.TestLogger{}}
	e, captured := newAuthEcho(t, cache, allowAll(cache))

	// Sign for one query string...
	signedReq := httptest.NewRequest(http.MethodGet, "/test?from=1", nil)
	signTestRequest(t, signedReq, privKey)

	// ...then replay against a different query string with the same headers.
	replayReq := httptest.NewRequest(http.MethodGet, "/test?from=999", nil)
	for k, v := range signedReq.Header {
		replayReq.Header[k] = v
	}

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, replayReq)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierUnverified, *captured, "signature is bound to query string; mismatched replay must fail")
}

// TestPeerAuthMiddleware_ReplayBlocked — submitting the same signed headers
// twice within the replay-cache TTL must succeed once and fail once.
func TestPeerAuthMiddleware_ReplayBlocked(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierMiner}, logger: ulogger.TestLogger{}}

	logger := ulogger.TestLogger{}
	verifier := newPeerAuthVerifier(logger, cache, allowAll(cache))
	e := echo.New()
	var capturedTier peerTier
	e.Use(verifier.Middleware())
	e.GET("/test", func(c echo.Context) error {
		capturedTier = c.Get("peer_tier").(peerTier)
		return c.NoContent(http.StatusOK)
	})

	// Sign once, then replay the same headers in a second request.
	original := httptest.NewRequest(http.MethodGet, "/test", nil)
	signTestRequest(t, original, privKey)

	// First submission: authenticated.
	rec1 := httptest.NewRecorder()
	e.ServeHTTP(rec1, original)
	require.Equal(t, http.StatusOK, rec1.Code)
	require.Equal(t, tierMiner, capturedTier)

	// Replay: same headers, fresh request — must drop to unverified.
	replay := httptest.NewRequest(http.MethodGet, "/test", nil)
	for k, v := range original.Header {
		replay.Header[k] = v
	}
	rec2 := httptest.NewRecorder()
	e.ServeHTTP(rec2, replay)
	require.Equal(t, http.StatusOK, rec2.Code)
	require.Equal(t, tierUnverified, capturedTier, "second submission of the same signature must be rejected")
}

// TestPeerAuthMiddleware_AllowlistEmpty_NoElevation — a valid, fresh, non-replayed
// signature whose peer is NOT in the allowlist must drop to tierUnverified.
func TestPeerAuthMiddleware_AllowlistEmpty_NoElevation(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	// Registry says this peer is a miner — but allowlist is empty.
	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierMiner}, logger: ulogger.TestLogger{}}
	e, captured := newAuthEcho(t, cache, nil) // empty allowlist

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	signTestRequest(t, req, privKey)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierUnverified, *captured, "empty allowlist must deny tier elevation")
}

// TestPeerAuthMiddleware_AllowlistMember_GetsTier — a valid signature from a
// peer in the allowlist gets the registry-derived tier.
func TestPeerAuthMiddleware_AllowlistMember_GetsTier(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierMiner}, logger: ulogger.TestLogger{}}
	allowlist := map[peer.ID]struct{}{peerID: {}}
	e, captured := newAuthEcho(t, cache, allowlist)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	signTestRequest(t, req, privKey)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierMiner, *captured)
}

// TestParsePeerAuthAllowlist — parsing of the pipe-separated config value.
func TestParsePeerAuthAllowlist(t *testing.T) {
	logger := ulogger.TestLogger{}

	t.Run("empty input returns empty set", func(t *testing.T) {
		got := parsePeerAuthAllowlist(logger, "")
		require.Empty(t, got)
	})

	t.Run("valid peer IDs are decoded", func(t *testing.T) {
		// Generate a couple of real peer IDs to feed in.
		k1, _, err := crypto.GenerateEd25519Key(rand.Reader)
		require.NoError(t, err)
		p1, err := peer.IDFromPublicKey(k1.GetPublic())
		require.NoError(t, err)

		k2, _, err := crypto.GenerateEd25519Key(rand.Reader)
		require.NoError(t, err)
		p2, err := peer.IDFromPublicKey(k2.GetPublic())
		require.NoError(t, err)

		raw := p1.String() + " | " + p2.String() + "||" // mix in whitespace and empty entries
		got := parsePeerAuthAllowlist(logger, raw)
		require.Len(t, got, 2)
		_, ok := got[p1]
		require.True(t, ok)
		_, ok = got[p2]
		require.True(t, ok)
	})

	t.Run("invalid entries are skipped", func(t *testing.T) {
		got := parsePeerAuthAllowlist(logger, "definitely-not-a-peer-id")
		require.Empty(t, got, "garbage entries must not enter the allowlist")
	})
}

// TestPeerAuthMiddleware_BodyPassesThroughToHandler — after digest verification,
// the handler must still receive the original body bytes.
func TestPeerAuthMiddleware_BodyPassesThroughToHandler(t *testing.T) {
	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierPeer}, logger: ulogger.TestLogger{}}

	logger := ulogger.TestLogger{}
	e := echo.New()
	var received []byte
	e.Use(newPeerAuthVerifier(logger, cache, allowAll(cache)).Middleware())
	e.POST("/test", func(c echo.Context) error {
		buf, err := io.ReadAll(c.Request().Body)
		if err != nil {
			return err
		}
		received = buf
		return c.NoContent(http.StatusOK)
	})

	body := []byte(`{"txids":["abc","def"]}`)
	req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader(body))
	signTestRequest(t, req, privKey)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, body, received, "handler must still observe the original body after digest check")
}

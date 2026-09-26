package httpimpl

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/p2p"
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

// registryStubP2PClient is a p2p.ClientI that only answers GetPeerRegistry.
type registryStubP2PClient struct {
	p2p.ClientI
	peers []*p2p.PeerInfo
}

func (c *registryStubP2PClient) GetPeerRegistry(_ context.Context) ([]*p2p.PeerInfo, error) {
	return c.peers, nil
}

// TestPeerTierCache_Refresh_ClassifiesByBlocksReceived — refresh's
// classification predicate: tierMiner requires BlocksReceived > 0 together with
// ReputationScore >= threshold (inclusive); every other registered peer is
// tierPeer and an unknown peer stays tierUnverified. The guard that
// BlocksReceived actually survives the p2p gRPC hop lives in
// services/p2p/server_handler_test.go (TestServer_GetPeerRegistry_ReceivedCountersSurviveWire).
func TestPeerTierCache_Refresh_ClassifiesByBlocksReceived(t *testing.T) {
	newPeerID := func() peer.ID {
		k, _, err := crypto.GenerateEd25519Key(rand.Reader)
		require.NoError(t, err)
		id, err := peer.IDFromPublicKey(k.GetPublic())
		require.NoError(t, err)
		return id
	}

	miner := newPeerID()
	atThreshold := newPeerID()
	noBlocks := newPeerID()
	lowRep := newPeerID()
	unknown := newPeerID()

	client := &registryStubP2PClient{peers: []*p2p.PeerInfo{
		{ID: miner, BlocksReceived: 1, ReputationScore: 90},
		{ID: atThreshold, BlocksReceived: 1, ReputationScore: 50},
		{ID: noBlocks, BlocksReceived: 0, ReputationScore: 100},
		{ID: lowRep, BlocksReceived: 50, ReputationScore: 10},
	}}

	cache := newPeerTierCache(ulogger.TestLogger{}, client, 50)
	cache.refresh(context.Background())

	require.Equal(t, tierMiner, cache.GetTier(miner))
	require.Equal(t, tierMiner, cache.GetTier(atThreshold), "reputation exactly at threshold qualifies")
	require.Equal(t, tierPeer, cache.GetTier(noBlocks), "no blocks received must not be a miner")
	require.Equal(t, tierPeer, cache.GetTier(lowRep), "reputation below threshold must not be a miner")
	require.Equal(t, tierUnverified, cache.GetTier(unknown))
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

// TestPeerAuthMiddleware_ReplayBlockedAcrossHexCase — hex is case-insensitive,
// so an upper-cased spelling of the same credential headers decodes to the same
// key and signature and must hit the same replay entry.
func TestPeerAuthMiddleware_ReplayBlockedAcrossHexCase(t *testing.T) {
	initPrometheusMetrics()

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierMiner}, logger: ulogger.TestLogger{}}
	e, captured := newAuthEcho(t, cache, allowAll(cache))

	original := httptest.NewRequest(http.MethodGet, "/test", nil)
	signTestRequest(t, original, privKey)

	rec1 := httptest.NewRecorder()
	e.ServeHTTP(rec1, original)
	require.Equal(t, http.StatusOK, rec1.Code)
	require.Equal(t, tierMiner, *captured)

	// Same credentials, upper-cased hex spelling.
	replay := httptest.NewRequest(http.MethodGet, "/test", nil)
	for k, v := range original.Header {
		replay.Header[k] = v
	}
	replay.Header.Set(peerAuthHeaderPubKey, strings.ToUpper(original.Header.Get(peerAuthHeaderPubKey)))
	replay.Header.Set(peerAuthHeaderSignature, strings.ToUpper(original.Header.Get(peerAuthHeaderSignature)))

	rec2 := httptest.NewRecorder()
	e.ServeHTTP(rec2, replay)
	require.Equal(t, http.StatusOK, rec2.Code)
	require.Equal(t, tierUnverified, *captured, "hex case variant of the same signature must be treated as a replay")
}

// TestPeerAuthMiddleware_ClaimReleasedOnFailure_AllowsRetryWithSameSignature —
// a signed request that fails auth AFTER the replay claim is taken (a body
// digest mismatch, in this case) must release its claim, so a later request
// carrying the exact same signature succeeds instead of being treated as a
// replay. This covers the claim-release defer in verifySignedRequest.
func TestPeerAuthMiddleware_ClaimReleasedOnFailure_AllowsRetryWithSameSignature(t *testing.T) {
	initPrometheusMetrics()

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierMiner}, logger: ulogger.TestLogger{}}
	e, captured := newAuthEcho(t, cache, allowAll(cache))

	realBody := []byte(`{"x":1}`)
	realDigest := sha256.Sum256(realBody)
	realDigestHex := hex.EncodeToString(realDigest[:])

	// Sign over the digest of realBody, but send a different body on the
	// first attempt so verification fails at the digest-mismatch step, which
	// runs after the replay claim is taken.
	wrongBody := []byte(`{"x":2}`)
	ts := strconv.FormatInt(time.Now().Unix(), 10)

	failReq := httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader(wrongBody))
	payload := "v2:" + ts + ":" + failReq.Host + ":" + failReq.Method + ":" + failReq.URL.RequestURI() + ":" + realDigestHex
	sig, err := privKey.Sign([]byte(payload))
	require.NoError(t, err)
	pubBytes, err := privKey.GetPublic().Raw()
	require.NoError(t, err)
	failReq.Header.Set(peerAuthHeaderPubKey, hex.EncodeToString(pubBytes))
	failReq.Header.Set(peerAuthHeaderTimestamp, ts)
	failReq.Header.Set(util.PeerAuthBodyDigestHeader, realDigestHex)
	failReq.Header.Set(peerAuthHeaderSignature, hex.EncodeToString(sig))

	rec1 := httptest.NewRecorder()
	e.ServeHTTP(rec1, failReq)
	require.Equal(t, http.StatusOK, rec1.Code)
	require.Equal(t, tierUnverified, *captured, "digest mismatch must fail auth")

	// Retry with the SAME signature and timestamp, this time with the body
	// that actually matches the declared digest. If the claim wasn't released
	// on the first failure, this would be rejected as a replay.
	retryReq := httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader(realBody))
	retryReq.Header.Set(peerAuthHeaderPubKey, hex.EncodeToString(pubBytes))
	retryReq.Header.Set(peerAuthHeaderTimestamp, ts)
	retryReq.Header.Set(util.PeerAuthBodyDigestHeader, realDigestHex)
	retryReq.Header.Set(peerAuthHeaderSignature, hex.EncodeToString(sig))

	rec2 := httptest.NewRecorder()
	e.ServeHTTP(rec2, retryReq)
	require.Equal(t, http.StatusOK, rec2.Code)
	require.Equal(t, tierMiner, *captured, "claim must be released on failure so the same signature can succeed on retry")
}

// TestPeerAuthMiddleware_ConcurrentReplayElevatesOnce — the replay claim must be
// atomic: racing copies of one signed request must yield exactly one elevation.
// Concurrency is the defect under test, so goroutines here are deliberate.
func TestPeerAuthMiddleware_ConcurrentReplayElevatesOnce(t *testing.T) {
	initPrometheusMetrics()

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierMiner}, logger: ulogger.TestLogger{}}

	var elevated atomic.Int64
	e := echo.New()
	e.Use(newPeerAuthVerifier(ulogger.TestLogger{}, cache, allowAll(cache)).Middleware())
	e.POST("/test", func(c echo.Context) error {
		if c.Get("peer_tier").(peerTier) != tierUnverified {
			elevated.Add(1)
		}
		return c.NoContent(http.StatusOK)
	})

	body := bytes.Repeat([]byte("a"), 64*1024)
	signed := httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader(body))
	signTestRequest(t, signed, privKey)

	const copies = 24
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(copies)
	for i := 0; i < copies; i++ {
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader(body))
			for k, v := range signed.Header {
				req.Header[k] = v
			}
			<-start
			e.ServeHTTP(httptest.NewRecorder(), req)
		}()
	}
	close(start)
	wg.Wait()

	require.Equal(t, int64(1), elevated.Load(), "exactly one racing copy of a signed request may be elevated")
}

// TestResolveMaxSignedBodyBytes_TracksCatchupBatchSize — the cap must grow
// with subtreevalidation's batch size (32 bytes per hash), never shrink below
// the default floor, and stay at the floor for non-positive input.
func TestResolveMaxSignedBodyBytes_TracksCatchupBatchSize(t *testing.T) {
	require.Equal(t, int64(defaultMaxSignedBodyBytes), resolveMaxSignedBodyBytes(0))
	require.Equal(t, int64(defaultMaxSignedBodyBytes), resolveMaxSignedBodyBytes(-1))
	require.Equal(t, int64(defaultMaxSignedBodyBytes), resolveMaxSignedBodyBytes(16384), "16384*32 = 512KiB, below the floor")

	const batchSize = 65536 // 65536*32 = 2MiB, above the default 1MiB floor
	want := int64(32*batchSize) + signedBodyCapHeadroom
	require.Equal(t, want, resolveMaxSignedBodyBytes(batchSize))
	require.Greater(t, resolveMaxSignedBodyBytes(batchSize), int64(defaultMaxSignedBodyBytes))
}

// TestPeerAuthMiddleware_LargeCatchupBodyAcceptedWithRaisedBatchSize — a
// signed POST body over the default 1 MiB cap but within 32*batchSize must
// pass once the verifier is constructed with the batch-derived cap, matching
// what an allowlisted peer's catchup fetch sends when
// subtreevalidation_missingTransactionsBatchSize is raised above 32768.
func TestPeerAuthMiddleware_LargeCatchupBodyAcceptedWithRaisedBatchSize(t *testing.T) {
	initPrometheusMetrics()

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierPeer}, logger: ulogger.TestLogger{}}

	const batchSize = 65536 // raised subtreevalidation_missingTransactionsBatchSize
	bodyCap := resolveMaxSignedBodyBytes(batchSize)

	logger := ulogger.TestLogger{}
	e := echo.New()
	handlerCalled := false
	e.Use(newPeerAuthVerifierWithBodyCap(logger, cache, allowAll(cache), bodyCap).Middleware())
	e.POST("/test", func(c echo.Context) error {
		handlerCalled = true
		return c.NoContent(http.StatusOK)
	})

	// Bigger than the default 1 MiB cap, well within 32*batchSize.
	body := bytes.Repeat([]byte("q"), defaultMaxSignedBodyBytes+(512*1024))
	req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader(body))
	signTestRequest(t, req, privKey)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "an allowlisted peer's large catchup body must not be rejected once the cap tracks the batch size")
	require.True(t, handlerCalled)
}

// countingBody records how many bytes the middleware pulled off the request body.
type countingBody struct {
	r    io.Reader
	read atomic.Int64
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.read.Add(int64(n))
	return n, err
}

func (b *countingBody) Close() error { return nil }

// TestPeerAuthMiddleware_NonAllowlistedBodyNotRead — an attacker-generated key
// is cheap to mint, so a peer that cannot be elevated must not cost a full body
// read and SHA-256 before it is rejected.
func TestPeerAuthMiddleware_NonAllowlistedBodyNotRead(t *testing.T) {
	initPrometheusMetrics()

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	// Registry knows the peer, but the operator has not allowlisted it.
	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierMiner}, logger: ulogger.TestLogger{}}
	e, captured := newAuthEcho(t, cache, nil)

	body := bytes.Repeat([]byte("z"), 256*1024)
	req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader(body))
	signTestRequest(t, req, privKey)

	counter := &countingBody{r: bytes.NewReader(body)}
	req.Body = counter
	req.ContentLength = int64(len(body))

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierUnverified, *captured)
	require.Zero(t, counter.read.Load(), "a peer that cannot be elevated must be rejected before the body is read")
}

// TestPeerAuthMiddleware_BadSignatureBodyNotRead — a signature that does not
// verify must be rejected before the body is buffered and hashed.
func TestPeerAuthMiddleware_BadSignatureBodyNotRead(t *testing.T) {
	initPrometheusMetrics()

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierMiner}, logger: ulogger.TestLogger{}}
	e, captured := newAuthEcho(t, cache, allowAll(cache))

	body := bytes.Repeat([]byte("z"), 256*1024)
	req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader(body))
	signTestRequest(t, req, privKey)
	req.Header.Set(peerAuthHeaderSignature, hex.EncodeToString(make([]byte, 64)))

	counter := &countingBody{r: bytes.NewReader(body)}
	req.Body = counter
	req.ContentLength = int64(len(body))

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierUnverified, *captured)
	require.Zero(t, counter.read.Load(), "an unverifiable signature must be rejected before the body is read")
}

// TestPeerAuthMiddleware_OversizedSignedBodyRejected — a signed body above
// maxSignedBodyBytes is refused outright rather than buffered.
func TestPeerAuthMiddleware_OversizedSignedBodyRejected(t *testing.T) {
	initPrometheusMetrics()

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{tiers: map[peer.ID]peerTier{peerID: tierMiner}, logger: ulogger.TestLogger{}}

	logger := ulogger.TestLogger{}
	e := echo.New()
	handlerCalled := false
	e.Use(newPeerAuthVerifier(logger, cache, allowAll(cache)).Middleware())
	e.POST("/test", func(c echo.Context) error {
		handlerCalled = true
		return c.NoContent(http.StatusOK)
	})

	body := bytes.Repeat([]byte("q"), defaultMaxSignedBodyBytes+1)
	req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader(body))
	signTestRequest(t, req, privKey)

	counter := &countingBody{r: bytes.NewReader(body)}
	req.Body = counter
	req.ContentLength = int64(len(body))

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	require.False(t, handlerCalled, "oversized signed request must not reach the handler")
	require.Zero(t, counter.read.Load(), "oversized signed body must be rejected on the declared length, not buffered")
}

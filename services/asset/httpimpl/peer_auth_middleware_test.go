package httpimpl

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// signTestRequest signs an HTTP request with the given Ed25519 private key,
// setting the X-Peer-PubKey, X-Peer-Timestamp, and X-Peer-Signature headers.
func signTestRequest(t *testing.T, req *http.Request, privKey crypto.PrivKey) {
	t.Helper()
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	payload := ts + ":" + req.Method + ":" + req.URL.Path
	sig, err := privKey.Sign([]byte(payload))
	require.NoError(t, err)
	pubBytes, err := privKey.GetPublic().Raw()
	require.NoError(t, err)
	req.Header.Set("X-Peer-PubKey", hex.EncodeToString(pubBytes))
	req.Header.Set("X-Peer-Timestamp", ts)
	req.Header.Set("X-Peer-Signature", hex.EncodeToString(sig))
}

func TestPeerAuthMiddleware_NoHeaders(t *testing.T) {
	logger := ulogger.TestLogger{}
	cache := &peerTierCache{
		tiers:  make(map[peer.ID]peerTier),
		logger: logger,
	}

	e := echo.New()
	var capturedTier peerTier
	e.Use(peerAuthMiddleware(logger, cache))
	e.GET("/test", func(c echo.Context) error {
		capturedTier = c.Get("peer_tier").(peerTier)
		return c.NoContent(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierUnverified, capturedTier)
}

func TestPeerAuthMiddleware_ValidMinerSignature(t *testing.T) {
	logger := ulogger.TestLogger{}

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{
		tiers:  map[peer.ID]peerTier{peerID: tierMiner},
		logger: logger,
	}

	e := echo.New()
	var capturedTier peerTier
	e.Use(peerAuthMiddleware(logger, cache))
	e.GET("/test", func(c echo.Context) error {
		capturedTier = c.Get("peer_tier").(peerTier)
		return c.NoContent(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	signTestRequest(t, req, privKey)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierMiner, capturedTier)
}

func TestPeerAuthMiddleware_ValidPeerSignature(t *testing.T) {
	logger := ulogger.TestLogger{}

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{
		tiers:  map[peer.ID]peerTier{peerID: tierPeer},
		logger: logger,
	}

	e := echo.New()
	var capturedTier peerTier
	e.Use(peerAuthMiddleware(logger, cache))
	e.GET("/test", func(c echo.Context) error {
		capturedTier = c.Get("peer_tier").(peerTier)
		return c.NoContent(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	signTestRequest(t, req, privKey)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierPeer, capturedTier)
}

func TestPeerAuthMiddleware_InvalidSignature(t *testing.T) {
	logger := ulogger.TestLogger{}

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{
		tiers:  map[peer.ID]peerTier{peerID: tierMiner},
		logger: logger,
	}

	e := echo.New()
	var capturedTier peerTier
	e.Use(peerAuthMiddleware(logger, cache))
	e.GET("/test", func(c echo.Context) error {
		capturedTier = c.Get("peer_tier").(peerTier)
		return c.NoContent(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	signTestRequest(t, req, privKey)
	// Corrupt the signature by replacing it with garbage.
	req.Header.Set("X-Peer-Signature", hex.EncodeToString([]byte("invalidsignaturedata")))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierUnverified, capturedTier)
}

func TestPeerAuthMiddleware_ExpiredTimestamp(t *testing.T) {
	logger := ulogger.TestLogger{}

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	peerID, err := peer.IDFromPublicKey(privKey.GetPublic())
	require.NoError(t, err)

	cache := &peerTierCache{
		tiers:  map[peer.ID]peerTier{peerID: tierMiner},
		logger: logger,
	}

	e := echo.New()
	var capturedTier peerTier
	e.Use(peerAuthMiddleware(logger, cache))
	e.GET("/test", func(c echo.Context) error {
		capturedTier = c.Get("peer_tier").(peerTier)
		return c.NoContent(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	// Manually build headers with a timestamp 120 seconds in the past.
	expiredTs := strconv.FormatInt(time.Now().Unix()-120, 10)
	payload := expiredTs + ":" + req.Method + ":" + req.URL.Path
	sig, err := privKey.Sign([]byte(payload))
	require.NoError(t, err)
	pubBytes, err := privKey.GetPublic().Raw()
	require.NoError(t, err)
	req.Header.Set("X-Peer-PubKey", hex.EncodeToString(pubBytes))
	req.Header.Set("X-Peer-Timestamp", expiredTs)
	req.Header.Set("X-Peer-Signature", hex.EncodeToString(sig))

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierUnverified, capturedTier)
}

func TestPeerAuthMiddleware_UnknownPeer(t *testing.T) {
	logger := ulogger.TestLogger{}

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	// Do not add this peer to the cache — it should remain unknown.
	cache := &peerTierCache{
		tiers:  make(map[peer.ID]peerTier),
		logger: logger,
	}

	e := echo.New()
	var capturedTier peerTier
	e.Use(peerAuthMiddleware(logger, cache))
	e.GET("/test", func(c echo.Context) error {
		capturedTier = c.Get("peer_tier").(peerTier)
		return c.NoContent(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	signTestRequest(t, req, privKey)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tierUnverified, capturedTier)
}

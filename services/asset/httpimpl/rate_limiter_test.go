package httpimpl

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func init() {
	// The rate limiter records a Prometheus counter on 429 responses, so the
	// metrics must be initialised before the middleware is exercised.
	initPrometheusMetrics()
}

// setTierMiddleware injects a fixed tier (and optional peer ID) into the echo
// context before the rate limiter sees the request.
func setTierMiddleware(tier peerTier, peerID string) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set("peer_tier", tier)
			if peerID != "" {
				c.Set("peer_id", peerID)
			}
			return next(c)
		}
	}
}

func TestTieredRateLimiter_UnverifiedGetsLimited(t *testing.T) {
	e := echo.New()
	e.Use(setTierMiddleware(tierUnverified, ""))
	e.Use(newTieredRateLimiter(2, 1, 0, "test").Middleware())
	e.GET("/test", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		e.ServeHTTP(rec, req)

		if i < 2 {
			require.Equal(t, http.StatusOK, rec.Code, "request %d should succeed", i+1)
		} else {
			require.Equal(t, http.StatusTooManyRequests, rec.Code, "request %d should be rate-limited", i+1)
		}
	}
}

// TestTieredRateLimiter_MinerExempt — minerRate=0 preserves the original
// fully-exempt behaviour for miners.
func TestTieredRateLimiter_MinerExempt(t *testing.T) {
	e := echo.New()
	e.Use(setTierMiddleware(tierMiner, "peer-A"))
	e.Use(newTieredRateLimiter(1, 1, 0, "test").Middleware())
	e.GET("/test", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		e.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "miner request %d should always succeed", i+1)
	}
}

// TestTieredRateLimiter_MinerCappedWhenConfigured — minerRate>0 enforces a
// per-peer cap so a compromised miner key can't unlock unlimited rate.
func TestTieredRateLimiter_MinerCappedWhenConfigured(t *testing.T) {
	e := echo.New()
	e.Use(setTierMiddleware(tierMiner, "peer-A"))
	e.Use(newTieredRateLimiter(1, 1, 2, "test").Middleware())
	e.GET("/test", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	gotLimited := false
	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		e.ServeHTTP(rec, req)
		if rec.Code == http.StatusTooManyRequests {
			gotLimited = true
			break
		}
	}
	require.True(t, gotLimited, "miner-tier traffic must be capped when minerRate > 0")
}

func TestTieredRateLimiter_PeerGetsHigherRate(t *testing.T) {
	e := echo.New()
	e.Use(setTierMiddleware(tierPeer, "peer-A"))
	e.Use(newTieredRateLimiter(1, 5, 0, "test").Middleware())
	e.GET("/test", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		e.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "peer request %d should succeed (3 < 5 burst)", i+1)
	}
}

// TestTieredRateLimiter_AuthBucketKeyedByPeerID — two authenticated peers
// behind one IP get independent buckets. Without peer-ID keying, the first
// peer's traffic would consume the bucket and starve the second.
func TestTieredRateLimiter_AuthBucketKeyedByPeerID(t *testing.T) {
	rl := newTieredRateLimiter(1, 1, 0, "test") // rate 1 req/s, burst 1

	exhaust := func(peerID string) (firstOK, secondOK bool) {
		e := echo.New()
		e.Use(setTierMiddleware(tierPeer, peerID))
		e.Use(rl.Middleware())
		e.GET("/test", func(c echo.Context) error { return c.NoContent(http.StatusOK) })

		// Both requests share the same IP (httptest default) but different
		// peerIDs go through different buckets.
		rec1 := httptest.NewRecorder()
		e.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/test", nil))
		rec2 := httptest.NewRecorder()
		e.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/test", nil))
		return rec1.Code == http.StatusOK, rec2.Code == http.StatusOK
	}

	// Peer A: first request OK, second 429 (burst=1).
	a1, a2 := exhaust("peer-A")
	require.True(t, a1)
	require.False(t, a2, "peer-A's burst should be exhausted")

	// Peer B from the same IP: should still get its first OK because its
	// bucket is independent.
	b1, _ := exhaust("peer-B")
	require.True(t, b1, "peer-B must not share peer-A's bucket")
}

func TestTieredRateLimiter_DisabledWhenZero(t *testing.T) {
	e := echo.New()
	e.Use(setTierMiddleware(tierUnverified, ""))
	e.Use(newTieredRateLimiter(0, 1, 0, "test").Middleware())
	e.GET("/test", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	for i := 0; i < 10; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		e.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "request %d should pass when rate limiting is disabled", i+1)
	}
}

// TestUnverifiedKey_IPv6Normalisation — two distinct /128 addresses inside
// the same /64 must collapse to a single bucket key.
func TestUnverifiedKey_IPv6Normalisation(t *testing.T) {
	a := unverifiedKey("2001:db8:abcd:0012::1")
	b := unverifiedKey("2001:db8:abcd:0012:ffff:ffff:ffff:fffe")
	require.Equal(t, a, b, "addresses within the same /64 must share a bucket key")

	c := unverifiedKey("2001:db8:abcd:0013::1")
	require.NotEqual(t, a, c, "different /64s must produce different keys")

	// IPv4 stays full precision.
	require.Equal(t, "10.0.0.1", unverifiedKey("10.0.0.1"))
	require.NotEqual(t, unverifiedKey("10.0.0.1"), unverifiedKey("10.0.0.2"))
}

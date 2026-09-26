package httpimpl

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
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
	e.Use(newTieredRateLimiter(2, 1, 0, 0, "test").Middleware())
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
	e.Use(newTieredRateLimiter(1, 1, 0, 0, "test").Middleware())
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
	e.Use(newTieredRateLimiter(1, 1, 2, 0, "test").Middleware())
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
	e.Use(newTieredRateLimiter(1, 5, 0, 0, "test").Middleware())
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
	rl := newTieredRateLimiter(1, 1, 0, 0, "test") // rate 1 req/s, burst 1

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
	e.Use(newTieredRateLimiter(0, 1, 0, 0, "test").Middleware())
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

// TestTieredRateLimiter_AuthFallbackUsesDefaultRate — defensive fallback for
// the "authenticated tier with no peer_id" path. This shouldn't happen in
// practice (the auth middleware always sets peer_id alongside the tier), but
// if a wiring bug ever drops peer_id we want to land in the *unverified*
// bucket at defaultRate — not at minerRate or peerRate. Otherwise an
// unauthenticated request could grab an elevated bucket simply because some
// upstream middleware set peer_tier without setting peer_id.
func TestTieredRateLimiter_AuthFallbackUsesDefaultRate(t *testing.T) {
	// defaultRate=1, minerRate=50 — if the fallback wrongly uses minerRate,
	// many requests will succeed; with the correct defaultRate fallback the
	// second request gets rate-limited.
	rl := newTieredRateLimiter(1, 1, 50, 0, "test")

	e := echo.New()
	// Tier set but peer_id deliberately missing.
	e.Use(setTierMiddleware(tierMiner, ""))
	e.Use(rl.Middleware())
	e.GET("/test", func(c echo.Context) error { return c.NoContent(http.StatusOK) })

	rec1 := httptest.NewRecorder()
	e.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/test", nil))
	require.Equal(t, http.StatusOK, rec1.Code, "first request should pass at defaultRate=1")

	rec2 := httptest.NewRecorder()
	e.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/test", nil))
	require.Equal(t, http.StatusTooManyRequests, rec2.Code,
		"second request must be limited at defaultRate; if it succeeds the fallback is wrongly using minerRate")
}

// TestTieredRateLimiter_AuthFallbackNormalisesIPv6 — the auth-tier fallback
// path (peer_id missing) must use the same IPv6 /64 normalisation as the
// default-tier path. Otherwise two /128 addresses in the same /64 get
// independent buckets via this seam, partially undoing H2.
func TestTieredRateLimiter_AuthFallbackNormalisesIPv6(t *testing.T) {
	rl := newTieredRateLimiter(1, 1, 0, 0, "test")

	exhaust := func(remote string) (firstOK, secondOK bool) {
		e := echo.New()
		// Use Echo's XFF extractor so RealIP returns the X-Forwarded-For value.
		e.IPExtractor = echo.ExtractIPFromXFFHeader()
		e.Use(setTierMiddleware(tierPeer, "")) // tierPeer with no peer_id → fallback path
		e.Use(rl.Middleware())
		e.GET("/test", func(c echo.Context) error { return c.NoContent(http.StatusOK) })

		req1 := httptest.NewRequest(http.MethodGet, "/test", nil)
		req1.Header.Set("X-Forwarded-For", remote)
		rec1 := httptest.NewRecorder()
		e.ServeHTTP(rec1, req1)

		req2 := httptest.NewRequest(http.MethodGet, "/test", nil)
		req2.Header.Set("X-Forwarded-For", remote)
		rec2 := httptest.NewRecorder()
		e.ServeHTTP(rec2, req2)

		return rec1.Code == http.StatusOK, rec2.Code == http.StatusOK
	}

	// First /128 — exhausts its bucket.
	ok1, ok2 := exhaust("2001:db8:abcd:1234::1")
	require.True(t, ok1)
	require.False(t, ok2, "second request to the same /128 should be limited")

	// Different /128 in the same /64 — must share the bucket (already exhausted).
	ok1Same64, _ := exhaust("2001:db8:abcd:1234:ffff:ffff:ffff:fffe")
	require.False(t, ok1Same64,
		"different /128 in the same /64 must share the bucket; if it gets a fresh OK the fallback isn't normalising IPv6")
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

// TestTieredRateLimiter_MinBurstAdmitsCatchupFanOut — a catching-up peer fans
// out subtreevalidation_getMissingTransactions concurrent requests from one
// unsigned (and therefore tier-unverified) source IP. With burst pinned to the
// rate, the heavy limiter's default rate of 10 rejects the rest with 429 —
// which util/http.go never retries and subtree validation reports as an
// invalid subtree, costing an honest peer its reputation. minBurst must admit
// the whole fan-out.
func TestTieredRateLimiter_MinBurstAdmitsCatchupFanOut(t *testing.T) {
	t.Parallel()

	const (
		heavyRate = 10 // asset_httpHeavyRateLimit default
		fanOut    = 32 // subtreevalidation_getMissingTransactions in settings.conf
	)

	e := echo.New()
	e.Use(setTierMiddleware(tierUnverified, ""))
	e.Use(newTieredRateLimiter(heavyRate, 5, 0, fanOut, "test").Middleware())
	e.GET("/subtree/:hash/txs", func(c echo.Context) error { return c.NoContent(http.StatusOK) })

	var (
		mu       sync.Mutex
		limited  int
		start    = make(chan struct{})
		wg       sync.WaitGroup
		requests = fanOut
	)

	for i := 0; i < requests; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/subtree/abc/txs", nil))

			if rec.Code != http.StatusOK {
				mu.Lock()
				limited++
				mu.Unlock()
			}
		}()
	}

	close(start)
	wg.Wait()

	require.Zero(t, limited, "all %d concurrent catchup requests must be admitted", requests)
}

// TestTieredRateLimiter_MinBurstStillEnforcesSustainedRate — raising the burst
// must not turn the bucket into an unlimited one. Once the burst is spent the
// configured rate still binds.
func TestTieredRateLimiter_MinBurstStillEnforcesSustainedRate(t *testing.T) {
	const (
		heavyRate = 10
		minBurst  = 32
	)

	e := echo.New()
	e.Use(setTierMiddleware(tierUnverified, ""))
	e.Use(newTieredRateLimiter(heavyRate, 5, 0, minBurst, "test").Middleware())
	e.GET("/test", func(c echo.Context) error { return c.NoContent(http.StatusOK) })

	// Drain the burst serially — fast enough that refill is negligible.
	for i := 0; i < minBurst; i++ {
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/test", nil))
		require.Equal(t, http.StatusOK, rec.Code, "request %d is inside the burst", i+1)
	}

	// The bucket is empty; at 10/s a refill takes 100ms, so the immediately
	// following request must be rejected.
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/test", nil))
	require.Equal(t, http.StatusTooManyRequests, rec.Code,
		"the sustained rate must still bind once the burst is spent")
}

// TestTieredRateLimiter_MinBurstZeroKeepsRateAsBurst — minBurst <= 0 means
// burst == rate, i.e. exactly the pre-existing behaviour. The global limiter
// relies on this.
func TestTieredRateLimiter_MinBurstZeroKeepsRateAsBurst(t *testing.T) {
	e := echo.New()
	e.Use(setTierMiddleware(tierUnverified, ""))
	e.Use(newTieredRateLimiter(2, 1, 0, 0, "test").Middleware())
	e.GET("/test", func(c echo.Context) error { return c.NoContent(http.StatusOK) })

	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/test", nil))

		if i < 2 {
			require.Equal(t, http.StatusOK, rec.Code, "request %d is inside burst==rate", i+1)
		} else {
			require.Equal(t, http.StatusTooManyRequests, rec.Code, "burst must not exceed the rate when minBurst is 0")
		}
	}
}

// TestTieredRateLimiter_MinBurstAppliesToPeerBucket — the peer bucket gets the
// same floor. An authenticated peer must not end up with a smaller burst than
// an unverified one.
func TestTieredRateLimiter_MinBurstAppliesToPeerBucket(t *testing.T) {
	e := echo.New()
	e.Use(setTierMiddleware(tierPeer, "peer-A"))
	e.Use(newTieredRateLimiter(1, 1, 0, 16, "test").Middleware())
	e.GET("/test", func(c echo.Context) error { return c.NoContent(http.StatusOK) })

	for i := 0; i < 16; i++ {
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/test", nil))
		require.Equal(t, http.StatusOK, rec.Code, "peer request %d is inside the minBurst floor", i+1)
	}
}

func TestResolveHeavyBurst(t *testing.T) {
	tests := []struct {
		name       string
		configured int
		floor      int
		rate       int
		want       int
		wantWarn   bool
	}{
		{name: "unset takes the floor silently", configured: 0, floor: 32, rate: 10, want: 32, wantWarn: false},
		{name: "configured below the floor is respected, not raised, but warns", configured: 8, floor: 32, rate: 10, want: 8, wantWarn: true},
		{name: "configured above the floor is kept", configured: 64, floor: 32, rate: 10, want: 64, wantWarn: false},
		{name: "configured equal to the floor is kept", configured: 32, floor: 32, rate: 10, want: 32, wantWarn: false},
		{name: "no floor keeps the configured value", configured: 8, floor: 0, rate: 10, want: 8, wantWarn: false},
		{name: "floor above 4x rate is clamped, and warns because the burst no longer covers the fan-out", configured: 0, floor: 100, rate: 10, want: 40, wantWarn: true},
		{name: "configured above the clamped floor is kept, no warn", configured: 50, floor: 100, rate: 10, want: 50, wantWarn: false},
		{name: "configured below the clamped floor is respected and warns against the clamped value", configured: 20, floor: 100, rate: 10, want: 20, wantWarn: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := ulogger.NewVerboseTestLogger(t)
			got, warned := resolveHeavyBurst(logger, tt.configured, tt.floor, tt.rate)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantWarn, warned)
		})
	}
}

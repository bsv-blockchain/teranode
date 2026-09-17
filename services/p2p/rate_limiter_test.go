// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

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

func TestIPRateLimiter_LimitsPerSource(t *testing.T) {
	e := echo.New()
	e.Use(newIPRateLimiter(2).Middleware())
	e.GET("/test", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.RemoteAddr = "203.0.113.1:12345"
		e.ServeHTTP(rec, req)

		if i < 2 {
			require.Equal(t, http.StatusOK, rec.Code, "request %d should succeed", i+1)
		} else {
			require.Equal(t, http.StatusTooManyRequests, rec.Code, "request %d should be rate-limited", i+1)
		}
	}
}

func TestIPRateLimiter_SourcesAreIndependent(t *testing.T) {
	e := echo.New()
	e.Use(newIPRateLimiter(1).Middleware())
	e.GET("/test", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	for _, addr := range []string{"203.0.113.1:1", "203.0.113.2:1"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.RemoteAddr = addr
		e.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "first request from %s should succeed", addr)
	}
}

func TestIPRateLimiter_ZeroDisables(t *testing.T) {
	e := echo.New()
	e.Use(newIPRateLimiter(0).Middleware())
	e.GET("/test", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	for i := 0; i < 10; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.RemoteAddr = "203.0.113.1:12345"
		e.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "request %d should succeed with rate limiting disabled", i+1)
	}
}

func TestIPRateLimiter_NotKeyedOnForwardedForHeader(t *testing.T) {
	// The limiter must key on RemoteAddr, not any client-supplied header;
	// otherwise an attacker rotates X-Forwarded-For to get a fresh bucket
	// on every request and the limit never binds.
	e := echo.New()
	e.Use(newIPRateLimiter(1).Middleware())
	e.GET("/test", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.RemoteAddr = "203.0.113.1:12345"
		req.Header.Set("X-Forwarded-For", "10.0.0.1")
		e.ServeHTTP(rec, req)

		if i == 0 {
			require.Equal(t, http.StatusOK, rec.Code)
		} else {
			require.Equal(t, http.StatusTooManyRequests, rec.Code, "spoofed X-Forwarded-For must not grant a fresh bucket")
		}
	}
}

func TestRateLimiterKey_IPv6NormalisedToSlash64(t *testing.T) {
	a := rateLimiterKey("[2001:db8::1]:1234")
	b := rateLimiterKey("[2001:db8::2]:5678")
	require.Equal(t, a, b, "distinct addresses in the same /64 must share a bucket")

	c := rateLimiterKey("[2001:db8:1::1]:1234")
	require.NotEqual(t, a, c, "addresses in different /64s must not share a bucket")
}

func TestRateLimiterKey_IPv4Verbatim(t *testing.T) {
	require.Equal(t, "203.0.113.1", rateLimiterKey("203.0.113.1:1234"))
}

func TestRateLimiterKey_NoPortFallsBackToRawInput(t *testing.T) {
	require.Equal(t, "not-an-address", rateLimiterKey("not-an-address"))
}

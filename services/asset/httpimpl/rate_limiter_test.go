package httpimpl

import (
	"net/http"
	"net/http/httptest"
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

func TestTieredRateLimiter_UnverifiedGetsLimited(t *testing.T) {
	logger := ulogger.TestLogger{}
	e := echo.New()

	// Set tier before rate limiter sees the request.
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set("peer_tier", tierUnverified)
			return next(c)
		}
	})
	e.Use(tieredRateLimitMiddleware(logger, 2, 1, "test"))
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

func TestTieredRateLimiter_MinerExempt(t *testing.T) {
	logger := ulogger.TestLogger{}
	e := echo.New()

	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set("peer_tier", tierMiner)
			return next(c)
		}
	})
	e.Use(tieredRateLimitMiddleware(logger, 1, 1, "test"))
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

func TestTieredRateLimiter_PeerGetsHigherRate(t *testing.T) {
	logger := ulogger.TestLogger{}
	e := echo.New()

	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set("peer_tier", tierPeer)
			return next(c)
		}
	})
	e.Use(tieredRateLimitMiddleware(logger, 1, 5, "test"))
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

func TestTieredRateLimiter_DisabledWhenZero(t *testing.T) {
	logger := ulogger.TestLogger{}
	e := echo.New()

	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set("peer_tier", tierUnverified)
			return next(c)
		}
	})
	e.Use(tieredRateLimitMiddleware(logger, 0, 1, "test"))
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

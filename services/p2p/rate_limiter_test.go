// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// TestIPRateLimiter_AllowsUpToConfiguredRate verifies that a fresh bucket
// grants exactly ratePerSec requests (the initial burst) before rejecting.
func TestIPRateLimiter_AllowsUpToConfiguredRate(t *testing.T) {
	rl := newIPRateLimiter(3)

	for i := 0; i < 3; i++ {
		require.True(t, rl.allow("10.0.0.1"), "request %d should be within the configured rate", i+1)
	}
}

// TestIPRateLimiter_RejectsBeyondRate verifies that once the burst is
// exhausted, further immediate requests from the same IP are rejected.
func TestIPRateLimiter_RejectsBeyondRate(t *testing.T) {
	rl := newIPRateLimiter(2)

	require.True(t, rl.allow("10.0.0.1"))
	require.True(t, rl.allow("10.0.0.1"))
	require.False(t, rl.allow("10.0.0.1"), "request beyond the configured rate must be rejected")
}

// TestIPRateLimiter_KeysPerIP verifies that each source IP gets its own
// independent bucket, so one IP exhausting its rate doesn't affect another.
func TestIPRateLimiter_KeysPerIP(t *testing.T) {
	rl := newIPRateLimiter(1)

	require.True(t, rl.allow("10.0.0.1"))
	require.False(t, rl.allow("10.0.0.1"), "10.0.0.1's burst should already be exhausted")

	require.True(t, rl.allow("10.0.0.2"), "a different IP must not share 10.0.0.1's bucket")
}

// TestIPRateLimiter_ConcurrentFirstTouchDoesNotResetBucket verifies the
// per-key bucket survives a concurrent first-touch burst. An unconditional
// Add lets a race-losing goroutine overwrite a bucket another goroutine
// already installed and consumed from, handing the key a fresh full burst;
// with N goroutines racing on one previously-unseen key and a burst of B,
// the number of grants must still be exactly B.
func TestIPRateLimiter_ConcurrentFirstTouchDoesNotResetBucket(t *testing.T) {
	const (
		ratePerSec = 2
		goroutines = 16
		trials     = 500
	)

	var peakGranted int64

	for trial := 0; trial < trials; trial++ {
		rl := newIPRateLimiter(ratePerSec)

		var (
			ready   sync.WaitGroup
			start   sync.WaitGroup
			done    sync.WaitGroup
			granted int64
		)

		start.Add(1)

		for g := 0; g < goroutines; g++ {
			ready.Add(1)
			done.Add(1)

			go func() {
				defer done.Done()

				ready.Done()
				start.Wait()

				if rl.allow("10.0.0.1") {
					atomic.AddInt64(&granted, 1)
				}
			}()
		}

		ready.Wait()
		start.Done()
		done.Wait()

		if granted > peakGranted {
			peakGranted = granted
		}
	}

	require.LessOrEqual(t, peakGranted, int64(ratePerSec),
		"a concurrent first-touch burst granted %d requests for a bucket of %d: "+
			"a race-losing goroutine replaced a live bucket", peakGranted, ratePerSec)
}

// TestIPRateLimiter_MiddlewarePassThroughWhenDisabled verifies that a
// non-positive rate disables the limiter entirely, so Middleware never
// rejects a request.
func TestIPRateLimiter_MiddlewarePassThroughWhenDisabled(t *testing.T) {
	e := echo.New()
	e.Use(newIPRateLimiter(0).Middleware())
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

// TestIPRateLimiter_MiddlewareRejectsBeyondRate verifies the Middleware path
// (not just the underlying allow()) returns 429 once the configured rate is
// exceeded for a given IP.
func TestIPRateLimiter_MiddlewareRejectsBeyondRate(t *testing.T) {
	e := echo.New()
	e.Use(newIPRateLimiter(1).Middleware())
	e.GET("/test", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	rec1 := httptest.NewRecorder()
	e.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/test", nil))
	require.Equal(t, http.StatusOK, rec1.Code, "first request should pass")

	rec2 := httptest.NewRecorder()
	e.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/test", nil))
	require.Equal(t, http.StatusTooManyRequests, rec2.Code, "second request should be rate-limited")
}

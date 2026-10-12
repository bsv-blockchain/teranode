// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"net"
	"net/http"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/labstack/echo/v4"
	"golang.org/x/time/rate"
)

// ipRateLimiterCapacity bounds the number of distinct per-source buckets
// retained. Mirrors the LRU-bounded pattern in
// services/asset/httpimpl/rate_limiter.go: without a bound, an attacker
// rotating source addresses could grow the map without limit.
const ipRateLimiterCapacity = 50_000

// ipRateLimiter is a per-source token-bucket rate limiter for the P2P HTTP
// server (/health, /p2p-ws). The P2P HTTP surface has no authenticated
// peer-tiering concept, so a single source-keyed bucket is the smallest
// adaptation of the Asset service's tiered pattern (services/asset/httpimpl)
// that fits here.
type ipRateLimiter struct {
	limiters   *lru.Cache[string, *rate.Limiter]
	ratePerSec int
}

// newIPRateLimiter creates an ipRateLimiter. ratePerSec <= 0 disables the
// limiter entirely (Middleware becomes a no-op).
func newIPRateLimiter(ratePerSec int) *ipRateLimiter {
	// lru.New only errors when capacity <= 0, which ipRateLimiterCapacity never is.
	cache, _ := lru.New[string, *rate.Limiter](ipRateLimiterCapacity)

	return &ipRateLimiter{
		limiters:   cache,
		ratePerSec: ratePerSec,
	}
}

// Middleware returns the Echo middleware function for this rate limiter.
//
// Requests are keyed by the transport-level RemoteAddr host, not
// echo.Context.RealIP(): without a configured IPExtractor, RealIP() falls
// back to trusting the X-Forwarded-For/X-Real-IP headers from any caller
// (see echo's context.go), which would let an attacker bypass the limit by
// rotating the header value on every request.
func (rl *ipRateLimiter) Middleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if rl.ratePerSec <= 0 {
				return next(c)
			}

			if !rl.allow(c.Request().RemoteAddr) {
				initPrometheusMetrics()
				prometheusP2PHTTPRateLimited.Inc()

				return c.JSON(http.StatusTooManyRequests, map[string]string{"message": "rate limit exceeded"})
			}

			return next(c)
		}
	}
}

// allow consumes one token from remoteAddr's bucket, creating it if
// necessary. PeekOrAdd checks-and-inserts under the cache lock, so
// concurrent first-touch callers for the same key converge on one bucket. An
// unconditional Add would let a race-losing goroutine replace a bucket
// another goroutine had already installed and consumed from, resetting it to
// a full burst - exploitable every time a key is recreated after LRU
// eviction, and eviction is attacker-driven via key churn.
func (rl *ipRateLimiter) allow(remoteAddr string) bool {
	key := rateLimiterKey(remoteAddr)

	if lim, ok := rl.limiters.Get(key); ok {
		return lim.Allow()
	}

	lim := rate.NewLimiter(rate.Limit(rl.ratePerSec), rl.ratePerSec)

	if existing, ok, _ := rl.limiters.PeekOrAdd(key, lim); ok {
		return existing.Allow()
	}

	return lim.Allow()
}

// rateLimiterKey normalizes remoteAddr (a "host:port" pair, as found on
// http.Request.RemoteAddr) into a bucket key: IPv4 addresses verbatim, IPv6
// addresses masked to their /64 so an attacker cannot rotate through a
// single routed prefix to obtain a fresh bucket on every request.
func rateLimiterKey(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return host
	}

	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}

	mask := net.CIDRMask(64, 128)

	return ip.Mask(mask).String()
}

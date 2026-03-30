package httpimpl

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"golang.org/x/time/rate"
)

// limiterEntry holds a rate limiter and the last time it was accessed.
type limiterEntry struct {
	limiter  *rate.Limiter
	lastSeen atomic.Int64
}

// tieredRateLimitMiddleware returns Echo middleware that applies per-IP rate
// limiting based on the caller's peer tier. Mining peers are exempt, known
// peers get a multiplied rate, and unverified clients get the base rate.
// Setting defaultRate to 0 disables the middleware entirely.
func tieredRateLimitMiddleware(
	logger ulogger.Logger,
	defaultRate int,
	peerMultiplier int,
	tierLabel string,
) echo.MiddlewareFunc {
	var unverifiedLimiters sync.Map
	var peerLimiters sync.Map

	// Background cleanup of stale entries every 5 minutes.
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			now := time.Now().Unix()
			cleanupMap(&unverifiedLimiters, now)
			cleanupMap(&peerLimiters, now)
		}
	}()

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if defaultRate == 0 {
				return next(c)
			}

			tier, _ := c.Get("peer_tier").(peerTier)

			if tier == tierMiner {
				return next(c)
			}

			ip := c.RealIP()

			var limiter *rate.Limiter
			switch tier {
			case tierPeer:
				peerRate := defaultRate * peerMultiplier
				val, loaded := peerLimiters.LoadOrStore(ip, &limiterEntry{
					limiter: rate.NewLimiter(rate.Limit(peerRate), peerRate),
				})
				entry := val.(*limiterEntry)
				entry.lastSeen.Store(time.Now().Unix())
				if !loaded {
					// newly created, entry already stored
				}
				limiter = entry.limiter
			default:
				val, loaded := unverifiedLimiters.LoadOrStore(ip, &limiterEntry{
					limiter: rate.NewLimiter(rate.Limit(defaultRate), defaultRate),
				})
				entry := val.(*limiterEntry)
				entry.lastSeen.Store(time.Now().Unix())
				if !loaded {
					// newly created, entry already stored
				}
				limiter = entry.limiter
			}

			if !limiter.Allow() {
				prometheusAssetHTTPRateLimited.WithLabelValues(tierLabel).Inc()
				return c.JSON(http.StatusTooManyRequests, map[string]string{"error": "rate limit exceeded"})
			}

			return next(c)
		}
	}
}

// cleanupMap removes entries from the sync.Map that have not been seen in over
// 5 minutes (300 seconds).
func cleanupMap(m *sync.Map, now int64) {
	m.Range(func(key, value any) bool {
		entry := value.(*limiterEntry)
		if now-entry.lastSeen.Load() > 300 {
			m.Delete(key)
		}
		return true
	})
}

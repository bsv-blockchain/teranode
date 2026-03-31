package httpimpl

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v4"
	"golang.org/x/time/rate"
)

// limiterEntry holds a rate limiter and the last time it was accessed.
type limiterEntry struct {
	limiter  *rate.Limiter
	lastSeen atomic.Int64
}

// tieredRateLimiter holds the state for a tiered rate limiting middleware instance.
// Call StartCleanup to begin the background cleanup goroutine.
type tieredRateLimiter struct {
	unverifiedLimiters sync.Map
	peerLimiters       sync.Map
	defaultRate        int
	peerMultiplier     int
	tierLabel          string
}

// newTieredRateLimiter creates a tiered rate limiter. defaultRate <= 0 disables
// the limiter entirely. peerMultiplier is clamped to a minimum of 1.
func newTieredRateLimiter(defaultRate int, peerMultiplier int, tierLabel string) *tieredRateLimiter {
	if peerMultiplier < 1 {
		peerMultiplier = 1
	}
	return &tieredRateLimiter{
		defaultRate:    defaultRate,
		peerMultiplier: peerMultiplier,
		tierLabel:      tierLabel,
	}
}

// StartCleanup launches a background goroutine that removes stale limiter entries
// every 5 minutes. The goroutine stops when ctx is cancelled.
func (rl *tieredRateLimiter) StartCleanup(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now().Unix()
				cleanupMap(&rl.unverifiedLimiters, now)
				cleanupMap(&rl.peerLimiters, now)
			}
		}
	}()
}

// Middleware returns the Echo middleware function for this rate limiter.
func (rl *tieredRateLimiter) Middleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if rl.defaultRate <= 0 {
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
				peerRate := rl.defaultRate * rl.peerMultiplier
				val, _ := rl.peerLimiters.LoadOrStore(ip, &limiterEntry{
					limiter: rate.NewLimiter(rate.Limit(peerRate), peerRate),
				})
				entry := val.(*limiterEntry)
				entry.lastSeen.Store(time.Now().Unix())
				limiter = entry.limiter
			default:
				val, _ := rl.unverifiedLimiters.LoadOrStore(ip, &limiterEntry{
					limiter: rate.NewLimiter(rate.Limit(rl.defaultRate), rl.defaultRate),
				})
				entry := val.(*limiterEntry)
				entry.lastSeen.Store(time.Now().Unix())
				limiter = entry.limiter
			}

			if !limiter.Allow() {
				prometheusAssetHTTPRateLimited.WithLabelValues(rl.tierLabel).Inc()
				return c.JSON(http.StatusTooManyRequests, map[string]string{"message": "rate limit exceeded"})
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

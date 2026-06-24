package httpimpl

import (
	"context"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/labstack/echo/v4"
	"golang.org/x/time/rate"
)

// unverifiedLRUCapacity bounds the number of distinct IP buckets we hold for
// tier-unverified requests. Beyond this, the oldest entry is evicted. Without
// the bound, an IPv6 attacker rotating source addresses across a /64 (2^64
// addresses) could grow the map to many GB before the 5-minute cleanup runs.
const unverifiedLRUCapacity = 50_000

// limiterEntry holds a rate limiter and the last time it was accessed.
type limiterEntry struct {
	limiter  *rate.Limiter
	lastSeen atomic.Int64
}

// tieredRateLimiter holds the state for a tiered rate limiting middleware instance.
// Call StartCleanup to begin the background cleanup goroutine.
//
// Bucket keying differs by tier so that authentication elevates the peer, not
// the source IP:
//
//   - tierUnverified: bucket keyed by client IP (or /64 prefix for IPv6).
//     Held in a bounded LRU so an IPv6 flood can't grow the map without limit.
//   - tierPeer:       bucket keyed by libp2p peer ID. Two authenticated peers
//     behind one egress IP get independent buckets.
//   - tierMiner:      either fully exempt (minerRate == 0, the legacy
//     behaviour) or bucket keyed by libp2p peer ID at minerRate.
type tieredRateLimiter struct {
	unverifiedLimiters *lru.Cache[string, *limiterEntry]
	peerLimiters       sync.Map // map[peerID]*limiterEntry
	minerLimiters      sync.Map // map[peerID]*limiterEntry — only used when minerRate > 0
	defaultRate        int
	peerMultiplier     int
	minerRate          int
	tierLabel          string
}

// newTieredRateLimiter creates a tiered rate limiter. defaultRate <= 0 disables
// the limiter entirely. peerMultiplier is clamped to a minimum of 1.
// minerRate <= 0 means miner-tier requests are fully exempt.
func newTieredRateLimiter(defaultRate, peerMultiplier, minerRate int, tierLabel string) *tieredRateLimiter {
	if peerMultiplier < 1 {
		peerMultiplier = 1
	}

	// LRU constructor only errors on size <= 0, which we guard against.
	cache, _ := lru.New[string, *limiterEntry](unverifiedLRUCapacity)

	return &tieredRateLimiter{
		unverifiedLimiters: cache,
		defaultRate:        defaultRate,
		peerMultiplier:     peerMultiplier,
		minerRate:          minerRate,
		tierLabel:          tierLabel,
	}
}

// StartCleanup launches a background goroutine that removes stale entries
// from the peer/miner maps every 5 minutes. The unverified map is bounded by
// the LRU and doesn't need active cleanup. The goroutine stops when ctx is
// cancelled.
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
				cleanupSyncMap(&rl.peerLimiters, now)
				cleanupSyncMap(&rl.minerLimiters, now)
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
			lim := rl.limiterFor(c)
			if lim == nil {
				// Miner tier with minerRate=0 is fully exempt.
				return next(c)
			}
			return rl.allowAtBucket(c, next, lim)
		}
	}
}

// limiterFor returns the rate.Limiter bucket the request should be charged
// against, or nil if the request is exempt.
//
// Selection rules:
//   - tierMiner with minerRate > 0 and peer_id present → per-peer minerRate.
//   - tierMiner with minerRate <= 0                    → exempt (nil).
//   - tierPeer with peer_id present                    → per-peer at defaultRate × peerMultiplier.
//   - everything else (including authenticated tiers with peer_id missing) →
//     the *unverified* path: IP-keyed at defaultRate, with the same /64
//     normalisation. Treating "tier set but peer_id missing" as unverified
//     guarantees a wiring bug can never silently grant the elevated rate.
func (rl *tieredRateLimiter) limiterFor(c echo.Context) *rate.Limiter {
	tier, _ := c.Get("peer_tier").(peerTier)
	peerID, _ := c.Get("peer_id").(string)

	if tier == tierMiner && rl.minerRate <= 0 {
		return nil
	}

	if peerID != "" {
		switch tier {
		case tierMiner:
			return rl.peerBucket(&rl.minerLimiters, peerID, rl.minerRate)
		case tierPeer:
			return rl.peerBucket(&rl.peerLimiters, peerID, rl.defaultRate*rl.peerMultiplier)
		}
	}
	return rl.unverifiedBucket(unverifiedKey(c.RealIP()), rl.defaultRate)
}

// allowAtBucket consumes one token from the given limiter and returns the next
// handler or HTTP 429.
func (rl *tieredRateLimiter) allowAtBucket(c echo.Context, next echo.HandlerFunc, lim *rate.Limiter) error {
	if !lim.Allow() {
		prometheusAssetHTTPRateLimited.WithLabelValues(rl.tierLabel).Inc()
		return c.JSON(http.StatusTooManyRequests, map[string]string{"message": "rate limit exceeded"})
	}
	return next(c)
}

// unverifiedBucket returns the rate.Limiter for the given key in the bounded
// LRU. Uses load-then-store so the race-loser doesn't allocate a stranded
// rate.Limiter on the contended path.
func (rl *tieredRateLimiter) unverifiedBucket(key string, ratePerSec int) *rate.Limiter {
	if entry, ok := rl.unverifiedLimiters.Get(key); ok {
		entry.lastSeen.Store(time.Now().Unix())
		return entry.limiter
	}
	newEntry := &limiterEntry{limiter: rate.NewLimiter(rate.Limit(ratePerSec), ratePerSec)}
	newEntry.lastSeen.Store(time.Now().Unix())
	// Add returns true if an existing entry was evicted; we don't care, the
	// LRU handles eviction internally.
	rl.unverifiedLimiters.Add(key, newEntry)
	// Re-Get to handle the race where two goroutines added concurrently —
	// LRU.Add is last-write-wins, so whichever Get returns is the surviving
	// entry. Both limiters are valid, just slightly less accurate for the
	// race window.
	if entry, ok := rl.unverifiedLimiters.Get(key); ok {
		return entry.limiter
	}
	return newEntry.limiter
}

// peerBucket returns the rate.Limiter for the given peer ID in the supplied
// sync.Map. Uses load-then-store so race-losers don't allocate.
func (rl *tieredRateLimiter) peerBucket(m *sync.Map, peerID string, ratePerSec int) *rate.Limiter {
	if v, ok := m.Load(peerID); ok {
		entry := v.(*limiterEntry)
		entry.lastSeen.Store(time.Now().Unix())
		return entry.limiter
	}
	newEntry := &limiterEntry{limiter: rate.NewLimiter(rate.Limit(ratePerSec), ratePerSec)}
	newEntry.lastSeen.Store(time.Now().Unix())
	actual, _ := m.LoadOrStore(peerID, newEntry)
	entry := actual.(*limiterEntry)
	entry.lastSeen.Store(time.Now().Unix())
	return entry.limiter
}

// unverifiedKey normalises the source identifier for unverified buckets. For
// IPv6 we collapse to the /64 prefix so an attacker rotating addresses across
// a single /64 can't trivially evade the bucket. IPv4 is kept full-precision
// because /32 already maps 1:1 to host.
func unverifiedKey(rawIP string) string {
	ip := net.ParseIP(rawIP)
	if ip == nil {
		return rawIP
	}
	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}
	mask := net.CIDRMask(64, 128)
	return ip.Mask(mask).String()
}

// cleanupSyncMap removes entries that haven't been seen in over 5 minutes.
func cleanupSyncMap(m *sync.Map, now int64) {
	m.Range(func(key, value any) bool {
		entry := value.(*limiterEntry)
		if now-entry.lastSeen.Load() > 300 {
			m.Delete(key)
		}
		return true
	})
}

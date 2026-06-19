package httpimpl

import (
	"context"
	"strconv"
	"sync"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"golang.org/x/sync/singleflight"
)

const (
	// defaultMainChainWindowSize is the window size used when the configured
	// retention (global_blockHeightRetention) is zero or absent. 288 blocks is
	// roughly two days at a 10-minute block target — comfortably deeper than
	// any realistic reorg.
	defaultMainChainWindowSize = 288

	// maxOldCacheEntries bounds the lazily-filled cache for blocks older than
	// the window. Scrapers walking ancient transactions could otherwise grow
	// it without bound. When full, overflow lookups go straight to RPC every
	// time — a rare path (old blocks) so the cost is acceptable.
	maxOldCacheEntries = 10_000
)

// mainChainCache answers "is this block ID on the current main chain?" for
// the hot asset HTTP endpoints (/merkle_proof, /txmeta) without a gRPC
// round-trip per request. Those endpoints are hit far more often than blocks
// are produced, so the cache pays for itself immediately.
//
// Design (three tiers, routed by the claimed block height):
//
//  1. Authoritative window. A set of the main-chain block IDs for the last
//     windowSize blocks, fetched with a single GetBlockHeaders call (which
//     walks BACKWARD from the best block) and rebuilt on every block-level
//     notification. For a lookup whose height falls inside
//     [windowMinHeight, windowMaxHeight], the set is authoritative: present
//     means main chain, absent means NOT main chain. Zero RPC either way.
//     This is the dominant case for asset traffic.
//
//  2. Below-window fallback (blockHeight < windowMinHeight). An ID missing
//     from the window may simply be an old main-chain block, so
//     missing-means-false does NOT hold below the window. These lookups use
//     a lazily-filled oldCache map; misses go to CheckBlockIsInCurrentChain,
//     deduplicated per block ID via singleflight (prevents stampedes on cold
//     IDs) and stored under a generation guard: invalidation increments the
//     generation, and a result fetched before the invalidation is only
//     written back if the generation is unchanged (closes the TOCTOU window
//     where a stale result could outlive a reorg).
//
//  3. Above-window fallback (blockHeight > windowMaxHeight). Between a new
//     block arriving and the notification-driven rebuild landing, a tx in
//     that block claims a height above the window top. The window cannot
//     speak for those, so they go straight to RPC, uncached (the rebuild is
//     moments away).
//
// If the window has never been populated, or a rebuild fails, the window is
// marked unhealthy and EVERY lookup falls back to direct RPC with no caching
// at all — degraded throughput, never degraded correctness. oldCache is only
// consulted while the window is healthy: an unhealthy window means
// notification-driven invalidation cannot be trusted, so cached entries may
// be stale. Simpler invariant: healthy window is the precondition for any
// cached answer.
//
// Rebuilds run only on the single consume goroutine (initial populate +
// notifications), so they are naturally serialized; rebuildMu additionally
// guards direct rebuild calls from tests. Pending notifications are drained
// before each rebuild so a burst of block events coalesces into one rebuild.
type mainChainCache struct {
	client     blockchain.ClientI
	logger     ulogger.Logger
	windowSize uint32

	mu sync.RWMutex
	// windowHealthy gates ALL cached answers. False until the first
	// successful rebuild; reset to false when a rebuild fails.
	windowHealthy bool
	// window holds the main-chain block IDs whose heights lie within
	// [windowMinHeight, windowMaxHeight]. Authoritative for that range.
	window          map[uint32]struct{}
	windowMinHeight uint32
	windowMaxHeight uint32
	// oldCache lazily caches CheckBlockIsInCurrentChain results for blocks
	// below the window. Valid only while windowHealthy; cleared on every
	// rebuild and on rebuild failure (deep-reorg safety).
	oldCache map[uint32]bool
	// generation increments on every rebuild or invalidation. Lazy fills
	// capture it before the RPC and only write back if it is unchanged.
	generation uint64

	// rebuildMu serializes rebuilds (belt-and-braces: production rebuilds all
	// run on the consume goroutine already).
	rebuildMu sync.Mutex

	// sf deduplicates concurrent below-window RPCs per block ID.
	sf singleflight.Group

	// consumeDone is closed when the notification consumer goroutine exits.
	// Test observability only.
	consumeDone chan struct{}
}

// newMainChainCache constructs an unstarted cache. windowSize is the number
// of recent blocks the authoritative window covers (normally
// settings.GlobalBlockHeightRetention); zero falls back to
// defaultMainChainWindowSize. Call Start(ctx) to populate the window and
// begin consuming blockchain notifications. An unstarted cache is permanently
// unhealthy: every lookup falls back to direct RPC, which keeps it correct
// (if slow) for tests that never call Start.
func newMainChainCache(client blockchain.ClientI, logger ulogger.Logger, windowSize uint32) *mainChainCache {
	if windowSize == 0 {
		windowSize = defaultMainChainWindowSize
	}
	return &mainChainCache{
		client:      client,
		logger:      logger,
		windowSize:  windowSize,
		oldCache:    make(map[uint32]bool),
		consumeDone: make(chan struct{}),
	}
}

// Start subscribes to blockchain notifications, then asynchronously populates
// the window and consumes notifications until ctx is cancelled. Returns an
// error only if the subscription itself fails. The initial populate is async
// so service startup never blocks on blockchain availability — lookups before
// the first successful rebuild simply take the unhealthy → direct-RPC path.
func (c *mainChainCache) Start(ctx context.Context) error {
	sub, err := c.client.Subscribe(ctx, "asset-mainchain-cache")
	if err != nil {
		return err
	}
	go func() {
		defer close(c.consumeDone)
		c.rebuild(ctx) // immediate initial populate; failure leaves the window unhealthy
		c.consume(ctx, sub)
	}()
	return nil
}

func (c *mainChainCache) consume(ctx context.Context, sub <-chan *blockchain_api.Notification) {
	for {
		select {
		case <-ctx.Done():
			return
		case n, ok := <-sub:
			if !ok {
				// Channel closed unexpectedly; without a live subscription the
				// window will never refresh on new blocks or reorgs. Drop to the
				// always-correct direct-RPC path and log loudly.
				c.logger.Warnf("[Asset] mainchain cache subscription closed unexpectedly; disabling cache")
				c.invalidate()
				return
			}
			if n == nil || !isBlockEvent(n.Type) {
				continue
			}
			// Coalesce: drain whatever is already queued so a burst of block
			// notifications triggers a single rebuild against the latest tip.
			closed := drainNotifications(sub)
			c.rebuild(ctx)
			if closed {
				c.logger.Warnf("[Asset] mainchain cache subscription closed unexpectedly; disabling cache")
				c.invalidate()
				return
			}
		}
	}
}

func isBlockEvent(t model.NotificationType) bool {
	return t == model.NotificationType_Block || t == model.NotificationType_BlockMinedUnset
}

// drainNotifications discards all currently queued notifications without
// blocking. Returns true if the channel was found closed.
func drainNotifications(sub <-chan *blockchain_api.Notification) bool {
	for {
		select {
		case _, ok := <-sub:
			if !ok {
				return true
			}
		default:
			return false
		}
	}
}

// rebuild fetches the last windowSize main-chain block headers and swaps in a
// new window atomically — there is never an empty-window gap during a
// successful rebuild. On any failure the window is marked unhealthy instead,
// so lookups degrade to direct RPC rather than serving stale answers.
// oldCache is cleared either way: a rebuild only runs because the chain
// changed, and a deep reorg can flip the on-chain status of old blocks.
func (c *mainChainCache) rebuild(ctx context.Context) {
	c.rebuildMu.Lock()
	defer c.rebuildMu.Unlock()

	bestHeader, _, err := c.client.GetBestBlockHeader(ctx)
	if err != nil || bestHeader == nil {
		c.markUnhealthy("GetBestBlockHeader", err)
		return
	}

	// GetBlockHeaders walks BACKWARD from the given hash, so this returns the
	// best block and up to windowSize-1 of its main-chain ancestors.
	_, metas, err := c.client.GetBlockHeaders(ctx, bestHeader.Hash(), uint64(c.windowSize))
	if err != nil {
		c.markUnhealthy("GetBlockHeaders", err)
		return
	}

	window := make(map[uint32]struct{}, len(metas))
	var minHeight, maxHeight uint32
	first := true
	for _, m := range metas {
		if m == nil {
			continue
		}
		window[m.ID] = struct{}{}
		if first || m.Height < minHeight {
			minHeight = m.Height
		}
		if first || m.Height > maxHeight {
			maxHeight = m.Height
		}
		first = false
	}
	if len(window) == 0 {
		c.markUnhealthy("GetBlockHeaders returned no headers", nil)
		return
	}

	c.mu.Lock()
	c.generation++
	c.window = window
	c.windowMinHeight = minHeight
	c.windowMaxHeight = maxHeight
	c.windowHealthy = true
	c.oldCache = make(map[uint32]bool)
	c.mu.Unlock()
}

// markUnhealthy zeroes the window and clears oldCache so every subsequent
// lookup takes the direct-RPC path until the next successful rebuild.
func (c *mainChainCache) markUnhealthy(op string, err error) {
	c.invalidate()
	c.logger.Warnf("[Asset] mainchain cache window rebuild failed (%s): %v; falling back to direct gRPC lookups", op, err)
}

// invalidate drops all cached state and bumps the generation so any in-flight
// lazy fill aborts its write-back.
func (c *mainChainCache) invalidate() {
	c.mu.Lock()
	c.generation++
	c.windowHealthy = false
	c.window = nil
	c.oldCache = make(map[uint32]bool)
	c.mu.Unlock()
}

// IsOnMainChain reports whether the block with the given internal ID, claimed
// to be at the given height, is part of the current best chain.
//
// Routing by claimed height (see the type comment for the full rationale):
//   - window healthy, height within window range: authoritative set lookup,
//     zero RPC — absent means NOT on the main chain.
//   - height below the window: oldCache, then singleflight RPC with a
//     generation-guarded write-back.
//   - height above the window, or window unhealthy: direct RPC, uncached.
func (c *mainChainCache) IsOnMainChain(ctx context.Context, blockID, blockHeight uint32) (bool, error) {
	c.mu.RLock()

	if !c.windowHealthy {
		c.mu.RUnlock()
		return c.client.CheckBlockIsInCurrentChain(ctx, []uint32{blockID})
	}

	if blockHeight >= c.windowMinHeight && blockHeight <= c.windowMaxHeight {
		_, ok := c.window[blockID]
		c.mu.RUnlock()
		return ok, nil
	}

	if blockHeight > c.windowMaxHeight {
		c.mu.RUnlock()
		return c.client.CheckBlockIsInCurrentChain(ctx, []uint32{blockID})
	}

	// Below the window.
	if v, ok := c.oldCache[blockID]; ok {
		c.mu.RUnlock()
		return v, nil
	}
	c.mu.RUnlock()

	v, err, _ := c.sf.Do(strconv.FormatUint(uint64(blockID), 10), func() (interface{}, error) {
		// Re-check under the singleflight: a caller that lost the oldCache race
		// to a just-completed leader becomes the new leader here and must not
		// re-issue the RPC.
		c.mu.RLock()
		if v, ok := c.oldCache[blockID]; ok {
			c.mu.RUnlock()
			return v, nil
		}
		gen := c.generation
		c.mu.RUnlock()

		onChain, err := c.client.CheckBlockIsInCurrentChain(ctx, []uint32{blockID})
		if err != nil {
			return false, err
		}

		c.mu.Lock()
		// Generation guard: only cache if no invalidation/rebuild happened while
		// the RPC was in flight; the result may describe the pre-reorg chain.
		// Size cap: skip caching when full rather than evicting — overflow IDs
		// just pay the RPC each time.
		if c.generation == gen && c.windowHealthy && len(c.oldCache) < maxOldCacheEntries {
			c.oldCache[blockID] = onChain
		}
		c.mu.Unlock()
		return onChain, nil
	})
	if err != nil {
		return false, err
	}
	return v.(bool), nil
}

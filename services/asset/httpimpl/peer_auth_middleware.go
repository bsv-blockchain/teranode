package httpimpl

import (
	"context"
	"encoding/hex"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

type peerTier int

const (
	tierUnverified peerTier = iota
	tierPeer
	tierMiner
)

// String returns a human-readable name for the peer tier.
func (t peerTier) String() string {
	switch t {
	case tierPeer:
		return "peer"
	case tierMiner:
		return "miner"
	default:
		return "unverified"
	}
}

// peerTierCache maintains a cached mapping of peer IDs to their computed tier,
// refreshed periodically from the P2P peer registry.
type peerTierCache struct {
	mu                       sync.RWMutex
	tiers                    map[peer.ID]peerTier
	p2pClient                p2p.ClientI
	minerReputationThreshold float64
	logger                   ulogger.Logger
}

// newPeerTierCache creates a new peerTierCache that classifies peers into tiers
// based on data from the P2P peer registry.
func newPeerTierCache(logger ulogger.Logger, p2pClient p2p.ClientI, minerReputationThreshold float64) *peerTierCache {
	return &peerTierCache{
		tiers:                    make(map[peer.ID]peerTier),
		p2pClient:                p2pClient,
		minerReputationThreshold: minerReputationThreshold,
		logger:                   logger,
	}
}

// Start launches a background goroutine that refreshes the tier cache every 30 seconds.
// It fetches the peer registry and classifies each peer as tierMiner (if the peer has
// received blocks and meets the reputation threshold) or tierPeer. On error the stale
// cache is preserved (fail open). The goroutine stops when ctx is cancelled.
func (c *peerTierCache) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		// Perform an initial refresh immediately.
		c.refresh(ctx)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.refresh(ctx)
			}
		}
	}()
}

// refresh fetches the peer registry and rebuilds the tier map.
func (c *peerTierCache) refresh(ctx context.Context) {
	peers, err := c.p2pClient.GetPeerRegistry(ctx)
	if err != nil {
		c.logger.Warnf("[PeerTierCache] failed to refresh peer registry: %v", err)
		return
	}

	updated := make(map[peer.ID]peerTier, len(peers))
	for _, p := range peers {
		if p.BlocksReceived > 0 && p.ReputationScore >= c.minerReputationThreshold {
			updated[p.ID] = tierMiner
		} else {
			updated[p.ID] = tierPeer
		}
	}

	c.mu.Lock()
	c.tiers = updated
	c.mu.Unlock()
}

// GetTier returns the cached tier for the given peer ID. If the peer is not found
// in the cache, tierUnverified is returned.
func (c *peerTierCache) GetTier(id peer.ID) peerTier {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tier, ok := c.tiers[id]
	if !ok {
		return tierUnverified
	}
	return tier
}

// peerAuthMiddleware returns Echo middleware that authenticates incoming requests
// using Ed25519 peer signatures. It inspects X-Peer-PubKey, X-Peer-Timestamp, and
// X-Peer-Signature headers, verifies the signature, and sets the "peer_tier" context
// value. All error paths silently continue as unverified (fail open).
func peerAuthMiddleware(logger ulogger.Logger, cache *peerTierCache) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set("peer_tier", tierUnverified)

			pubKeyHex := c.Request().Header.Get("X-Peer-PubKey")
			if pubKeyHex == "" {
				return next(c)
			}

			// Validate timestamp is within ±60 seconds.
			tsStr := c.Request().Header.Get("X-Peer-Timestamp")
			ts, err := strconv.ParseInt(tsStr, 10, 64)
			if err != nil {
				return next(c)
			}
			if math.Abs(float64(time.Now().Unix()-ts)) > 60 {
				return next(c)
			}

			// Decode the public key.
			pubKeyBytes, err := hex.DecodeString(pubKeyHex)
			if err != nil {
				return next(c)
			}
			pubKey, err := crypto.UnmarshalEd25519PublicKey(pubKeyBytes)
			if err != nil {
				return next(c)
			}

			// Build the payload and verify the signature.
			payload := tsStr + ":" + c.Request().Method + ":" + c.Request().URL.Path

			sigHex := c.Request().Header.Get("X-Peer-Signature")
			sigBytes, err := hex.DecodeString(sigHex)
			if err != nil {
				return next(c)
			}

			ok, err := pubKey.Verify([]byte(payload), sigBytes)
			if err != nil || !ok {
				return next(c)
			}

			peerID, err := peer.IDFromPublicKey(pubKey)
			if err != nil {
				return next(c)
			}

			tier := cache.GetTier(peerID)
			c.Set("peer_tier", tier)
			logger.Debugf("[PeerAuth] authenticated peer %s as %s", peerID, tier)

			return next(c)
		}
	}
}

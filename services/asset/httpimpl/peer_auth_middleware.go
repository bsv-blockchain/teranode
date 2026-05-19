package httpimpl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// peerTier constants are emitted in metric labels and access logs; never
// renumber after merge — append only.
type peerTier int

const (
	tierUnverified peerTier = iota
	tierPeer
	tierMiner
)

// freshnessWindowSeconds is the maximum drift between the client clock (as
// signed into the request) and the server clock. Tight enough that NTP-drifted
// hosts will fail loudly rather than open a wide replay window; loose enough
// to survive normal multi-second clock jitter on well-NTP'd infrastructure.
const freshnessWindowSeconds = 10

// peerAuthHeaderTimestamp / Signature / PubKey are the request headers a
// signed peer must set. The body-digest header is util.PeerAuthBodyDigestHeader.
const (
	peerAuthHeaderTimestamp = "X-Peer-Timestamp"
	peerAuthHeaderSignature = "X-Peer-Signature"
	peerAuthHeaderPubKey    = "X-Peer-PubKey"
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
// using Ed25519 peer signatures (v2 signed-payload format) and sets the "peer_tier"
// context value.
//
// Signed payload format (see util.buildSignedPayload):
//
//	v2:<unix_ts>:<host>:<method>:<request_uri>:<sha256_body_hex>
//
// Headers required:
//   - X-Peer-PubKey      — hex-encoded Ed25519 public key
//   - X-Peer-Timestamp   — unix seconds, must be within freshnessWindowSeconds
//   - X-Peer-Body-Digest — lowercase hex SHA-256 of the request body; verified
//     against the actual body bytes so a signature can't be replayed across
//     different bodies
//   - X-Peer-Signature   — hex-encoded Ed25519 signature over the payload
//
// All error paths fall through with tierUnverified (fail open). NTP drift
// outside the freshness window is treated as an auth failure; operators should
// keep clocks within ±5s of UTC.
func peerAuthMiddleware(logger ulogger.Logger, cache *peerTierCache) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set("peer_tier", tierUnverified)

			pubKeyHex := c.Request().Header.Get(peerAuthHeaderPubKey)
			if pubKeyHex == "" {
				return next(c)
			}

			// Validate freshness.
			tsStr := c.Request().Header.Get(peerAuthHeaderTimestamp)
			ts, err := strconv.ParseInt(tsStr, 10, 64)
			if err != nil {
				return next(c)
			}
			if math.Abs(float64(time.Now().Unix()-ts)) > freshnessWindowSeconds {
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

			// Decode the signature.
			sigHex := c.Request().Header.Get(peerAuthHeaderSignature)
			sigBytes, err := hex.DecodeString(sigHex)
			if err != nil {
				return next(c)
			}

			// Verify the body digest header against the actual body bytes.
			// Without this step the digest is just attacker-controlled data.
			declaredDigest := strings.ToLower(c.Request().Header.Get(util.PeerAuthBodyDigestHeader))
			actualDigest, err := digestRequestBody(c.Request())
			if err != nil {
				return next(c)
			}
			if declaredDigest != actualDigest {
				return next(c)
			}

			// Build and verify the canonical payload.
			payload := "v2:" + tsStr + ":" + c.Request().Host + ":" + c.Request().Method + ":" + c.Request().URL.RequestURI() + ":" + declaredDigest
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

// digestRequestBody computes the lowercase hex SHA-256 of the request body and
// replaces req.Body so handlers downstream still see it. For requests with no
// body (GET/HEAD typically) it returns util.EmptyBodySHA256Hex without reading.
func digestRequestBody(req *http.Request) (string, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return util.EmptyBodySHA256Hex, nil
	}

	buf, err := io.ReadAll(req.Body)
	if err != nil {
		return "", err
	}
	_ = req.Body.Close()

	if len(buf) == 0 {
		req.Body = http.NoBody
		return util.EmptyBodySHA256Hex, nil
	}

	sum := sha256.Sum256(buf)
	req.Body = io.NopCloser(bytes.NewReader(buf))
	req.ContentLength = int64(len(buf))
	return hex.EncodeToString(sum[:]), nil
}

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
	"github.com/jellydator/ttlcache/v3"
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

// replayCacheTTL is how long a seen (pubkey, signature) pair is remembered.
// It must exceed freshnessWindowSeconds so that an attacker can't outlast the
// cache by replaying right at the edge of the window.
const replayCacheTTL = 15 * time.Second

// replayCacheCapacity bounds memory usage under a signature-flood attack.
// At ~70 bytes per entry (key + ttlcache overhead) this is ~7 MB worst case.
const replayCacheCapacity = 100_000

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

// peerAuthVerifier holds the shared state used by the peer-auth middleware:
// the tier cache (peer registry snapshot), the replay cache, and the
// per-peer allowlist for tier elevation. Cache goroutines are started via
// Start(ctx) and stopped when ctx is cancelled.
type peerAuthVerifier struct {
	logger      ulogger.Logger
	tierCache   *peerTierCache
	replayCache *ttlcache.Cache[string, struct{}]

	// allowlist is the set of peer IDs eligible for tierPeer/tierMiner. An
	// empty allowlist means **no peer is eligible** — every authenticated
	// peer is treated as tierUnverified for rate-limit purposes. Operators
	// opt in by setting asset_peerAuthAllowlist.
	allowlist map[peer.ID]struct{}
}

// newPeerAuthVerifier constructs a verifier with its own replay cache and the
// parsed allowlist of peer IDs eligible for tier elevation.
func newPeerAuthVerifier(logger ulogger.Logger, tierCache *peerTierCache, allowlist map[peer.ID]struct{}) *peerAuthVerifier {
	return &peerAuthVerifier{
		logger:    logger,
		tierCache: tierCache,
		allowlist: allowlist,
		replayCache: ttlcache.New[string, struct{}](
			ttlcache.WithTTL[string, struct{}](replayCacheTTL),
			ttlcache.WithCapacity[string, struct{}](replayCacheCapacity),
		),
	}
}

// parsePeerAuthAllowlist turns a pipe-separated string of libp2p peer IDs
// into a set. Empty or whitespace-only input returns an empty set. Invalid
// entries are logged at Warn and skipped (the operator's intent should fail
// safe: an unparseable list shouldn't accidentally trust everyone).
func parsePeerAuthAllowlist(logger ulogger.Logger, raw string) map[peer.ID]struct{} {
	out := make(map[peer.ID]struct{})
	for _, part := range strings.Split(raw, "|") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := peer.Decode(part)
		if err != nil {
			logger.Warnf("[PeerAuth] ignoring invalid peer ID in asset_peerAuthAllowlist: %q (%v)", part, err)
			continue
		}
		out[id] = struct{}{}
	}
	return out
}

// Start launches background goroutines for the tier and replay caches. They
// stop when ctx is cancelled.
func (v *peerAuthVerifier) Start(ctx context.Context) {
	if v.tierCache != nil {
		v.tierCache.Start(ctx)
	}
	go v.replayCache.Start()
	go func() {
		<-ctx.Done()
		v.replayCache.Stop()
	}()
}

// Middleware returns Echo middleware that authenticates incoming requests
// using Ed25519 peer signatures (v2 signed-payload format) and sets the
// "peer_tier" context value.
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
func (v *peerAuthVerifier) Middleware() echo.MiddlewareFunc {
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

			// Replay check: hash (pubkey, signature) for a short fixed key and
			// reject anything we've already seen within the TTL window.
			replayKey := replayCacheKey(pubKeyHex, sigHex)
			if v.replayCache.Has(replayKey) {
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

			// Signature is valid AND fresh — record it so a re-submit within
			// the window is rejected. Recorded after verify so a flood of
			// invalid signatures doesn't pollute the cache.
			v.replayCache.Set(replayKey, struct{}{}, ttlcache.DefaultTTL)

			peerID, err := peer.IDFromPublicKey(pubKey)
			if err != nil {
				return next(c)
			}

			// Allowlist gate: signature is valid but the peer is only
			// eligible for tier elevation if explicitly listed by the
			// operator. Empty allowlist => no peer is eligible. Authenticated
			// but un-allowlisted peers stay at tierUnverified — this is the
			// authentication signal without the rate-limit privilege.
			if _, ok := v.allowlist[peerID]; !ok {
				v.logger.Debugf("[PeerAuth] authenticated peer %s not in allowlist; staying unverified", peerID)
				return next(c)
			}

			tier := v.tierCache.GetTier(peerID)
			c.Set("peer_tier", tier)
			v.logger.Debugf("[PeerAuth] authenticated peer %s as %s", peerID, tier)

			return next(c)
		}
	}
}

// replayCacheKey returns a short fixed-length key for the (pubkey, signature)
// pair. Using SHA-256 keeps the map keys bounded regardless of input size.
func replayCacheKey(pubKeyHex, sigHex string) string {
	sum := sha256.Sum256([]byte(pubKeyHex + ":" + sigHex))
	return string(sum[:])
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

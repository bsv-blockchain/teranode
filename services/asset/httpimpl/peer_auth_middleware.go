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

// Result labels for prometheusAssetHTTPPeerAuthResult.
const (
	peerAuthResultOK             = "ok"
	peerAuthResultBadSig         = "bad_sig"
	peerAuthResultBadDigest      = "bad_digest"
	peerAuthResultExpired        = "expired"
	peerAuthResultReplay         = "replay"
	peerAuthResultUnknownKey     = "unknown_key"
	peerAuthResultNotAllowlisted = "not_allowlisted"
)

// recordAuthResult increments the auth-result counter, tolerating an uninitialised
// metric (some tests skip metrics setup).
func recordAuthResult(result string) {
	if prometheusAssetHTTPPeerAuthResult == nil {
		return
	}
	prometheusAssetHTTPPeerAuthResult.WithLabelValues(result).Inc()
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
// All error paths fall through with tierUnverified (fail open) and increment
// prometheusAssetHTTPPeerAuthResult with the specific failure reason. NTP
// drift outside the freshness window is treated as an auth failure;
// operators should keep clocks within ±5s of UTC.
func (v *peerAuthVerifier) Middleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set("peer_tier", tierUnverified)

			peerID, result, attempted := v.verifySignedRequest(c)
			if !attempted {
				// No auth attempted — this is the common path for
				// unauthenticated public traffic; no metric increment.
				return next(c)
			}
			if result != peerAuthResultOK {
				recordAuthResult(result)
				return next(c)
			}

			tier := v.tierCache.GetTier(peerID)
			if tier == tierUnverified {
				// Peer is allowlisted but not in the registry yet (e.g. fresh
				// connection before tier cache refresh). Treat as unknown.
				recordAuthResult(peerAuthResultUnknownKey)
				return next(c)
			}

			c.Set("peer_tier", tier)
			// peer_id is consumed by the rate limiter so authenticated buckets
			// are keyed by stable peer identity rather than the (possibly
			// shared, possibly mobile) source IP.
			c.Set("peer_id", peerID.String())
			recordAuthResult(peerAuthResultOK)
			v.logger.Debugf("[PeerAuth] authenticated peer %s as %s", peerID, tier)
			return next(c)
		}
	}
}

// verifySignedRequest performs the cryptographic and policy checks against a
// signed request. Returns (peerID, result, attempted) where:
//   - attempted=false: no X-Peer-PubKey header; caller should fall through
//     without recording a metric.
//   - attempted=true and result==peerAuthResultOK: caller should look up the
//     tier and elevate.
//   - attempted=true and result!=peerAuthResultOK: caller should record the
//     result label and fall through as tierUnverified.
//
// Splitting this out keeps Middleware itself well under the cognitive-complexity
// budget; the bulk of the logic here is straight-line fail-fast checks.
func (v *peerAuthVerifier) verifySignedRequest(c echo.Context) (peer.ID, string, bool) {
	req := c.Request()

	pubKeyHex := req.Header.Get(peerAuthHeaderPubKey)
	if pubKeyHex == "" {
		return "", "", false
	}

	if !checkFreshness(req.Header.Get(peerAuthHeaderTimestamp)) {
		return "", peerAuthResultExpired, true
	}

	pubKey, ok := decodeEd25519PublicKey(pubKeyHex)
	if !ok {
		return "", peerAuthResultBadSig, true
	}

	sigHex := req.Header.Get(peerAuthHeaderSignature)
	sigBytes, err := hex.DecodeString(sigHex)
	if err != nil {
		return "", peerAuthResultBadSig, true
	}

	// Replay check before any expensive work (signature verify, body read).
	replayKey := replayCacheKey(pubKeyHex, sigHex)
	if v.replayCache.Has(replayKey) {
		return "", peerAuthResultReplay, true
	}

	declaredDigest := strings.ToLower(req.Header.Get(util.PeerAuthBodyDigestHeader))
	actualDigest, err := digestRequestBody(req)
	if err != nil || declaredDigest != actualDigest {
		return "", peerAuthResultBadDigest, true
	}

	payload := "v2:" + req.Header.Get(peerAuthHeaderTimestamp) + ":" + req.Host + ":" + req.Method + ":" + req.URL.RequestURI() + ":" + declaredDigest
	verified, err := pubKey.Verify([]byte(payload), sigBytes)
	if err != nil || !verified {
		return "", peerAuthResultBadSig, true
	}

	// Signature is valid AND fresh — record so a re-submit within the window
	// is rejected. Recorded only after verify so a flood of invalid sigs
	// doesn't pollute the cache.
	v.replayCache.Set(replayKey, struct{}{}, ttlcache.DefaultTTL)

	peerID, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		return "", peerAuthResultBadSig, true
	}

	if _, ok := v.allowlist[peerID]; !ok {
		v.logger.Debugf("[PeerAuth] authenticated peer %s not in allowlist; staying unverified", peerID)
		return peerID, peerAuthResultNotAllowlisted, true
	}

	return peerID, peerAuthResultOK, true
}

// checkFreshness parses the X-Peer-Timestamp header and verifies it is within
// the freshness window. Wraps the strconv + math.Abs check into a single bool.
func checkFreshness(tsStr string) bool {
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return false
	}
	return math.Abs(float64(time.Now().Unix()-ts)) <= freshnessWindowSeconds
}

// decodeEd25519PublicKey decodes a hex-encoded Ed25519 public key.
func decodeEd25519PublicKey(hexStr string) (crypto.PubKey, bool) {
	raw, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, false
	}
	pubKey, err := crypto.UnmarshalEd25519PublicKey(raw)
	if err != nil {
		return nil, false
	}
	return pubKey, true
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

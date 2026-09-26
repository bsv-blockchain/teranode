package httpimpl

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
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

// defaultMaxSignedBodyBytes is the floor for the cap on how much request body
// the verifier will buffer and hash on behalf of a signed request. Asset does
// serve signed POST routes with non-trivial bodies — notably
// POST /subtree/:hash/txs, which peer catchup uses to fetch missing
// transactions (services/subtreevalidation/SubtreeValidation.go) — so the
// effective cap is resolved per-instance from subtreevalidation's batch size
// (see resolveMaxSignedBodyBytes) rather than fixed at this floor. It exists
// so that the digest step can never be turned into an unbounded allocation,
// including when asset_httpBodyLimit is left empty and the Echo body-limit
// middleware is skipped entirely.
const defaultMaxSignedBodyBytes = 1 << 20

// signedBodyCapHeadroom is added on top of the raw catchup payload size
// (32 bytes per requested tx hash) when deriving the signed-body cap, to
// leave room for header/framing overhead without having to track it exactly.
const signedBodyCapHeadroom = 4096

// resolveMaxSignedBodyBytes derives the signed-body cap from
// subtreevalidation_missingTransactionsBatchSize: POST /subtree/:hash/txs
// carries 32 bytes per requested tx hash, up to that batch size, so a cap
// fixed below 32*batchSize would 413 a legitimate allowlisted peer's catchup
// request. The cap never drops below defaultMaxSignedBodyBytes.
func resolveMaxSignedBodyBytes(missingTransactionsBatchSize int) int64 {
	if missingTransactionsBatchSize <= 0 {
		return defaultMaxSignedBodyBytes
	}

	needed := 32*int64(missingTransactionsBatchSize) + signedBodyCapHeadroom
	if needed > defaultMaxSignedBodyBytes {
		return needed
	}

	return defaultMaxSignedBodyBytes
}

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
	// empty allowlist means no peer is eligible: a signed request is rejected
	// at the membership check, before the replay claim, the signature
	// verification and the body digest, and stays at tierUnverified.
	// Operators opt in by setting asset_peerAuthAllowlist.
	allowlist map[peer.ID]struct{}

	// maxSignedBodyBytes is the resolved cap for this verifier instance; see
	// resolveMaxSignedBodyBytes.
	maxSignedBodyBytes int64
}

// newPeerAuthVerifier constructs a verifier with its own replay cache and the
// parsed allowlist of peer IDs eligible for tier elevation, using the default
// signed-body cap. Production wiring should use newPeerAuthVerifierWithBodyCap
// so the cap tracks subtreevalidation's batch size.
func newPeerAuthVerifier(logger ulogger.Logger, tierCache *peerTierCache, allowlist map[peer.ID]struct{}) *peerAuthVerifier {
	return newPeerAuthVerifierWithBodyCap(logger, tierCache, allowlist, defaultMaxSignedBodyBytes)
}

// newPeerAuthVerifierWithBodyCap is like newPeerAuthVerifier but takes an
// explicit signed-body cap (see resolveMaxSignedBodyBytes).
func newPeerAuthVerifierWithBodyCap(logger ulogger.Logger, tierCache *peerTierCache, allowlist map[peer.ID]struct{}, maxSignedBodyBytes int64) *peerAuthVerifier {
	return &peerAuthVerifier{
		logger:             logger,
		tierCache:          tierCache,
		allowlist:          allowlist,
		maxSignedBodyBytes: maxSignedBodyBytes,
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
	peerAuthResultBodyTooLarge   = "body_too_large"
)

// errSignedBodyTooLarge is returned by digestRequestBody when a signed request
// declares or streams more than the verifier's resolved signed-body cap.
var errSignedBodyTooLarge = errors.NewInvalidArgumentError("signed request body exceeds configured limit")

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
// Error paths fall through with tierUnverified (fail open) and increment
// prometheusAssetHTTPPeerAuthResult with the specific failure reason. The one
// exception is a signed body over maxSignedBodyBytes, which is answered with
// 413 because the body was deliberately not buffered and so cannot be passed
// on intact. NTP drift outside the freshness window is treated as an auth
// failure; operators should keep clocks within ±5s of UTC.
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
			if result == peerAuthResultBodyTooLarge {
				// The only hard rejection in this middleware: the body has not
				// been buffered, so it cannot be handed on intact.
				recordAuthResult(result)
				return echo.NewHTTPError(http.StatusRequestEntityTooLarge, "signed request body too large")
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

	// Cheap fixed-size checks first. Nothing below may touch the request body
	// until the request has been shown to be worth that work.
	if !checkFreshness(req.Header.Get(peerAuthHeaderTimestamp)) {
		return "", peerAuthResultExpired, true
	}

	pubKeyRaw, pubKey, ok := decodeEd25519PublicKey(pubKeyHex)
	if !ok {
		return "", peerAuthResultBadSig, true
	}

	sigBytes, err := hex.DecodeString(req.Header.Get(peerAuthHeaderSignature))
	if err != nil || len(sigBytes) != ed25519.SignatureSize {
		return "", peerAuthResultBadSig, true
	}

	peerID, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		return "", peerAuthResultBadSig, true
	}

	// Anyone can mint an Ed25519 key, so a valid signature is not what makes a
	// request worth spending resources on — allowlist membership is. Check it
	// before the replay claim, the signature check and the body read.
	if _, ok := v.allowlist[peerID]; !ok {
		v.logger.Debugf("[PeerAuth] authenticated peer %s not in allowlist; staying unverified", peerID)
		return peerID, peerAuthResultNotAllowlisted, true
	}

	// Claim the (pubkey, signature) pair in one locked operation so racing
	// copies of a single signed request cannot all pass. The key is built from
	// the decoded bytes, so hex case variants collapse onto the same entry.
	replayKey := replayCacheKey(pubKeyRaw, sigBytes)
	if _, seen := v.replayCache.GetOrSet(replayKey, struct{}{}, ttlcache.WithTTL[string, struct{}](replayCacheTTL)); seen {
		return "", peerAuthResultReplay, true
	}

	// Release the claim unless the request actually authenticates, so a flood
	// of malformed submissions can't burn a peer's signature or evict live
	// entries from the cache.
	authenticated := false

	defer func() {
		if !authenticated {
			v.replayCache.Delete(replayKey)
		}
	}()

	// The signature covers the *declared* digest header, not the computed one,
	// so it can be verified before a single body byte is read. The body is
	// then checked against that same declared digest below.
	declaredDigest := strings.ToLower(req.Header.Get(util.PeerAuthBodyDigestHeader))

	payload := "v2:" + req.Header.Get(peerAuthHeaderTimestamp) + ":" + req.Host + ":" + req.Method + ":" + req.URL.RequestURI() + ":" + declaredDigest
	verified, err := pubKey.Verify([]byte(payload), sigBytes)
	if err != nil || !verified {
		return "", peerAuthResultBadSig, true
	}

	actualDigest, err := v.digestRequestBody(req)
	if err != nil {
		if errors.Is(err, errSignedBodyTooLarge) {
			return peerID, peerAuthResultBodyTooLarge, true
		}

		return "", peerAuthResultBadDigest, true
	}

	if declaredDigest != actualDigest {
		return "", peerAuthResultBadDigest, true
	}

	authenticated = true

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

// decodeEd25519PublicKey decodes a hex-encoded Ed25519 public key, returning
// both the decoded bytes (for canonical replay-cache keying) and the key.
func decodeEd25519PublicKey(hexStr string) ([]byte, crypto.PubKey, bool) {
	raw, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, nil, false
	}
	pubKey, err := crypto.UnmarshalEd25519PublicKey(raw)
	if err != nil {
		return nil, nil, false
	}
	return raw, pubKey, true
}

// replayCacheKey returns a short fixed-length key for the (pubkey, signature)
// pair. It hashes the *decoded* bytes, not the hex headers: hex is
// case-insensitive, so keying on the header text would let an attacker mint a
// fresh replay identity for one captured signature just by changing case.
func replayCacheKey(pubKeyRaw, sigBytes []byte) string {
	h := sha256.New()
	_, _ = h.Write(pubKeyRaw)
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write(sigBytes)
	return string(h.Sum(nil))
}

// digestRequestBody computes the lowercase hex SHA-256 of the request body and
// replaces req.Body so handlers downstream still see it. For requests with no
// body (GET/HEAD typically) it returns util.EmptyBodySHA256Hex without reading.
// Bodies over v.maxSignedBodyBytes are refused on the declared length where one
// is available, and otherwise as soon as the cap is crossed.
func (v *peerAuthVerifier) digestRequestBody(req *http.Request) (string, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return util.EmptyBodySHA256Hex, nil
	}

	if req.ContentLength > v.maxSignedBodyBytes {
		return "", errSignedBodyTooLarge
	}

	buf, err := io.ReadAll(io.LimitReader(req.Body, v.maxSignedBodyBytes+1))
	_ = req.Body.Close()

	if err != nil {
		return "", err
	}

	if int64(len(buf)) > v.maxSignedBodyBytes {
		return "", errSignedBodyTooLarge
	}

	if len(buf) == 0 {
		req.Body = http.NoBody
		return util.EmptyBodySHA256Hex, nil
	}

	sum := sha256.Sum256(buf)
	req.Body = io.NopCloser(bytes.NewReader(buf))
	req.ContentLength = int64(len(buf))
	return hex.EncodeToString(sum[:]), nil
}

package p2p

import (
	"math"
	"sync"
	"time"
)

// rejectedTxGateClock is the wall clock the gate reads when none is injected.
var rejectedTxGateClock = time.Now

const (
	// defaultRejectedTxPublishRate is the steady-state number of internally
	// rejected transactions per second this node re-broadcasts on the
	// rejected_tx gossip topic. Honest rejections are rare and the topic is
	// informational for recipients (handleRejectedTxTopic takes no action on
	// it), so the budget only has to cover a normal trickle; anything above it
	// is a flood of cheap junk transactions that would otherwise be amplified
	// by the mesh fan-out at this node's expense.
	defaultRejectedTxPublishRate = 10

	// defaultRejectedTxPublishBurst is how many re-broadcasts may be sent back
	// to back before the steady-state rate applies, so a legitimate burst of
	// rejections (e.g. a batch of double spends after a reorg) still goes out.
	defaultRejectedTxPublishBurst = 100

	// rejectedTxSuppressedRateLimited and rejectedTxSuppressedDuplicate label
	// the suppression counter with why a re-broadcast was dropped.
	rejectedTxSuppressedRateLimited = "rate_limited"
	rejectedTxSuppressedDuplicate   = "duplicate"
)

// rejectedTxEgressGate bounds the re-broadcast of internally rejected
// transactions. Every message on the validator's rejected-tx Kafka topic is
// attacker-triggerable at the cost of one invalid transaction submitted to the
// propagation surface, and without this gate each one became a gossipsub
// publish fanned out to every mesh peer. The gate applies two limits before
// the publish:
//
//   - a per-txid dedup on the shared seenHashCache primitive, keyed by our own
//     peer ID as the single announcer, so a txid the validator keeps
//     rejecting is re-broadcast periodically rather than on every rejection
//     (see Dedup period below);
//   - a token bucket on the publish rate, so a distinct-txid flood is capped at
//     the configured rate and burst regardless of how fast the validator
//     rejects.
//
// A grant whose publish never reached the network gives back its dedup slot,
// so the txid can retry. Whether the rate token comes back depends on why:
//
//   - a policy drop (the publish gate refused it: listen mode, FSM state,
//     unknown topic) did no publish work and is handed back in full
//     (rejectedTxGrant.publishFailed);
//   - a publish error (rejectedTxGrant.publishErrored) keeps the token. While
//     the mesh is down or the topic is not joined every publish errors, and a
//     full refund would leave the bucket permanently full: the gate would
//     never engage, and every rejection in a junk flood would cost the full
//     publish work and an error log line. Keeping the token holds both to the
//     configured rate. The price is that the bucket starts an outage's
//     recovery partly drained.
//
// The bucket is this package's own rather than golang.org/x/time/rate because
// that limiter cannot return a token once the reservation's act time has
// passed, which is always the case by the time a publish is known to have
// been dropped.
//
// Dedup period. The seenHashCache window is seenHashPublishWindow (15s), but
// with one announcer the observed period is longer: a repeat inside the window
// is refused on the spent budget, and the first repeat after the window rolls
// over is refused too, because the cache remembers last window's grantee (us)
// and withholds the rollover retry grant from it. Only a repeat after a second
// rollover is granted, so while its entry lives a txid the validator keeps
// rejecting is re-broadcast about once every two windows (~30s).
//
// The entry lives for the seen-hash TTL. When it ages out the next rejection
// starts it fresh and is granted at once, without consulting the window, so
// the two-window period assumes a TTL longer than two windows, as the 2m
// default is:
//
//   - with an effective TTL of two windows or less the period is the TTL
//     itself, and at the one-window floor ttlLocked applies to a shorter
//     p2p_seen_hash_ttl it is one window (~15s);
//   - with a longer TTL each age-out adds one re-broadcast, which can follow
//     the previous one by less than a window.
//
// The window is a package constant shared with the block and subtree
// announcement dedup; of the values passed to setLimits only the TTL changes
// the period.
//
// Like the other bounds in this package there is no unbounded mode: an
// unconfigured gate falls back to the defaults above on first use.
type rejectedTxEgressGate struct {
	mu     sync.Mutex
	bucket *tokenBucket
	seen   seenHashCache
	// clock is the single time source for allow and publishFailed, so a test
	// on a synthetic timeline cannot have the bucket rewound by one call that
	// reads the wall clock. Nil means time.Now.
	clock func() time.Time
}

// rejectedTxGrant is one approved re-broadcast: the dedup slot and the rate
// token allow took for a txid. It exists so a publish that did not reach the
// network can return exactly what it was charged.
type rejectedTxGrant struct {
	gate   *rejectedTxEgressGate
	bucket *tokenBucket
	txID   string
	selfID string
}

// tokenBucket is a refundable token bucket: perSecond tokens are added per
// second up to burst, take spends one, and refund returns one (never above
// burst, so a refund into a full bucket is simply lost, which is right: nothing
// was sent and nothing is owed).
type tokenBucket struct {
	mu        sync.Mutex
	perSecond float64
	burst     float64
	tokens    float64
	last      time.Time
}

// newTokenBucket returns a full bucket.
func newTokenBucket(perSecond, burst int) *tokenBucket {
	return &tokenBucket{perSecond: float64(perSecond), burst: float64(burst), tokens: float64(burst)}
}

// advanceLocked credits the refill earned since the last call. Time is never
// allowed to run backwards: a now earlier than last neither refills nor moves
// last, so out-of-order callers cannot mint tokens.
func (b *tokenBucket) advanceLocked(now time.Time) {
	if b.last.IsZero() {
		b.last = now
		return
	}

	if !now.After(b.last) {
		return
	}

	b.tokens = math.Min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.perSecond)
	b.last = now
}

// take spends one token if one is available.
func (b *tokenBucket) take(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.advanceLocked(now)

	if b.tokens < 1 {
		return false
	}

	b.tokens--

	return true
}

// refund returns one token, capped at burst.
func (b *tokenBucket) refund(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.advanceLocked(now)
	b.tokens = math.Min(b.burst, b.tokens+1)
}

// setLimits configures the publish rate and burst and the dedup cache bounds.
// Non-positive values select the package defaults.
func (g *rejectedTxEgressGate) setLimits(perSecond, burst, seenMaxSize int, seenTTL time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if perSecond <= 0 {
		perSecond = defaultRejectedTxPublishRate
	}

	if burst <= 0 {
		burst = defaultRejectedTxPublishBurst
	}

	g.bucket = newTokenBucket(perSecond, burst)
	// One publisher: this node is the only announcer on its own egress path.
	g.seen.setLimits(seenMaxSize, 1, seenTTL)
}

// bucketLocked returns the token bucket, building the default one on first use.
func (g *rejectedTxEgressGate) bucketLocked() *tokenBucket {
	if g.bucket == nil {
		g.bucket = newTokenBucket(defaultRejectedTxPublishRate, defaultRejectedTxPublishBurst)
		g.seen.setLimits(0, 1, 0)
	}

	return g.bucket
}

// now reads the gate's clock.
func (g *rejectedTxEgressGate) now() time.Time {
	g.mu.Lock()
	clock := g.clock
	g.mu.Unlock()

	if clock == nil {
		return rejectedTxGateClock()
	}

	return clock()
}

// allow reports whether the rejected transaction txID may be re-broadcast now.
// On approval it returns the grant to hand back should the publish not reach
// the network; otherwise the grant is nil and reason is one of the
// rejectedTxSuppressed* labels. The dedup check runs first so a repeated txid
// never spends a rate-limit token; a txid refused by the rate limiter has its
// dedup grant returned so it is not also treated as already announced.
func (g *rejectedTxEgressGate) allow(txID, selfID string) (grant *rejectedTxGrant, reason string) {
	g.mu.Lock()
	bucket := g.bucketLocked()
	g.mu.Unlock()

	now := g.now()

	if publish, _ := g.seen.Check(txID, selfID, now); !publish {
		return nil, rejectedTxSuppressedDuplicate
	}

	if !bucket.take(now) {
		g.seen.PublishFailed(txID, selfID)

		return nil, rejectedTxSuppressedRateLimited
	}

	return &rejectedTxGrant{gate: g, bucket: bucket, txID: txID, selfID: selfID}, ""
}

// publishFailed returns the grant after a re-broadcast that allow approved was
// dropped before any publish was attempted: the dedup slot, so the next
// rejection of the same txid can retry rather than being suppressed as a
// duplicate, and the rate token, since no publish work was done.
func (gr *rejectedTxGrant) publishFailed() {
	gr.bucket.refund(gr.gate.now())
	gr.gate.seen.PublishFailed(gr.txID, gr.selfID)
}

// publishErrored returns the dedup slot after a publish that was attempted and
// errored, so the txid can retry, but keeps the rate token: a persistently
// failing publish must stay rate limited rather than turning the gate into a
// no-op (see rejectedTxEgressGate).
func (gr *rejectedTxGrant) publishErrored() {
	gr.gate.seen.PublishFailed(gr.txID, gr.selfID)
}

// deleteExpired drops dedup entries whose accounting window has passed and
// returns how many. The cache self-bounds at insert, so this only reclaims
// memory for txids that stopped being rejected.
func (g *rejectedTxEgressGate) deleteExpired(now time.Time) int {
	return g.seen.DeleteExpired(now)
}

// size returns the number of txids currently tracked by the dedup.
func (g *rejectedTxEgressGate) size() int {
	return g.seen.Len()
}

// clear drops the dedup state. The bucket is kept: it holds no memory worth
// freeing and a refilled bucket on restart would only widen the burst.
func (g *rejectedTxEgressGate) clear() {
	g.seen.Clear()
}

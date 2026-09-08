package p2p

import (
	"math"
	"sync"
	"time"
)

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
//     peer ID as the single announcer, so a repeated txid is re-broadcast at
//     most once per publish window;
//   - a token bucket on the publish rate, so a distinct-txid flood is capped at
//     the configured rate and burst regardless of how fast the validator
//     rejects.
//
// Both limits charge for network egress, so a grant whose publish never
// reached the network is handed back in full (rejectedTxGrant.publishFailed):
// the dedup slot so the txid can retry, and the rate token so a burst during a
// brief mesh outage does not throttle the first rejections that can actually
// be published once it clears. The bucket is this package's own rather than
// golang.org/x/time/rate because that limiter cannot return a token once the
// reservation's act time has passed, which is always the case by the time a
// publish is known to have failed.
//
// Dedup period. The seenHashCache window is seenHashPublishWindow (15s), but
// with one announcer the observed period is longer: a repeat inside the window
// is refused on the spent budget, and the first repeat after the window rolls
// over is refused too, because the cache remembers last window's grantee (us)
// and withholds the rollover retry grant from it. Only a repeat after a second
// rollover is granted, so a txid the validator keeps rejecting is re-broadcast
// about once every two windows (~30s) until its entry ages out at the seen-hash
// TTL and it starts fresh. Neither value passed to setLimits changes this; the
// window is a package constant shared with the block and subtree announcement
// dedup.
//
// Like the other bounds in this package there is no unbounded mode: an
// unconfigured gate falls back to the defaults above on first use.
type rejectedTxEgressGate struct {
	mu     sync.Mutex
	bucket *tokenBucket
	seen   seenHashCache
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

// allow reports whether the rejected transaction txID may be re-broadcast now.
// On approval it returns the grant to hand back should the publish not reach
// the network; otherwise the grant is nil and reason is one of the
// rejectedTxSuppressed* labels. The dedup check runs first so a repeated txid
// never spends a rate-limit token; a txid refused by the rate limiter has its
// dedup grant returned so it is not also treated as already announced.
func (g *rejectedTxEgressGate) allow(txID, selfID string, now time.Time) (grant *rejectedTxGrant, reason string) {
	g.mu.Lock()
	bucket := g.bucketLocked()
	g.mu.Unlock()

	if publish, _ := g.seen.Check(txID, selfID, now); !publish {
		return nil, rejectedTxSuppressedDuplicate
	}

	if !bucket.take(now) {
		g.seen.PublishFailed(txID, selfID)

		return nil, rejectedTxSuppressedRateLimited
	}

	return &rejectedTxGrant{gate: g, bucket: bucket, txID: txID, selfID: selfID}, ""
}

// publishFailed returns the grant after a re-broadcast that allow approved did
// not actually reach the network: the dedup slot, so the next rejection of the
// same txid can retry rather than being suppressed as a duplicate, and the
// rate token, since nothing was sent.
func (gr *rejectedTxGrant) publishFailed() {
	gr.bucket.refund(time.Now())
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

// clear drops the dedup state. The limiter is kept: it holds no memory worth
// freeing and a refilled bucket on restart would only widen the burst.
func (g *rejectedTxEgressGate) clear() {
	g.seen.Clear()
}

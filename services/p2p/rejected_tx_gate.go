package p2p

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
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
//     most once per publish window (a grant that never reached the network is
//     handed back via publishFailed so the txid can retry);
//   - a token bucket on the publish rate, so a distinct-txid flood is capped at
//     the configured rate and burst regardless of how fast the validator
//     rejects.
//
// Like the other bounds in this package there is no unbounded mode: an
// unconfigured gate falls back to the defaults above on first use.
type rejectedTxEgressGate struct {
	mu      sync.Mutex
	limiter *rate.Limiter
	seen    seenHashCache
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

	g.limiter = rate.NewLimiter(rate.Limit(perSecond), burst)
	// One publisher: this node is the only announcer on its own egress path.
	g.seen.setLimits(seenMaxSize, 1, seenTTL)
}

// limiterLocked returns the token bucket, building the default one on first use.
func (g *rejectedTxEgressGate) limiterLocked() *rate.Limiter {
	if g.limiter == nil {
		g.limiter = rate.NewLimiter(rate.Limit(defaultRejectedTxPublishRate), defaultRejectedTxPublishBurst)
		g.seen.setLimits(0, 1, 0)
	}

	return g.limiter
}

// allow reports whether the rejected transaction txID may be re-broadcast now.
// When it may not, reason is one of the rejectedTxSuppressed* labels. The
// dedup check runs first so a repeated txid never spends a rate-limit token;
// a txid refused by the rate limiter has its dedup grant returned so it is
// not also treated as already announced.
func (g *rejectedTxEgressGate) allow(txID, selfID string, now time.Time) (ok bool, reason string) {
	g.mu.Lock()
	limiter := g.limiterLocked()
	g.mu.Unlock()

	if publish, _ := g.seen.Check(txID, selfID, now); !publish {
		return false, rejectedTxSuppressedDuplicate
	}

	if !limiter.AllowN(now, 1) {
		g.seen.PublishFailed(txID, selfID)
		return false, rejectedTxSuppressedRateLimited
	}

	return true, ""
}

// publishFailed returns the dedup grant for txID after a re-broadcast that
// allow approved did not actually reach the network, so the next rejection of
// the same txid can retry rather than being suppressed as a duplicate.
func (g *rejectedTxEgressGate) publishFailed(txID, selfID string) {
	g.seen.PublishFailed(txID, selfID)
}

// clear drops the dedup state. The limiter is kept: it holds no memory worth
// freeing and a refilled bucket on restart would only widen the burst.
func (g *rejectedTxEgressGate) clear() {
	g.seen.Clear()
}

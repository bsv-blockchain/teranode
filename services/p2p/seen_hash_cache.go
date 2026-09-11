package p2p

import (
	"container/list"
	"sync"
	"time"
)

const (
	// defaultSeenHashCacheSize bounds each per-topic seen-hash cache. Inserts
	// are driven entirely by untrusted gossip, so the bound is enforced inline
	// at insert (oldest entry evicted), mirroring cappedPeerMap: a distinct-hash
	// flood can rotate the cache but never grow it.
	defaultSeenHashCacheSize = 10_000

	// defaultSeenHashCacheTTL is how long an entry keeps its per-peer repeat
	// accounting. Suppression itself is governed by the much shorter publish
	// window below; this longer window only has to outlast a replay campaign
	// so the per-peer repeat counts can reach seenHashRepeatWarnThreshold and
	// give the operator one signal per peer per hash.
	defaultSeenHashCacheTTL = 2 * time.Minute

	// seenHashMaxPublishersPerHash is how many DISTINCT announcers of one hash
	// get their announcement published to Kafka per publish window. More than
	// one, because the first announcer's DataHub URL is peer-controlled and may
	// be dead or hostile: block validation collects the later announcements as
	// alternative fetch sources (catchupAlternatives), a failover that a budget
	// of one would starve. That failover is only reached when
	// blockvalidation_useCatchupWhenBehind is enabled (default off); with it
	// off the extra announcements still buy URL redundancy against a dead
	// first source, since each carries its announcer's own DataHub URL. The
	// budget is partially Sybil-consumable — colluding identities can take the
	// grants honest announcers do not win by racing, degrading the redundancy
	// without eliminating it — so it is configurable via
	// p2p_seen_hash_max_publishers (this constant is the default).
	seenHashMaxPublishersPerHash = 3

	// seenHashPublishWindow is how long a spent publisher budget stays spent.
	// Deliberately much shorter than the accounting TTL: the burst in which
	// every peer announces one new hash is over in about a second, so a short
	// window loses no dedup value — while a budget that stayed spent for the
	// whole TTL would let seenHashMaxPublishersPerHash colluding identities
	// that win the announcement race own every fetch source block validation
	// is given for that hash for two minutes. With the window, a captured
	// budget re-opens in seconds, and the steady-state amplification is
	// bounded at the budget per window per hash. The rollover also lets one
	// repeat announcement through per window (the published == 0 retry path)
	// — a few messages per minute, which matters in sparse topologies where
	// the only announcer's earlier publish may have been dropped — but that
	// grant is refused to any peer that took a grant in the PREVIOUS window,
	// so once the announcer tracking is full (when the retry grant is the only
	// reachable one) a single persistent announcer cannot hold it across
	// consecutive windows and an honest late announcer gets it within a window
	// or two. A fleet alternating two or more identities can still rotate the
	// retry grant between them; that residual needs identity-cost rate
	// limiting (issue 2870), not more state here — and it is currently
	// UNOBSERVED: holding the grant costs one announcement per window (~8 per
	// accounting TTL, and the count resets with the entry), far under
	// seenHashRepeatWarnThreshold, while the peer being suppressed is
	// untracked so its peerRepeats stays 0 and its suppression logs only at
	// debug level. Do not expect warn lines from this case.
	seenHashPublishWindow = 15 * time.Second

	// seenHashRepeatWarnThreshold is how many repeat announcements of the same
	// hash by the same peer are tolerated within the TTL before the handler
	// logs a warning — ONCE per peer per hash per TTL. Deliberately a log and
	// not a ban score: the suppression above already removes the amplification
	// (a repeat costs this node only pre-suppression handler work), while a
	// degraded honest peer can repeat a hash fast — a crash-looping blockchain
	// service replays its tip on every 1s subscription reconnect, ~60
	// repeats/minute, and the sender-side suppression in
	// handleBlockNotification exists only on upgraded peers. ReasonSpam at 50
	// points against the default 100-point threshold would turn two such
	// windows into a 24h network-wide ban of an honest node; automated scoring
	// of same-hash repeats is deferred to the rate-limiting work (issue 2870),
	// and the warning gives operators the signal to ban manually meanwhile.
	seenHashRepeatWarnThreshold = 50

	// seenHashMaxAnnouncersPerHash bounds the per-hash announcer tracking so a
	// Sybil fleet announcing one hash cannot grow a single entry without
	// limit. It is a FLOOR: distinct-announcer grants require the peer to be
	// tracked, so the bound in force rises to the configured publisher budget
	// when p2p_seen_hash_max_publishers exceeds it (announcersLocked) —
	// otherwise a raised budget would be silently capped here. It is kept low
	// because the worst-case footprint is the PRODUCT of the bound in force
	// and the size cap — up to defaultSeenHashCacheSize x that many stored
	// peer-ID strings per topic — not the size cap alone. An announcer
	// the bound excluded is treated as a repeat by Check (never handed the
	// distinct-announcer budget, never repeat-counted); the retry grant
	// (published == 0) remains reachable to it, worth at most one publish per
	// window. A peer can also shed its tracking by flooding enough distinct
	// hashes to evict the entry, after which its next announcement publishes
	// as fresh — bounded by the same per-window budget, and one reason the
	// repeat threshold above only warns rather than scores.
	seenHashMaxAnnouncersPerHash = 8
)

// seenHashCache is a size-bounded, TTL-windowed record of which announcement
// hashes have already been processed on a gossip topic. GossipSub only dedups
// on message ID (sender + seqno), so byte-identical announcements with fresh
// seqnos arrive as new messages; without this record every replay is amplified
// into a Kafka publish and the downstream RPC/fetch work it triggers.
//
// Per hash it grants a publish budget (seenHashMaxPublishersPerHash distinct
// announcers per seenHashPublishWindow) and keeps per-peer announcement counts
// across the longer accounting TTL, so the handlers can warn about a peer that
// keeps re-announcing the same hash without penalising the normal case of many
// distinct peers announcing one new block.
// A grant is optimistic: callers whose publish did not actually happen return
// it via PublishFailed, so broker backpressure cannot permanently suppress a
// hash and never resets the repeat counts.
//
// Like cappedPeerMap there is no unbounded mode: an unconfigured zero value
// falls back to the defaults above.
type seenHashCache struct {
	mu            sync.Mutex
	entries       map[string]*list.Element
	order         *list.List // front = oldest, back = newest; values are *seenHashNode
	maxSize       int
	maxPublishers int
	ttl           time.Duration
}

// setLimits configures the size cap, per-window publisher budget and
// accounting TTL. Non-positive values select the package defaults — like
// cappedPeerMap, there is no unbounded mode, so a construction path that
// forgets this call costs configurability, not the bounds.
func (c *seenHashCache) setLimits(maxSize, maxPublishers int, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxSize = maxSize
	c.maxPublishers = maxPublishers
	c.ttl = ttl
}

// seenHashNode is the list payload: the key, when the hash was first announced
// in the current accounting window, when the current publish window opened,
// how many publish grants are outstanding or spent in it, who took them (this
// window and last — the rollover retry grant is refused to last window's
// grantees), and how many times each peer has announced the hash.
type seenHashNode struct {
	hash               string
	firstSeen          time.Time
	publishWindowStart time.Time
	published          int
	grantees           map[string]struct{}
	prevGrantees       map[string]struct{}
	announcers         map[string]int
}

// recordGranteeLocked notes that peerID took a publish grant in the current
// window, so the next window's rollover retry grant can be refused to it.
// PublishFailed withdraws the record along with the budget, so on the handler
// path the set tracks published and stays within the publisher budget; the
// bound below is not the primary limit but a cheap defence for a grant and
// its return straddling a window rollover, and it errs toward allowing a
// retry rather than suppressing one.
func (n *seenHashNode) recordGranteeLocked(peerID string, bound int) {
	if n.grantees == nil {
		n.grantees = make(map[string]struct{}, seenHashMaxPublishersPerHash)
	}

	if len(n.grantees) < bound {
		n.grantees[peerID] = struct{}{}
	}
}

// capLocked returns the size cap in force. Callers must hold the mutex.
func (c *seenHashCache) capLocked() int {
	if c.maxSize <= 0 {
		return defaultSeenHashCacheSize
	}

	return c.maxSize
}

// ttlLocked returns the TTL in force. Callers must hold the mutex.
func (c *seenHashCache) ttlLocked() time.Duration {
	if c.ttl <= 0 {
		return defaultSeenHashCacheTTL
	}

	return c.ttl
}

// publishersLocked returns the per-window publisher budget in force. Callers
// must hold the mutex.
func (c *seenHashCache) publishersLocked() int {
	if c.maxPublishers <= 0 {
		return seenHashMaxPublishersPerHash
	}

	return c.maxPublishers
}

// announcersLocked returns the per-hash announcer tracking bound in force: the
// default, raised to the configured publisher budget when that is larger.
// Distinct-announcer grants require the peer to be TRACKED, so without this a
// p2p_seen_hash_max_publishers above the tracking bound would be silently
// capped at it — the 9th+ distinct announcer could never take a distinct
// grant. Raising the budget therefore also raises the per-entry footprint
// (the worst case is size cap x this bound peer-ID strings). Callers must
// hold the mutex.
func (c *seenHashCache) announcersLocked() int {
	if n := c.publishersLocked(); n > seenHashMaxAnnouncersPerHash {
		return n
	}

	return seenHashMaxAnnouncersPerHash
}

// initLocked prepares the internal structures. Callers must hold the mutex.
func (c *seenHashCache) initLocked() {
	if c.entries == nil {
		c.entries = make(map[string]*list.Element)
		c.order = list.New()
	}
}

// Check records an announcement of hash by peerID and reports whether this
// announcement should be published (publish) and how many times this peer had
// announced the hash before within the window (peerRepeats).
//
// Publish is granted to the first seenHashMaxPublishersPerHash TRACKED
// distinct announcers of a hash per publish window, and additionally to any
// announcement while no grant has stuck in the current window (published ==
// 0, i.e. every earlier grant was returned via PublishFailed, or the window
// just rolled over) so a hash can never be suppressed without having been
// handed to Kafka at least once — except a peer that took a grant in the
// previous window, so the rollover grant rotates away from a persistent
// announcer. Repeats never take the distinct-announcer budget — and neither
// does an announcer the tracking bound excluded, since prev == 0 for such a
// peer on every announcement and treating it as new would let one identity
// drain the budget every window unscored.
func (c *seenHashCache) Check(hash, peerID string, now time.Time) (publish bool, peerRepeats int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.initLocked()

	if element, ok := c.entries[hash]; ok {
		node := element.Value.(*seenHashNode)

		if now.Sub(node.firstSeen) < c.ttlLocked() {
			// Re-open a spent publish budget on the short window, keeping the
			// announcer counts: suppression is meant to collapse the seconds-long
			// announcement burst, while the repeat accounting spans the full TTL.
			// Last window's grantees are remembered so the single rollover retry
			// grant cannot be won by the same identity every window.
			if now.Sub(node.publishWindowStart) >= seenHashPublishWindow {
				node.publishWindowStart = now
				node.published = 0
				node.prevGrantees, node.grantees = node.grantees, nil
			}

			prev, tracked := node.announcers[peerID]
			if tracked || len(node.announcers) < c.announcersLocked() {
				node.announcers[peerID] = prev + 1
				tracked = true
			}

			_, heldLastWindow := node.prevGrantees[peerID]

			publish = (tracked && prev == 0 && node.published < c.publishersLocked()) ||
				(node.published == 0 && !heldLastWindow)
			if publish {
				node.published++
				node.recordGranteeLocked(peerID, c.announcersLocked())
			}

			return publish, prev
		}

		// Accounting window expired: treat as a fresh announcement, reusing the entry.
		node.firstSeen = now
		node.publishWindowStart = now
		node.published = 1
		node.grantees = map[string]struct{}{peerID: {}}
		node.prevGrantees = nil
		node.announcers = map[string]int{peerID: 1}
		c.order.MoveToBack(element)

		return true, 0
	}

	// Loop rather than evict once, for the same reason as cappedPeerMap: a cap
	// lowered on a populated cache must drain via new keys.
	limit := c.capLocked()
	for c.order.Len() >= limit {
		oldest := c.order.Front()
		if oldest == nil {
			break
		}

		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(*seenHashNode).hash)
	}

	c.entries[hash] = c.order.PushBack(&seenHashNode{
		hash:               hash,
		firstSeen:          now,
		publishWindowStart: now,
		published:          1,
		grantees:           map[string]struct{}{peerID: {}},
		announcers:         map[string]int{peerID: 1},
	})

	return true, 0
}

// PublishFailed returns a publish grant for hash that did not result in an
// actual publish (producer backpressure, marshal failure, no producer), so a
// later announcement can retry. The grantee record is withdrawn along with
// the budget: a grant that never reached Kafka must not make peerID "last
// window's grantee" and cost it the rollover retry grant this method exists
// to re-enable. The announcer counts are deliberately kept: a backlogged
// broker must not reset the repeat accounting.
//
// Accepted micro-race: a concurrent Check can roll the publish window between
// the grant and this return, in which case the grantee record has already
// moved to prevGrantees (costing the peer one window's retry grant) and the
// decrement lands on the new window's budget (freeing one extra grant there).
// The two calls sit microseconds apart in one handler invocation against a
// 15-second window, and both effects are bounded to a single grant for a
// single window.
func (c *seenHashCache) PublishFailed(hash, peerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, ok := c.entries[hash]; ok {
		node := element.Value.(*seenHashNode)
		if node.published > 0 {
			node.published--
		}

		delete(node.grantees, peerID)
	}
}

// DeleteExpired removes every entry whose window has passed and returns how
// many it removed. Insertion order tracks firstSeen order (Check moves an
// entry to the back when it resets the window), but the walk is full rather
// than early-exiting, matching cappedPeerMap.DeleteExpired's reasoning.
func (c *seenHashCache) DeleteExpired(now time.Time) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.order == nil {
		return 0
	}

	ttl := c.ttlLocked()
	removed := 0

	for element := c.order.Front(); element != nil; {
		next := element.Next()

		if node := element.Value.(*seenHashNode); now.Sub(node.firstSeen) >= ttl {
			c.order.Remove(element)
			delete(c.entries, node.hash)

			removed++
		}

		element = next
	}

	return removed
}

// Clear removes every entry. The configured cap and TTL survive, mirroring
// cappedPeerMap.Clear.
func (c *seenHashCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = nil
	c.order = nil
}

// Len returns the number of entries.
func (c *seenHashCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.entries)
}

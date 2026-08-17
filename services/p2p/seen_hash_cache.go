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

	// defaultSeenHashCacheTTL is how long announcements of a hash are collapsed
	// into at most seenHashMaxPublishersPerHash Kafka publishes. Short on
	// purpose: past the publisher budget the suppression trades away
	// announcement redundancy, so the window only needs to outlive the burst in
	// which every peer announces the same new block or subtree.
	defaultSeenHashCacheTTL = 2 * time.Minute

	// seenHashMaxPublishersPerHash is how many DISTINCT announcers of one hash
	// get their announcement published to Kafka within the TTL. More than one,
	// because the first announcer's DataHub URL is peer-controlled and may be
	// dead or hostile: block validation collects the later announcements as
	// alternative fetch sources (catchupAlternatives), a failover that a budget
	// of one would starve. Three bounds the amplification at three downstream
	// pipelines per hash per TTL while keeping that failover alive.
	seenHashMaxPublishersPerHash = 3

	// seenHashSpamRepeatTolerance is how many repeat announcements of the same
	// hash by the same peer are tolerated within the TTL before each further
	// repeat is scored as spam. GossipSub's message-ID dedup already removes
	// exact duplicates, but honest same-hash repeats still happen: a reorg away
	// and back re-announces a recent hash, and a blockchain-subscription
	// reconnect replays the current tip (suppressed at the sender by
	// handleBlockNotification, but only on upgraded peers). The tolerance
	// absorbs a burst of those; a replay flood blows through it in under a
	// second and, at 50 spam points per repeat against the default 100-point
	// ban threshold, bans itself two repeats later.
	seenHashSpamRepeatTolerance = 5

	// seenHashMaxAnnouncersPerHash bounds the per-hash announcer counts so a
	// Sybil fleet announcing one hash cannot grow a single entry without limit.
	// Announcers beyond the bound are still suppressed (the publisher budget is
	// long spent by then); the consequence accepted here is that they are not
	// individually counted, so an identity introduced after the bound can
	// replay the hash without accruing spam score — costing this node only the
	// pre-suppression handler work, never a Kafka publish.
	seenHashMaxAnnouncersPerHash = 32
)

// seenHashCache is a size-bounded, TTL-windowed record of which announcement
// hashes have already been processed on a gossip topic. GossipSub only dedups
// on message ID (sender + seqno), so byte-identical announcements with fresh
// seqnos arrive as new messages; without this record every replay is amplified
// into a Kafka publish and the downstream RPC/fetch work it triggers.
//
// Per hash it grants a publish budget (seenHashMaxPublishersPerHash distinct
// announcers) and keeps per-peer announcement counts, so the handlers can
// score a peer that keeps re-announcing the same hash (ReasonSpam) without
// penalising the normal case of many distinct peers announcing one new block.
// A grant is optimistic: callers whose publish did not actually happen return
// it via PublishFailed, so broker backpressure cannot permanently suppress a
// hash and never resets the spam counters.
//
// Like cappedPeerMap there is no unbounded mode: an unconfigured zero value
// falls back to the defaults above.
type seenHashCache struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	order   *list.List // front = oldest, back = newest; values are *seenHashNode
	maxSize int
	ttl     time.Duration
}

// seenHashNode is the list payload: the key, when the hash was first announced
// in the current window, how many publish grants are outstanding or spent, and
// how many times each peer has announced it.
type seenHashNode struct {
	hash       string
	firstSeen  time.Time
	published  int
	announcers map[string]int
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
// Publish is granted to the first seenHashMaxPublishersPerHash DISTINCT
// announcers of a hash, and additionally to any announcement while no grant
// has stuck (published == 0, i.e. every earlier grant was returned via
// PublishFailed) so a hash can never be suppressed without having been handed
// to Kafka at least once. A peer's own repeats are otherwise never published,
// whatever the budget.
func (c *seenHashCache) Check(hash, peerID string, now time.Time) (publish bool, peerRepeats int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.initLocked()

	if element, ok := c.entries[hash]; ok {
		node := element.Value.(*seenHashNode)

		if now.Sub(node.firstSeen) < c.ttlLocked() {
			prev := node.announcers[peerID]
			if prev > 0 || len(node.announcers) < seenHashMaxAnnouncersPerHash {
				node.announcers[peerID] = prev + 1
			}

			publish = (prev == 0 && node.published < seenHashMaxPublishersPerHash) || node.published == 0
			if publish {
				node.published++
			}

			return publish, prev
		}

		// Window expired: treat as a fresh announcement, reusing the entry.
		node.firstSeen = now
		node.published = 1
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
		hash:       hash,
		firstSeen:  now,
		published:  1,
		announcers: map[string]int{peerID: 1},
	})

	return true, 0
}

// PublishFailed returns a publish grant for hash that did not result in an
// actual publish (producer backpressure, marshal failure), so a later
// announcement can retry. The announcer counts are deliberately kept: a
// backlogged broker must not reset spam accounting.
func (c *seenHashCache) PublishFailed(hash string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, ok := c.entries[hash]; ok {
		if node := element.Value.(*seenHashNode); node.published > 0 {
			node.published--
		}
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

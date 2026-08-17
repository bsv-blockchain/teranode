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

	// defaultSeenHashCacheTTL is how long a first announcement of a hash
	// suppresses re-publishes of the same hash to Kafka. Short on purpose: the
	// suppression trades away announcement redundancy (a second peer's
	// announcement of the same hash carries an alternative DataHub URL), so the
	// window only needs to outlive the burst in which every peer announces the
	// same new block or subtree.
	defaultSeenHashCacheTTL = 2 * time.Minute

	// seenHashSpamRepeatTolerance is how many repeat announcements of the same
	// hash by the same peer are tolerated within the TTL before each further
	// repeat is scored as spam. Non-zero because a reorg can legitimately make a
	// node re-announce a recent hash; GossipSub's own message-ID dedup already
	// removes exact duplicates, so anything past this is a deliberate republish.
	seenHashSpamRepeatTolerance = 2

	// seenHashMaxAnnouncersPerHash bounds the per-hash announcer counts so a
	// Sybil fleet announcing one hash cannot grow a single entry without limit.
	// Peers beyond the bound are still suppressed as duplicates; they just are
	// not individually counted for spam scoring.
	seenHashMaxAnnouncersPerHash = 32
)

// seenHashCache is a size-bounded, TTL-windowed record of which announcement
// hashes have already been processed on a gossip topic. GossipSub only dedups
// on message ID (sender + seqno), so byte-identical announcements with fresh
// seqnos arrive as new messages; without this record every replay is amplified
// into a Kafka publish and the downstream RPC/fetch work it triggers.
//
// It also keeps a per-peer announcement count per hash, so the handlers can
// score a peer that keeps re-announcing the same hash (ReasonSpam) without
// penalising the normal case of many distinct peers announcing one new block.
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
// in the current window, and how many times each peer has announced it.
type seenHashNode struct {
	hash       string
	firstSeen  time.Time
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

// Check records an announcement of hash by peerID and reports whether the hash
// was already announced within the TTL (duplicate) and how many times this
// peer had announced it before (peerRepeats). The first announcement of a hash
// returns (false, 0); the first announcement by a DIFFERENT peer of a seen
// hash returns (true, 0) — a duplicate to suppress, but not spam.
func (c *seenHashCache) Check(hash, peerID string, now time.Time) (duplicate bool, peerRepeats int) {
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

			return true, prev
		}

		// Window expired: treat as a fresh announcement, reusing the entry.
		node.firstSeen = now
		node.announcers = map[string]int{peerID: 1}
		c.order.MoveToBack(element)

		return false, 0
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
		announcers: map[string]int{peerID: 1},
	})

	return false, 0
}

// Forget removes the entry for hash so the next announcement is treated as
// fresh. Used when a suppressed-side effect did not actually happen (the Kafka
// publish was dropped under backpressure) and a retry must stay possible.
func (c *seenHashCache) Forget(hash string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, ok := c.entries[hash]; ok {
		c.order.Remove(element)
		delete(c.entries, hash)
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

// Len returns the number of entries.
func (c *seenHashCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.entries)
}

package p2p

import (
	"container/list"
	"sync"
	"time"
)

// boundedTTLCache is a size-bounded map of per-entry expiring values keyed by
// peer ID, for the gossip hot-path caches. Inserts are driven by untrusted
// input (the pubsub author of a message, an identity a remote party mints for
// free), so the bound is enforced inline at insert like cappedPeerMap and
// seenHashCache: at capacity a new key evicts the oldest entry rather than
// growing the map, so a stream of never-before-seen authors rotates the cache
// but cannot grow it between sweeps.
//
// Every entry carries the same TTL and Set refreshes an updated key's position,
// so insertion order is expiry order and the eviction victim is always the
// entry closest to expiring. Get treats an expired entry as a miss and drops
// it; DeleteExpired is the periodic sweep that reclaims entries nobody looks
// up again.
//
// Like the other bounded gossip structures there is no unbounded mode: a zero
// value falls back to defaultPeerMapMaxSize.
type boundedTTLCache[V any] struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	order   *list.List // front = oldest insert, back = newest; values are *boundedTTLEntry[V]
	maxSize int
	// evicted counts entries dropped to make room since the last
	// EvictionsSinceLastRead, so the periodic sweep can report cap pressure.
	evicted int
}

type boundedTTLEntry[V any] struct {
	key       string
	value     V
	expiresAt time.Time
}

// setMaxSize configures the insert cap; a non-positive value selects
// defaultPeerMapMaxSize.
func (c *boundedTTLCache[V]) setMaxSize(maxSize int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if maxSize <= 0 {
		maxSize = defaultPeerMapMaxSize
	}

	c.maxSize = maxSize
}

// capLocked returns the cap in force. Callers must hold the mutex.
func (c *boundedTTLCache[V]) capLocked() int {
	if c.maxSize <= 0 {
		return defaultPeerMapMaxSize
	}

	return c.maxSize
}

// initLocked prepares the internal structures. Callers must hold the mutex.
func (c *boundedTTLCache[V]) initLocked() {
	if c.entries == nil {
		c.entries = make(map[string]*list.Element)
		c.order = list.New()
	}
}

// Get returns the value for key if present and not expired at now. An expired
// entry is removed and reported as a miss.
func (c *boundedTTLCache[V]) Get(key string, now time.Time) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var zero V

	element, ok := c.entries[key]
	if !ok {
		return zero, false
	}

	entry := element.Value.(*boundedTTLEntry[V])
	if !now.Before(entry.expiresAt) {
		c.removeLocked(element)
		return zero, false
	}

	return entry.value, true
}

// Set inserts or refreshes key. A refreshed key moves to the newest position
// so eviction order keeps tracking expiry order; a new key evicts the oldest
// entries while the cache is at or over the cap. The loop, rather than a
// single eviction, drains a map left over-cap by a lowered cap.
func (c *boundedTTLCache[V]) Set(key string, value V, expiresAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.initLocked()

	if element, ok := c.entries[key]; ok {
		entry := element.Value.(*boundedTTLEntry[V])
		entry.value = value
		entry.expiresAt = expiresAt
		c.order.MoveToBack(element)

		return
	}

	limit := c.capLocked()
	for len(c.entries) >= limit {
		oldest := c.order.Front()
		if oldest == nil {
			break
		}

		c.removeLocked(oldest)
		c.evicted++
	}

	c.entries[key] = c.order.PushBack(&boundedTTLEntry[V]{key: key, value: value, expiresAt: expiresAt})
}

// Delete removes key if present.
func (c *boundedTTLCache[V]) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, ok := c.entries[key]; ok {
		c.removeLocked(element)
	}
}

// DeleteExpired removes every entry expired at now and returns how many it
// removed. The walk is full rather than early-exiting: Set refreshes are
// clock-ordered only approximately under concurrency, matching the reasoning
// in cappedPeerMap.DeleteExpired.
func (c *boundedTTLCache[V]) DeleteExpired(now time.Time) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.order == nil {
		return 0
	}

	removed := 0

	for element := c.order.Front(); element != nil; {
		next := element.Next()

		if entry := element.Value.(*boundedTTLEntry[V]); !now.Before(entry.expiresAt) {
			c.removeLocked(element)
			removed++
		}

		element = next
	}

	return removed
}

// EvictionsSinceLastRead returns how many entries the cap evicted since the
// previous call, and resets the counter.
func (c *boundedTTLCache[V]) EvictionsSinceLastRead() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	evicted := c.evicted
	c.evicted = 0

	return evicted
}

// Len returns the number of entries, expired ones included until swept.
func (c *boundedTTLCache[V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.entries)
}

// removeLocked unlinks element and deletes its key. Callers must hold the
// mutex.
func (c *boundedTTLCache[V]) removeLocked(element *list.Element) {
	c.order.Remove(element)
	delete(c.entries, element.Value.(*boundedTTLEntry[V]).key)
}

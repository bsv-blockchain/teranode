package expiringmap

import (
	"sync"
	"time"
)

type itemWrapper[V any] struct {
	item    V
	expiry  int64
	addedAt int64
}

// ExpiringMap is a map that expires items after a given duration.
// It uses go generics to allow for any type of key and value, although
// the key must be comparable.
// The map is safe for concurrent use.
type ExpiringMap[K comparable, V any] struct {
	mu         sync.RWMutex
	expiry     time.Duration
	items      map[K]*itemWrapper[V]
	evictionCh chan []V
	evictionFn func(K, V) bool
	stopCh     chan struct{}
	maxSize    int
}

// New creates a new ExpiringMap with the given expiry duration.
func New[K comparable, V any](expire time.Duration) *ExpiringMap[K, V] {
	m := &ExpiringMap[K, V]{
		expiry: expire,
		items:  make(map[K]*itemWrapper[V]),
		stopCh: make(chan struct{}),
	}

	if expire != 0 {
		ticker := time.NewTicker(expire)
		go func() {
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					m.clean()
				case <-m.stopCh:
					return
				}
			}
		}()
	}

	return m
}

// Stop stops the background cleanup goroutine.
func (m *ExpiringMap[K, V]) Stop() {
	select {
	case <-m.stopCh:
		// already stopped
	default:
		close(m.stopCh)
	}
}

func (m *ExpiringMap[K, V]) WithEvictionChannel(ch chan []V) *ExpiringMap[K, V] {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.evictionCh = ch
	return m
}

func (m *ExpiringMap[K, V]) WithEvictionFunction(f func(K, V) bool) *ExpiringMap[K, V] {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.evictionFn = f
	return m
}

// WithMaxSize bounds the map to at most n entries. Inserting a new key when
// the map is at capacity evicts the oldest entry (by insertion time) before
// the new entry is added. If an eviction function is configured and it vetoes
// the eviction of the oldest entry, the new entry is dropped instead — the
// cap is always honored. A value of 0 (the default) disables the cap and
// preserves the original unbounded behaviour.
//
// Note: cap-eviction's eviction-channel send is non-blocking (the notification
// is dropped if the channel is full), since cap-eviction runs synchronously
// inside Set. The TTL clean() path's eviction-channel send remains blocking.
func (m *ExpiringMap[K, V]) WithMaxSize(n int) *ExpiringMap[K, V] {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.maxSize = n
	return m
}

// Set sets the value for the given key.
func (m *ExpiringMap[K, V]) Set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	if _, exists := m.items[key]; !exists && m.maxSize > 0 && len(m.items) >= m.maxSize {
		if !m.evictOldestLocked() {
			return
		}
	}

	m.items[key] = &itemWrapper[V]{
		item:    value,
		expiry:  now.Add(m.expiry).UnixNano(),
		addedAt: now.UnixNano(),
	}
}

// evictOldestLocked removes the entry with the smallest addedAt timestamp.
// The caller must hold m.mu for writing. The configured eviction function
// and channel (if any) fire for the removed entry, mirroring TTL eviction.
// Returns true if an entry was evicted; false if the eviction function vetoed
// the removal (in which case the caller must not insert a new entry, to
// preserve the cap).
func (m *ExpiringMap[K, V]) evictOldestLocked() bool {
	var (
		oldestKey  K
		oldestItem *itemWrapper[V]
		haveOldest bool
	)

	for key, item := range m.items {
		if !haveOldest || item.addedAt < oldestItem.addedAt {
			oldestKey = key
			oldestItem = item
			haveOldest = true
		}
	}

	if !haveOldest {
		return false
	}

	if m.evictionFn != nil && !m.evictionFn(oldestKey, oldestItem.item) {
		return false
	}

	delete(m.items, oldestKey)

	if m.evictionCh != nil {
		select {
		case m.evictionCh <- []V{oldestItem.item}:
		default:
		}
	}

	return true
}

// Get returns the value for the given key.
func (m *ExpiringMap[K, V]) Get(key K) (value V, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	item, ok := m.items[key]
	if !ok {
		return
	}

	if time.Now().UnixNano() > item.expiry {
		ok = false
		return
	}

	value = item.item
	return
}

// Delete deletes the given key from the map.
func (m *ExpiringMap[K, V]) Delete(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.items, key)
}

// Len returns the number of items in the map.
func (m *ExpiringMap[K, V]) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.items)
}

// IterateWithFn iterates over the map and calls the given function for each item.
// If the function returns false, the iteration stops.
func (m *ExpiringMap[K, V]) IterateWithFn(fn func(K, V) bool, stopCh ...chan bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ch := make(chan bool)
	if len(stopCh) > 0 {
		ch = stopCh[0]
	}

	for key, item := range m.items {
		select {
		case <-ch:
			return
		default:
			if !fn(key, item.item) {
				return
			}
		}
	}
}

// Items returns a copy of the items in the map.
func (m *ExpiringMap[K, V]) Items() map[K]V {
	m.mu.RLock()
	defer m.mu.RUnlock()

	items := make(map[K]V, len(m.items))

	for key, item := range m.items {
		items[key] = item.item
	}

	return items
}

// Clear clears the map.
func (m *ExpiringMap[K, V]) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.items = make(map[K]*itemWrapper[V])
}

func (m *ExpiringMap[K, V]) clean() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UnixNano()

	var expiredItems []V

	for key, item := range m.items {
		if now > item.expiry {
			if m.evictionFn != nil && !m.evictionFn(key, item.item) {
				continue
			}

			if m.evictionCh != nil {
				expiredItems = append(expiredItems, item.item)
			}

			delete(m.items, key)
		}
	}

	if m.evictionCh != nil && len(expiredItems) > 0 {
		m.evictionCh <- expiredItems
	}
}

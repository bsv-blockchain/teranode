package util

import (
	"sync"

	"github.com/dolthub/swiss"
)

type SyncedMap[K comparable, V any] struct {
	mu    sync.RWMutex
	m     map[K]V
	limit int
}

// NewSyncedMap creates a new SyncedMap with an optional limit.
// If the limit is set, the map will automatically delete a random item when the limit is reached.
func NewSyncedMap[K comparable, V any](l ...int) *SyncedMap[K, V] {
	limit := 0
	if len(l) > 0 {
		limit = l[0]
	}

	return &SyncedMap[K, V]{
		m:     make(map[K]V),
		limit: limit,
	}
}

func (m *SyncedMap[K, V]) Length() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.m)
}

func (m *SyncedMap[K, V]) Exists(key K) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.m[key]

	return ok
}

func (m *SyncedMap[K, V]) Get(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, ok := m.m[key]

	return val, ok
}

// Range returns a copy of the map.
// Note that if v is a pointer, the map value may be modified by other goroutines at the same time.
func (m *SyncedMap[K, V]) Range() map[K]V {
	m.mu.RLock()
	defer m.mu.RUnlock()

	items := map[K]V{}

	for k, v := range m.m {
		items[k] = v
	}

	return items
}

// Iterate iterates over the map and calls the function f for each key-value pair.
// unlike Range(), Iterate does not return a copy of the map and keeps the lock for the duration of the iteration.
// If f returns false, the iteration stops.
func (m *SyncedMap[K, V]) Iterate(f func(key K, value V) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for k, v := range m.m {
		if !f(k, v) {
			return
		}
	}
}

func (m *SyncedMap[K, V]) Set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.setUnlocked(key, value)
}

func (m *SyncedMap[K, V]) setUnlocked(key K, value V) {
	if m.limit > 0 && len(m.m) >= m.limit {
		for k := range m.m {
			// delete a random item
			delete(m.m, k)
			break
		}
	}

	m.m[key] = value
}

func (m *SyncedMap[K, V]) SetMulti(keys []K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// add the keys
	for _, key := range keys {
		m.setUnlocked(key, value)
	}
}

func (m *SyncedMap[K, V]) Delete(key K) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, key)

	return true
}

func (m *SyncedMap[K, V]) Clear() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m = make(map[K]V)

	return true
}

type SyncedSlice[V any] struct {
	mu    sync.RWMutex
	items []*V
}

// NewSyncedSlice creates a new SyncedSlice.
func NewSyncedSlice[V any](length ...int) *SyncedSlice[V] {
	initialLength := 0
	if len(length) > 0 {
		initialLength = length[0]
	}

	return &SyncedSlice[V]{
		items: make([]*V, 0, initialLength),
	}
}

func (s *SyncedSlice[V]) Length() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.items)
}

func (s *SyncedSlice[V]) Get(index int) (*V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if index < 0 || index >= len(s.items) {
		return nil, false
	}

	return s.items[index], true
}

func (s *SyncedSlice[V]) Append(item *V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items = append(s.items, item)
}

// Pop removes and returns the last item in the slice.
func (s *SyncedSlice[V]) Pop() (*V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.items) == 0 {
		return nil, false
	}

	item := s.items[len(s.items)-1]
	s.items = s.items[:len(s.items)-1]

	return item, true
}

// Shift removes and returns the first item in the slice.
func (s *SyncedSlice[V]) Shift() (*V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.items) == 0 {
		return nil, false
	}

	item := s.items[0]
	s.items = s.items[1:]

	return item, true
}

type SyncedSwissMap[K comparable, V any] struct {
	mu       sync.RWMutex
	swissMap *swiss.Map[K, V]
}

func NewSyncedSwissMap[K comparable, V any](length uint32) *SyncedSwissMap[K, V] {
	return &SyncedSwissMap[K, V]{
		swissMap: swiss.NewMap[K, V](length),
	}
}

func (m *SyncedSwissMap[K, V]) Get(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.swissMap.Get(key)
}

func (m *SyncedSwissMap[K, V]) Range() map[K]V {
	m.mu.RLock()
	defer m.mu.RUnlock()

	items := map[K]V{}

	m.swissMap.Iter(func(key K, value V) bool {
		items[key] = value
		return false
	})

	return items
}

func (m *SyncedSwissMap[K, V]) Set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.swissMap.Put(key, value)
}

func (m *SyncedSwissMap[K, V]) Length() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.swissMap.Count()
}

func (m *SyncedSwissMap[K, V]) Delete(key K) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.swissMap.Delete(key)
}

func (m *SyncedSwissMap[K, V]) DeleteBatch(keys []K) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	ok := false

	for _, key := range keys {
		ok = m.swissMap.Delete(key)
	}

	return ok
}

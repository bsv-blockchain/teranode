package util

import (
	"sync"

	"github.com/dolthub/swiss"
)

type SyncedMap[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

func NewSyncedMap[K comparable, V any]() *SyncedMap[K, V] {
	return &SyncedMap[K, V]{
		m: make(map[K]V),
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

	m.m[key] = value
}

func (m *SyncedMap[K, V]) SetMulti(keys []K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, key := range keys {
		m.m[key] = value
	}
}

func (m *SyncedMap[K, V]) Delete(key K) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, key)

	return true
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
		return true
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

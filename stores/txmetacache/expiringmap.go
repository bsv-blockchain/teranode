package txmetacache

import (
	"sync"

	"github.com/dolthub/swiss"
	"github.com/libsv/go-bt/v2/chainhash"
)

type ExpiringMap[V any] struct {
	maps      []*SplitSwissMapUint64[V]
	capacity  int
	maxMaps   int
	currIndex int
	mu        sync.RWMutex
}

func NewExpiringMap[V any](capacity, maxMaps int) *ExpiringMap[V] {
	em := &ExpiringMap[V]{
		maps:      make([]*SplitSwissMapUint64[V], maxMaps),
		capacity:  capacity,
		maxMaps:   maxMaps,
		currIndex: 0,
		mu:        sync.RWMutex{},
	}
	for i := 0; i < maxMaps; i++ {
		em.maps[i] = NewSplitSwissMapUint64[V](capacity)
	}
	return em
}

func (m *ExpiringMap[V]) Put(key chainhash.Hash, value V) {
	currentMap := m.maps[m.maxMaps-1]
	if currentMap.length >= m.capacity {
		m.resizeMaps()
		currentMap = m.maps[m.maxMaps-1]
	}
	currentMap.Put(key, value)
}

func (m *ExpiringMap[V]) resizeMaps() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.maps = append(m.maps[1:], NewSplitSwissMapUint64[V](m.capacity))
}

func (m *ExpiringMap[V]) Get(key chainhash.Hash) (V, bool) {
	for i := m.currIndex; i >= 0; i-- {
		if value, ok := m.maps[i].Get(key); ok {
			return value, true
		}
	}
	var zero V
	return zero, false
}

func (m *ExpiringMap[V]) Length() int {
	length := 0
	for _, m := range m.maps {
		length += m.length
	}
	return length
}

type SplitSwissMapUint64[V any] struct {
	m           map[uint16]*SwissMapUint64[V]
	nrOfBuckets uint16
	length      int
}

func NewSplitSwissMapUint64[V any](length int) *SplitSwissMapUint64[V] {
	m := &SplitSwissMapUint64[V]{
		m:           make(map[uint16]*SwissMapUint64[V], 256),
		nrOfBuckets: 1024,
	}

	for i := uint16(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewSwissMapUint64[V](length / int(m.nrOfBuckets))
	}

	return m
}

func (g *SplitSwissMapUint64[V]) Exists(hash chainhash.Hash) bool {
	return g.m[shard(hash, g.nrOfBuckets)].Exists(hash)
}

func (g *SplitSwissMapUint64[V]) Put(hash chainhash.Hash, n V) error {
	g.length++

	return g.m[shard(hash, g.nrOfBuckets)].Put(hash, n)
}

func (g *SplitSwissMapUint64[V]) Get(hash chainhash.Hash) (V, bool) {
	return g.m[shard(hash, g.nrOfBuckets)].Get(hash)
}

func shard(b chainhash.Hash, mod uint16) uint16 {
	return (uint16(b[0])<<8 | uint16(b[1])) % mod
}

type SwissMapUint64[V any] struct {
	m  *swiss.Map[chainhash.Hash, V]
	mu sync.RWMutex
}

func NewSwissMapUint64[V any](length int) *SwissMapUint64[V] {
	return &SwissMapUint64[V]{
		m: swiss.NewMap[chainhash.Hash, V](uint32(length)),
	}
}

func (s *SwissMapUint64[V]) Exists(hash chainhash.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m.Get(hash)
	return ok
}

func (s *SwissMapUint64[V]) Put(hash chainhash.Hash, n V) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// exists := s.m.Has(hash)
	// if exists {
	// 	return fmt.Errorf("hash already exists in map")
	// }

	s.m.Put(hash, n)

	return nil
}

func (s *SwissMapUint64[V]) Get(hash chainhash.Hash) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n, ok := s.m.Get(hash)
	if !ok {
		var zeroValue V
		return zeroValue, false
	}

	return n, true
}

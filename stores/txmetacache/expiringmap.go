package txmetacache

import (
	"sync"
	"sync/atomic"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/dolthub/swiss"
	"github.com/libsv/go-bt/v2/chainhash"
)

type ExpiringMap struct {
	maps     []*SplitSwissMap
	current  atomic.Pointer[SplitSwissMap]
	capacity int
	maxMaps  int
	mu       sync.RWMutex
}

func NewExpiringMap(capacity, maxMaps int) *ExpiringMap {
	em := &ExpiringMap{
		maps:     make([]*SplitSwissMap, maxMaps),
		capacity: capacity,
		maxMaps:  maxMaps,
		mu:       sync.RWMutex{},
	}
	for i := 0; i < maxMaps; i++ {
		em.maps[i] = NewSplitSwissMap(capacity)
	}
	em.current.Store(em.maps[2])
	return em
}

func (m *ExpiringMap) Put(key chainhash.Hash, value txmeta.Data) {
	current := m.current.Load()
	if int(current.length.Load()) >= m.capacity {
		m.resizeMaps()
		current = m.current.Load()
	}
	_ = currentMap.Put(key, value)
}

func (m *ExpiringMap) resizeMaps() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.maps = append(m.maps[1:], NewSplitSwissMap(m.capacity))
	m.current.Store(m.maps[2])
}

func (m *ExpiringMap) Get(key chainhash.Hash) (txmeta.Data, bool) {
	for i := 2; i >= 0; i-- {
		if value, ok := m.maps[i].Get(key); ok {
			return value, true
		}
	}
	var zero txmeta.Data
	return zero, false
}

func (m *ExpiringMap) Length() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	length := 0
	for _, m := range m.maps {
		length += int(m.length.Load())
	}
	return length
}

type SplitSwissMap struct {
	m      map[[1]byte]*SwissMap
	length atomic.Int32
}

func NewSplitSwissMap(length int) *SplitSwissMap {
	m := &SplitSwissMap{
		m:      make(map[[1]byte]*SwissMap, 256),
		length: atomic.Int32{},
	}

	for i := 0; i <= 256; i++ {
		m.m[[1]byte{uint8(i)}] = NewSwissMap(length / 256)
	}

	return m
}

func (g *SplitSwissMap) Exists(hash chainhash.Hash) bool {
	return g.m[shard(hash)].Exists(hash)
}

func (g *SplitSwissMap) Put(hash chainhash.Hash, n txmeta.Data) error {
	g.length.Add(1)

	return g.m[shard(hash)].Put(hash, n)
}

func (g *SplitSwissMap) Get(hash chainhash.Hash) (txmeta.Data, bool) {
	return g.m[shard(hash)].Get(hash)
}

func (g *SplitSwissMap) Length() int {
	return int(g.length.Load())
}

func shard(b chainhash.Hash) [1]byte {
	// return (uint16(b[0])<<8 | uint16(b[1])) % mod
	return [1]byte{b[0]}
}

type SwissMap struct {
	m  *swiss.Map[chainhash.Hash, txmeta.Data]
	mu sync.RWMutex
}

func NewSwissMap(length int) *SwissMap {
	return &SwissMap{
		m: swiss.NewMap[chainhash.Hash, txmeta.Data](uint32(length)),
	}
}

func (s *SwissMap) Exists(hash chainhash.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m.Get(hash)
	return ok
}

func (s *SwissMap) Put(hash chainhash.Hash, n txmeta.Data) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// exists := s.m.Has(hash)
	// if exists {
	// 	return fmt.Errorf("hash already exists in map")
	// }

	s.m.Put(hash, n)

	return nil
}

func (s *SwissMap) Get(hash chainhash.Hash) (txmeta.Data, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n, ok := s.m.Get(hash)
	if !ok {
		var zeroValue txmeta.Data
		return zeroValue, false
	}

	return n, true
}

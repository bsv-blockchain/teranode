package txmap

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/dolthub/swiss"
	"github.com/libsv/go-bt/v2/chainhash"
)

type TxMap interface {
	Put(hash chainhash.Hash, value uint64) error
	Get(hash chainhash.Hash) (uint64, bool)
	Exists(hash chainhash.Hash) bool
	Length() int
	Keys() []chainhash.Hash
}

type SwissMap struct {
	mu     sync.RWMutex
	m      *swiss.Map[chainhash.Hash, struct{}]
	length int
}

func NewSwissMap(length uint32) *SwissMap {
	return &SwissMap{
		m: swiss.NewMap[chainhash.Hash, struct{}](length),
	}
}

func (s *SwissMap) Exists(hash chainhash.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m.Get(hash)

	return ok
}

func (s *SwissMap) Get(hash chainhash.Hash) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m.Get(hash)

	return 0, ok
}

func (s *SwissMap) Put(hash chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length++

	s.m.Put(hash, struct{}{})

	return nil
}

func (s *SwissMap) PutMulti(hashes []chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, hash := range hashes {
		s.m.Put(hash, struct{}{})

		s.length++
	}

	return nil
}

func (s *SwissMap) Delete(hash chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length--

	s.m.Delete(hash)

	return nil
}

func (s *SwissMap) Length() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.length
}

func (s *SwissMap) Keys() []chainhash.Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]chainhash.Hash, 0, s.length)

	s.m.Iter(func(k chainhash.Hash, v struct{}) (stop bool) {
		keys = append(keys, k)
		return false
	})

	return keys
}

type SwissMapUint64 struct {
	mu     sync.Mutex
	m      *swiss.Map[chainhash.Hash, uint64]
	length int
}

func NewSwissMapUint64(length uint32) *SwissMapUint64 {
	return &SwissMapUint64{
		m: swiss.NewMap[chainhash.Hash, uint64](length),
	}
}

func (s *SwissMapUint64) Map() *swiss.Map[chainhash.Hash, uint64] {
	return s.m
}

func (s *SwissMapUint64) Exists(hash chainhash.Hash) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.m.Get(hash)

	return ok
}

func (s *SwissMapUint64) Put(hash chainhash.Hash, n uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	exists := s.m.Has(hash)
	if exists {
		return fmt.Errorf("hash already exists in map: %v", hash)
	}

	s.m.Put(hash, n)

	s.length++

	return nil
}

func (s *SwissMapUint64) Get(hash chainhash.Hash) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length++

	n, ok := s.m.Get(hash)
	if !ok {
		return 0, false
	}

	return n, true
}

func (s *SwissMapUint64) Length() int {
	return s.length
}

func (s *SwissMapUint64) Keys() []chainhash.Hash {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]chainhash.Hash, 0, s.length)

	s.m.Iter(func(k chainhash.Hash, v uint64) (stop bool) {
		keys = append(keys, k)
		return false
	})

	return keys
}

// Lock-free map for uint64 keys and values
type SwissMapKVUint64 struct {
	m      *swiss.Map[uint64, uint64]
	length atomic.Uint32
}

func NewSwissMapKVUint64(length int) *SwissMapKVUint64 {
	return &SwissMapKVUint64{
		m:      swiss.NewMap[uint64, uint64](uint32(length)),
		length: atomic.Uint32{},
	}
}

func (s *SwissMapKVUint64) Map() *swiss.Map[uint64, uint64] {
	return s.m
}

func (s *SwissMapKVUint64) Exists(hash uint64) bool {
	_, ok := s.m.Get(hash)
	return ok
}

func (s *SwissMapKVUint64) Put(hash uint64, n uint64) error {
	exists := s.m.Has(hash)
	if exists {
		return fmt.Errorf("hash already exists in map")
	}

	s.m.Put(hash, n)
	s.length.Add(1)

	return nil
}

func (s *SwissMapKVUint64) Get(hash uint64) (uint64, bool) {
	// s.length.Add(1)
	n, ok := s.m.Get(hash)
	if !ok {
		return 0, false
	}

	return n, true
}

func (s *SwissMapKVUint64) Length() int {
	return int(s.length.Load())
}

type SplitSwissMap struct {
	m           map[uint16]*SwissMap
	nrOfBuckets uint16
}

func (g *SplitSwissMap) Keys() []chainhash.Hash {
	keys := make([]chainhash.Hash, 0, g.Length())

	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		keys = append(keys, g.m[i].Keys()...)
	}

	return keys
}

func NewSplitSwissMap(length int) *SplitSwissMap {
	m := &SplitSwissMap{
		m:           make(map[uint16]*SwissMap, 1024),
		nrOfBuckets: 1024,
	}

	for i := uint16(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewSwissMap(uint32(math.Ceil(float64(length) / float64(m.nrOfBuckets))))
	}

	return m
}

func (g *SplitSwissMap) Buckets() uint16 {
	return g.nrOfBuckets
}

func (g *SplitSwissMap) Exists(hash chainhash.Hash) bool {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Exists(hash)
}

func (g *SplitSwissMap) Get(hash chainhash.Hash) (uint64, bool) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Get(hash)
}

func (g *SplitSwissMap) Put(hash chainhash.Hash, n uint64) error {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Put(hash)
}
func (g *SplitSwissMap) PutMulti(bucket uint16, hashes []chainhash.Hash) error {
	return g.m[bucket].PutMulti(hashes)
}

func (g *SplitSwissMap) Length() int {
	length := 0
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		length += g.m[i].Length()
	}

	return length
}

type SplitSwissMapUint64 struct {
	m           map[uint16]*SwissMapUint64
	nrOfBuckets uint16
}

func (g *SplitSwissMapUint64) Keys() []chainhash.Hash {
	keys := make([]chainhash.Hash, 0, g.Length())

	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		keys = append(keys, g.m[i].Keys()...)
	}

	return keys
}

func NewSplitSwissMapUint64(length uint32) *SplitSwissMapUint64 {
	m := &SplitSwissMapUint64{
		m:           make(map[uint16]*SwissMapUint64, 256),
		nrOfBuckets: 1024,
	}

	for i := uint16(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewSwissMapUint64(length / uint32(m.nrOfBuckets))
	}

	return m
}

func (g *SplitSwissMapUint64) Exists(hash chainhash.Hash) bool {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Exists(hash)
}

func (g *SplitSwissMapUint64) Map() map[uint16]*SwissMapUint64 {
	return g.m
}

func (g *SplitSwissMapUint64) Put(hash chainhash.Hash, n uint64) error {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Put(hash, n)
}

func (g *SplitSwissMapUint64) Get(hash chainhash.Hash) (uint64, bool) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Get(hash)
}

func (g *SplitSwissMapUint64) Length() int {
	length := 0
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		length += g.m[i].length
	}

	return length
}

type SplitSwissMapKVUint64 struct {
	m           map[uint64]*SwissMapKVUint64
	nrOfBuckets uint64
}

func NewSplitSwissMapKVUint64(length int) *SplitSwissMapKVUint64 {
	m := &SplitSwissMapKVUint64{
		m:           make(map[uint64]*SwissMapKVUint64, 256),
		nrOfBuckets: 1024,
	}

	for i := uint64(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewSwissMapKVUint64(length / int(m.nrOfBuckets))
	}

	return m
}

func (g *SplitSwissMapKVUint64) Exists(hash uint64) bool {
	return g.m[hash%g.nrOfBuckets].Exists(hash)
}

func (g *SplitSwissMapKVUint64) Map() map[uint64]*SwissMapKVUint64 {
	return g.m
}

func (g *SplitSwissMapKVUint64) Put(hash uint64, n uint64) error {
	return g.m[hash%g.nrOfBuckets].Put(hash, n)
}

func (g *SplitSwissMapKVUint64) Get(hash uint64) (uint64, bool) {
	return g.m[hash%g.nrOfBuckets].Get(hash)
}

func (g *SplitSwissMapKVUint64) Length() int {
	length := 0
	for i := uint64(0); i <= g.nrOfBuckets; i++ {
		length += int(g.m[i].length.Load())
	}

	return length
}

type SplitGoMap struct {
	m           map[uint16]*SyncedMap[chainhash.Hash, struct{}]
	nrOfBuckets uint16
}

func NewSplitGoMap(length int) *SplitGoMap {
	m := &SplitGoMap{
		m:           make(map[uint16]*SyncedMap[chainhash.Hash, struct{}], length),
		nrOfBuckets: 1024,
	}

	for i := uint16(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewSyncedMap[chainhash.Hash, struct{}]()
	}

	return m
}

func (g *SplitGoMap) Buckets() uint16 {
	return g.nrOfBuckets
}

func (g *SplitGoMap) Exists(hash chainhash.Hash) bool {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Exists(hash)
}

func (g *SplitGoMap) Get(hash chainhash.Hash) (uint64, bool) {
	_, ok := g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Get(hash)
	return 0, ok
}

func (g *SplitGoMap) Put(hash chainhash.Hash, n uint64) error {
	g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Set(hash, struct{}{})
	return nil
}

func (g *SplitGoMap) PutMulti(bucket uint16, hashes []chainhash.Hash) error {
	g.m[bucket].SetMulti(hashes, struct{}{})
	return nil
}

func (g *SplitGoMap) Length() int {
	length := 0
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		length += g.m[i].Length()
	}

	return length
}

func Bytes2Uint16Buckets(b chainhash.Hash, mod uint16) uint16 {
	return (uint16(b[0])<<8 | uint16(b[1])) % mod
}

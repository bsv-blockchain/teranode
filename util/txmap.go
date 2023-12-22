package util

import (
	"fmt"
	"math"
	"sync"

	"github.com/dolthub/swiss"
)

type TxMap interface {
	Put(hash [32]byte, value uint64) error
	Get(hash [32]byte) (uint64, bool)
	Exists(hash [32]byte) bool
	Length() int
}

type SwissMap struct {
	mu     sync.RWMutex
	m      *swiss.Map[[32]byte, struct{}]
	length int
}

func NewSwissMap(length int) *SwissMap {
	return &SwissMap{
		m: swiss.NewMap[[32]byte, struct{}](uint32(length)),
	}
}

func (s *SwissMap) Exists(hash [32]byte) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m.Get(hash)
	return ok
}

func (s *SwissMap) Get(hash [32]byte) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m.Get(hash)

	return 0, ok
}

func (s *SwissMap) Put(hash [32]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length++

	s.m.Put(hash, struct{}{})
	return nil
}

func (s *SwissMap) PutMulti(hashes [][32]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, hash := range hashes {
		s.m.Put(hash, struct{}{})
		s.length++
	}
	return nil
}

func (s *SwissMap) Delete(hash [32]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length--

	s.m.Delete(hash)
	return nil
}

func (s *SwissMap) Length() int {
	return s.length
}

type SwissMapUint64 struct {
	mu     sync.Mutex
	m      *swiss.Map[[32]byte, uint64]
	length int
}

func NewSwissMapUint64(length int) *SwissMapUint64 {
	return &SwissMapUint64{
		m: swiss.NewMap[[32]byte, uint64](uint32(length)),
	}
}

func (s *SwissMapUint64) Exists(hash [32]byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.m.Get(hash)
	return ok
}

func (s *SwissMapUint64) Put(hash [32]byte, n uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	exists := s.m.Has(hash)
	if exists {
		return fmt.Errorf("hash already exists in map")
	}

	s.m.Put(hash, n)
	s.length++

	return nil
}

func (s *SwissMapUint64) Get(hash [32]byte) (uint64, bool) {
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

type SplitSwissMap struct {
	m           map[uint16]*SwissMap
	nrOfBuckets uint16
}

func NewSplitSwissMap(length int) *SplitSwissMap {
	m := &SplitSwissMap{
		m:           make(map[uint16]*SwissMap, 1024),
		nrOfBuckets: 1024,
	}

	for i := uint16(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewSwissMap(int(math.Ceil(float64(length) / float64(m.nrOfBuckets))))
	}

	return m
}

func (g *SplitSwissMap) Buckets() uint16 {
	return g.nrOfBuckets
}

func (g *SplitSwissMap) Exists(hash [32]byte) bool {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Exists(hash)
}

func (g *SplitSwissMap) Get(hash [32]byte) (uint64, bool) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Get(hash)
}

func (g *SplitSwissMap) Put(hash [32]byte, n uint64) error {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Put(hash)
}
func (g *SplitSwissMap) PutMulti(bucket uint16, hashes [][32]byte) error {
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

func NewSplitSwissMapUint64(length int) *SplitSwissMapUint64 {
	m := &SplitSwissMapUint64{
		m:           make(map[uint16]*SwissMapUint64, 256),
		nrOfBuckets: 1024,
	}

	for i := uint16(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewSwissMapUint64(length / int(m.nrOfBuckets))
	}

	return m
}

func (g *SplitSwissMapUint64) Exists(hash [32]byte) bool {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Exists(hash)
}

func (g *SplitSwissMapUint64) Put(hash [32]byte, n uint64) error {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Put(hash, n)
}

func (g *SplitSwissMapUint64) Get(hash [32]byte) (uint64, bool) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Get(hash)
}

func (g *SplitSwissMapUint64) Length() int {
	length := 0
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		length += g.m[i].length
	}

	return length
}

type SplitGoMap struct {
	m           map[uint16]*SyncedMap[[32]byte, struct{}]
	nrOfBuckets uint16
}

func NewSplitGoMap(length int) *SplitGoMap {
	m := &SplitGoMap{
		m:           make(map[uint16]*SyncedMap[[32]byte, struct{}], length),
		nrOfBuckets: 1024,
	}

	for i := uint16(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewSyncedMap[[32]byte, struct{}]()
	}

	return m
}

func (g *SplitGoMap) Buckets() uint16 {
	return g.nrOfBuckets
}

func (g *SplitGoMap) Exists(hash [32]byte) bool {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Exists(hash)
}

func (g *SplitGoMap) Get(hash [32]byte) (uint64, bool) {
	_, ok := g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Get(hash)
	return 0, ok
}

func (g *SplitGoMap) Put(hash [32]byte, n uint64) error {
	g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Set(hash, struct{}{})
	return nil
}
func (g *SplitGoMap) PutMulti(bucket uint16, hashes [][32]byte) error {
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

func Bytes2Uint16Buckets(b [32]byte, mod uint16) uint16 {
	return (uint16(b[0])<<8 | uint16(b[1])) % mod
}

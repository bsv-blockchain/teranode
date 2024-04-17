package model

import (
	"math"
	"sync"

	"github.com/dolthub/swiss"
)

// genericMap is an interface that defines the methods that a map must implement
type genericMap[K comparable, V any] interface {
	Put(K, V)
	Get(K) (V, bool)
	Delete(K) bool
	Iter(cb func(K, V) (stop bool))
	Exists(K) bool
	Length() int
}

// hashable is a custom interface that defines a method that must be implemented by a type in order to be used as a key in a splitSwiss map
type hashable interface {
	Hash(mod uint16) uint16
}

// make a composite interface that combines the comparable and hashable interfaces
type comparableAndHashable interface {
	comparable // This is a Go built-in interface
	hashable   // This is a custom interface defined above
}

type splitSwissMap[K comparableAndHashable, V any] struct {
	m           map[uint16]genericMap[K, V]
	nrOfBuckets uint16
}

func newSplitSwissMap[K comparableAndHashable, V any](length int) genericMap[K, V] {
	ssm := &splitSwissMap[K, V]{
		m:           make(map[uint16]genericMap[K, V], 1024),
		nrOfBuckets: 1024,
	}

	splitLength := int(math.Ceil(float64(length) / float64(ssm.nrOfBuckets)))

	for i := uint16(0); i <= uint16(ssm.nrOfBuckets); i++ {
		ssm.m[i] = newSwissMap[K, V](splitLength)
	}

	return ssm
}

func (ssm *splitSwissMap[K, V]) Put(k K, v V) {
	ssm.m[k.Hash(ssm.nrOfBuckets)].Put(k, v)
}

func (ssm *splitSwissMap[K, V]) Get(k K) (V, bool) {
	return ssm.m[k.Hash(ssm.nrOfBuckets)].Get(k)
}

func (ssm *splitSwissMap[K, V]) Delete(k K) bool {
	return ssm.m[k.Hash(ssm.nrOfBuckets)].Delete(k)
}

func (ssm *splitSwissMap[K, V]) Iter(cb func(K, V) (stop bool)) {
	for i := uint16(0); i <= ssm.nrOfBuckets; i++ {
		ssm.m[i].Iter(cb)
	}
}

func (ssm *splitSwissMap[K, V]) Exists(k K) bool {
	return ssm.m[k.Hash(ssm.nrOfBuckets)].Exists(k)
}

func (ssm *splitSwissMap[K, V]) Length() int {
	length := 0
	for i := uint16(0); i <= ssm.nrOfBuckets; i++ {
		length += ssm.m[i].Length()
	}

	return length
}

type swissMap[K comparable, V any] struct {
	mu     sync.RWMutex
	m      *swiss.Map[K, V]
	length int
}

func newSwissMap[K comparable, V any](length int) genericMap[K, V] {
	return &swissMap[K, V]{
		m: swiss.NewMap[K, V](uint32(length)),
	}
}

func (s *swissMap[K, V]) Put(k K, v V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length++

	s.m.Put(k, v)
}

func (s *swissMap[K, V]) Get(k K) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.m.Get(k)
}

func (s *swissMap[K, V]) Delete(k K) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length--

	return s.m.Delete(k)
}

func (s *swissMap[K, V]) Iter(cb func(K, V) (stop bool)) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.m.Iter(cb)
}

func (s *swissMap[K, V]) Exists(k K) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m.Get(k)
	return ok
}

func (s *swissMap[K, V]) Length() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.length
}

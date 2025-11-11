// Package model defines the core data structures for UTXO (Unspent Transaction Output) management.
//
// This package provides the fundamental types and operations for handling UTXOs in the Teranode
// block persister service. UTXOs represent the unspent outputs from Bitcoin transactions that
// can be used as inputs in future transactions, forming the basis of Bitcoin's accounting model.
//
// The package includes:
//   - UTXO: Core structure representing an unspent transaction output
//   - UTXOKey: Unique identifier for UTXOs combining transaction hash and output index
//   - UTXOValue: Value and script data associated with a UTXO
//   - UTXOSet: Collection of UTXOs with efficient lookup and modification operations
//   - UTXODiff: Difference sets for tracking UTXO changes during block processing
//   - UTXOMap: High-performance map implementation optimized for UTXO operations
//
// Thread safety:
// Map implementations (swissMap, goMap) and UTXOSetCache provide built-in thread safety
// using sync.RWMutex for concurrent access protection.
package model

import (
	"fmt"
	"math"
	"sync"

	"github.com/dolthub/swiss"
)

// genericMap is an interface that defines the methods that a map must implement
type genericMap[K comparable, V any] interface {
	// Put stores a value V for key K
	Put(K, V)

	// Get retrieves a value V for key K, returns (value, exists)
	Get(K) (V, bool)

	// Delete removes the entry for key K, returns true if the key existed
	Delete(K) bool

	// Iter iterates over all entries, calling cb for each.
	// If cb returns true, iteration stops
	Iter(cb func(K, V) (stop bool))

	// Exists checks if a key exists in the map
	Exists(K) bool

	// Length returns the number of entries in the map
	Length() int

	// IsEqual compares this map with another using the provided comparison function
	IsEqual(other genericMap[K, V], fn ValueCompareFn[V]) (bool, string)
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

// splitMap implements a sharded map for better concurrency
type splitMap[K comparableAndHashable, V any] struct {
	m           map[uint16]genericMap[K, V]
	nrOfBuckets uint16
}

// NewSplitSwissMap creates a new sharded map using Swiss tables for high-performance UTXO operations.
//
// This function creates a split map that uses Swiss table implementations for each shard,
// providing excellent performance characteristics for large-scale UTXO set operations.
// The map is sharded across multiple buckets to reduce contention in concurrent scenarios
// and improve cache locality.
//
// Swiss tables offer faster lookups and better memory efficiency compared to standard
// Go maps, making them ideal for the high-throughput requirements of blockchain
// transaction processing where millions of UTXOs may need to be accessed rapidly.
//
// Parameters:
//   - length: Initial capacity hint for sizing the underlying storage structures
//
// Returns a genericMap interface backed by sharded Swiss table implementations.
// The returned map is thread-safe and optimized for concurrent access patterns
// typical in blockchain processing workloads.
func NewSplitSwissMap[K comparableAndHashable, V any](length int) genericMap[K, V] {
	ssm := &splitMap[K, V]{
		m:           make(map[uint16]genericMap[K, V], 1024),
		nrOfBuckets: 1024,
	}

	splitLength := int(math.Ceil(float64(length) / float64(ssm.nrOfBuckets)))

	for i := uint16(0); i <= ssm.nrOfBuckets; i++ {
		ssm.m[i] = newSwissMap[K, V](splitLength)
	}

	return ssm
}

// NewSplitGoMap creates a new sharded map using standard Go maps for UTXO operations.
//
// This function creates a split map that uses standard Go map implementations for each shard,
// providing good performance characteristics while maintaining compatibility with standard
// Go runtime optimizations. The map is sharded across multiple buckets to reduce contention
// in concurrent scenarios.
//
// While not as performant as Swiss tables for very large datasets, Go maps offer excellent
// compatibility and are well-optimized by the Go runtime. This implementation is suitable
// for moderate-scale UTXO operations or when Swiss table dependencies are not desired.
//
// Parameters:
//   - length: Initial capacity hint for sizing the underlying storage structures (currently unused)
//
// Returns a genericMap interface backed by sharded standard Go map implementations.
// The returned map is thread-safe and provides good performance for typical blockchain
// processing workloads with reasonable UTXO set sizes.
func NewSplitGoMap[K comparableAndHashable, V any](length int) genericMap[K, V] {
	ssm := &splitMap[K, V]{
		m:           make(map[uint16]genericMap[K, V], 1024),
		nrOfBuckets: 1024,
	}

	for i := uint16(0); i <= ssm.nrOfBuckets; i++ {
		ssm.m[i] = NewGoMap[K, V]()
	}

	return ssm
}

func (ssm *splitMap[K, V]) Put(k K, v V) {
	ssm.m[k.Hash(ssm.nrOfBuckets)].Put(k, v)
}

func (ssm *splitMap[K, V]) Get(k K) (V, bool) {
	return ssm.m[k.Hash(ssm.nrOfBuckets)].Get(k)
}

func (ssm *splitMap[K, V]) Delete(k K) bool {
	return ssm.m[k.Hash(ssm.nrOfBuckets)].Delete(k)
}

func (ssm *splitMap[K, V]) Iter(cb func(K, V) (stop bool)) {
	for i := uint16(0); i <= ssm.nrOfBuckets; i++ {
		ssm.m[i].Iter(cb)
	}
}

func (ssm *splitMap[K, V]) Exists(k K) bool {
	return ssm.m[k.Hash(ssm.nrOfBuckets)].Exists(k)
}

func (ssm *splitMap[K, V]) Length() int {
	length := 0
	for i := uint16(0); i <= ssm.nrOfBuckets; i++ {
		length += ssm.m[i].Length()
	}

	return length
}

type ValueCompareFn[V any] func(V, V) bool

func (ssm *splitMap[K, V]) IsEqual(other genericMap[K, V], fn ValueCompareFn[V]) (bool, string) {
	if ssm.Length() != other.Length() {
		return false, fmt.Sprintf("Lengths are different: %d vs %d", ssm.Length(), other.Length())
	}

	// compare contents of ssm with other
	different := false

	var difference string

	ssm.Iter(func(k K, v V) bool {
		otherV, ok := other.Get(k)
		if !ok || !fn(v, otherV) {
			different = true
			difference = fmt.Sprintf("Values are different for key %v: %v vs %v", k, v, otherV)

			return false // stop iterating
		}

		return true // continue iterating
	})

	if different {
		return false, difference
	}

	return true, ""
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

	if !s.m.Has(k) {
		// Only increment the length if the key is new
		// TODO: This is a hack to get around the fact that the swiss map doesn't have a length method
		s.length++
	}

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

	deleted := s.m.Delete(k)

	if deleted {
		s.length-- // Only decrement the length if the key was actually deleted
	}

	return deleted
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

func (ssm *swissMap[K, V]) IsEqual(other genericMap[K, V], fn ValueCompareFn[V]) (bool, string) {
	if ssm.Length() != other.Length() {
		return false, fmt.Sprintf("Lengths are different: %d vs %d", ssm.Length(), other.Length())
	}

	// compare contents of ssm with other
	different := false

	var difference string

	ssm.Iter(func(k K, v V) bool {
		otherV, ok := other.Get(k)
		if !ok || !fn(v, otherV) {
			different = true
			difference = fmt.Sprintf("Values are different for key %v: %v vs %v", k, v, otherV)

			return false // stop iterating
		}

		return true // continue iterating
	})

	if different {
		return false, difference
	}

	return true, ""
}

type goMap[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

func NewGoMap[K comparable, V any]() genericMap[K, V] {
	return &goMap[K, V]{
		m: make(map[K]V),
	}
}

func (s *goMap[K, V]) Put(k K, v V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[k] = v
}

func (s *goMap[K, V]) Get(k K) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.m[k]

	return v, ok
}

func (s *goMap[K, V]) Delete(k K) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.m, k)

	return true
}

func (s *goMap[K, V]) Iter(cb func(K, V) (stop bool)) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for k, v := range s.m {
		if cb(k, v) {
			return
		}
	}
}

func (s *goMap[K, V]) Exists(k K) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m[k]

	return ok
}

func (s *goMap[K, V]) Length() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.m)
}

func (ssm *goMap[K, V]) IsEqual(other genericMap[K, V], fn ValueCompareFn[V]) (bool, string) {
	if ssm.Length() != other.Length() {
		return false, fmt.Sprintf("Lengths are different: %d vs %d", ssm.Length(), other.Length())
	}

	// compare contents of ssm with other
	different := false

	var difference string

	ssm.Iter(func(k K, v V) bool {
		otherV, ok := other.Get(k)
		if !ok || !fn(v, otherV) {
			different = true
			difference = fmt.Sprintf("Values are different for key %v: %v vs %v", k, v, otherV)

			return false // stop iterating
		}

		return true // continue iterating
	})

	if different {
		return false, difference
	}

	return true, ""
}

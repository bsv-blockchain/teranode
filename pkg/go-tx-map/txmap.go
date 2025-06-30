package txmap

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/dolthub/swiss"
	"github.com/libsv/go-bt/v2/chainhash"
)

// TxMap is a map that stores transaction hashes and associated uint64 values.
type TxMap interface {
	Put(hash chainhash.Hash, value uint64) error
	PutMulti(hashes []chainhash.Hash, value uint64) error
	Get(hash chainhash.Hash) (uint64, bool)
	Exists(hash chainhash.Hash) bool
	Length() int
	Keys() []chainhash.Hash
	Delete(hash chainhash.Hash) error
}

// TxMapUint64 is a map that stores uint64's and associated uint64 value.
type TxMapUint64 interface {
	Put(hash, value uint64) error
	Get(hash uint64) (uint64, bool)
	Exists(hash uint64) bool
	Length() int
}

// TxHashMap is a map that stores transaction hashes without any associated value.
type TxHashMap interface {
	Put(hash chainhash.Hash) error
	PutMulti(hashes []chainhash.Hash) error
	Get(hash chainhash.Hash) (uint64, bool)
	Exists(hash chainhash.Hash) bool
	Length() int
	Keys() []chainhash.Hash
	Delete(hash chainhash.Hash) error
}

// SwissMap is a simple concurrent-safe map that uses the swiss package
type SwissMap struct {
	mu     sync.RWMutex
	m      *swiss.Map[chainhash.Hash, struct{}]
	length int
}

// NewSwissMap creates a new SwissMap with the specified initial length.
// The length is used to preallocate the map size for better performance.
// It is not a hard limit, but a hint to the underlying swiss map.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *SwissMap: A pointer to the newly created SwissMap instance.
//
// Note: The length is not enforced, and the map can grow beyond this size.
func NewSwissMap(length uint32) *SwissMap {
	return &SwissMap{
		m: swiss.NewMap[chainhash.Hash, struct{}](length),
	}
}

// Exists checks if the given hash exists in the map.
// It returns true if the hash is found, false otherwise.
//
// Params:
//   - hash: The hash to check for existence in the map.
//
// Returns:
//   - bool: True if the hash exists in the map, false otherwise.
func (s *SwissMap) Exists(hash chainhash.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m.Get(hash)

	return ok
}

// Get retrieves the value associated with the given hash from the map.
// It always returns 0 and a boolean indicating whether the hash was found.
//
// Params:
//   - hash: The hash to retrieve from the map.
//
// Returns:
//   - uint64: Always returns 0, as this map does not store values.
//   - bool: True if the hash was found in the map, false otherwise.
func (s *SwissMap) Get(hash chainhash.Hash) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m.Get(hash)

	return 0, ok
}

// Put adds a new hash to the map. It increments the length of the map.
//
// Params:
//   - hash: The hash to add to the map.
//
// Returns:
//   - error: always returns nil, as this map does not have any constraints on adding hashes.
func (s *SwissMap) Put(hash chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length++

	s.m.Put(hash, struct{}{})

	return nil
}

// PutMulti adds multiple hashes to the map. It increments the length of the map for each hash added.
//
// Params:
//   - hashes: A slice of hashes to add to the map.
//
// Returns:
//   - error: always returns nil, as this map does not have any constraints on adding hashes.
func (s *SwissMap) PutMulti(hashes []chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, hash := range hashes {
		s.m.Put(hash, struct{}{})

		s.length++
	}

	return nil
}

// Delete removes a hash from the map. It decrements the length of the map.
//
// Params:
//   - hash: The hash to remove from the map.
//
// Returns:
//   - error: always returns nil, as this map does not have any constraints on deleting hashes.
func (s *SwissMap) Delete(hash chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length--

	s.m.Delete(hash)

	return nil
}

// Length returns the current number of hashes in the map.
//
// Returns:
//   - int: The number of hashes currently stored in the map.
func (s *SwissMap) Length() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.length
}

// Keys returns a slice of all hashes currently stored in the map.
// It iterates over the map and collects the keys.
// The order of keys is not guaranteed.
//
// Returns:
//   - []chainhash.Hash: A slice containing all the hashes in the map.
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

func (s *SwissMap) Map() TxHashMap {
	return s
}

// SwissMapUint64 is a concurrent-safe map that uses the swiss package to store
// transaction hashes as keys and uint64 values.
type SwissMapUint64 struct {
	mu     sync.RWMutex
	m      *swiss.Map[chainhash.Hash, uint64]
	length int
}

// NewSwissMapUint64 creates a new SwissMapUint64 with the specified initial length.
// The length is used to preallocate the map size for better performance.
// It is not a hard limit, but a hint to the underlying swiss map.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *SwissMapUint64: A pointer to the newly created SwissMapUint64 instance.
func NewSwissMapUint64(length uint32) *SwissMapUint64 {
	return &SwissMapUint64{
		m: swiss.NewMap[chainhash.Hash, uint64](length),
	}
}

// Map returns the underlying swiss map used by SwissMapUint64.
//
// Returns:
//   - *swiss.Map[chainhash.Hash, uint64]: A pointer to the underlying swiss map.
func (s *SwissMapUint64) Map() *swiss.Map[chainhash.Hash, uint64] {
	return s.m
}

// Exists checks if the given hash exists in the map.
// It returns true if the hash is found, false otherwise.
//
// Params:
//   - hash: The hash to check for existence in the map.
//
// Returns:
//   - bool: True if the hash exists in the map, false otherwise.
func (s *SwissMapUint64) Exists(hash chainhash.Hash) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.m.Get(hash)

	return ok
}

// Put adds a new hash with an associated uint64 value to the map.
// It checks if the hash already exists in the map and returns an error if it does.
// If the hash does not exist, it adds the hash and increments the length of the map.
//
// Params:
//   - hash: The hash to add to the map.
//   - n: The uint64 value to associate with the hash.
//
// Returns:
//   - error: An error if the hash already exists in the map, nil otherwise.
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

// PutMulti adds multiple hashes with an associated uint64 value to the map.
// It checks if any of the hashes already exist in the map and returns an error if any do.
// If none of the hashes exist, it adds each hash with the value and increments the length of the map.
//
// Params:
//   - hashes: A slice of hashes to add to the map.
//   - n: The uint64 value to associate with each hash.
//
// Returns:
//   - error: An error if any of the hashes already exist in the map, nil otherwise.
func (s *SwissMapUint64) PutMulti(hashes []chainhash.Hash, n uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, hash := range hashes {
		exists := s.m.Has(hash)
		if exists {
			return fmt.Errorf("hash already exists in map: %v", hash)
		}

		s.m.Put(hash, n)

		s.length++
	}

	return nil
}

// Get retrieves the uint64 value associated with the given hash from the map.
// It locks the map for reading, checks if the hash exists, and returns the value and a boolean indicating success.
// If the hash does not exist, it returns 0 and false.
//
// Params:
//   - hash: The hash to retrieve from the map.
//
// Returns:
//   - uint64: The value associated with the hash, or 0 if the hash does not exist.
//   - bool: True if the hash was found in the map, false otherwise.
func (s *SwissMapUint64) Get(hash chainhash.Hash) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n, ok := s.m.Get(hash)
	if !ok {
		return 0, false
	}

	return n, true
}

// Length returns the current number of hashes in the map.
// It locks the map for reading and returns the length.
//
// Returns:
//   - int: The number of hashes currently stored in the map.
func (s *SwissMapUint64) Length() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.length
}

// Keys returns a slice of all hashes currently stored in the map.
// It locks the map for reading, iterates over the map, and collects the keys.
// The order of keys is not guaranteed.
//
// Returns:
//   - []chainhash.Hash: A slice containing all the hashes in the map.
func (s *SwissMapUint64) Keys() []chainhash.Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]chainhash.Hash, 0, s.length)

	s.m.Iter(func(k chainhash.Hash, v uint64) (stop bool) {
		keys = append(keys, k)
		return false
	})

	return keys
}

// Delete removes a hash from the map. It decrements the length of the map.
// It locks the map for writing, checks if the hash exists, and removes it if found.
// If the hash does not exist, it returns an error.
//
// Params:
//   - hash: The hash to remove from the map.
//
// Returns:
//   - error: An error if the hash does not exist in the map, nil otherwise.
func (s *SwissMapUint64) Delete(hash chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.m.Has(hash) {
		return fmt.Errorf("hash %s does not exist in map", hash)
	}

	s.m.Delete(hash)

	s.length--

	return nil
}

// SwissLockFreeMapUint64 is a lock-free map for uint64 keys and values
type SwissLockFreeMapUint64 struct {
	m      *swiss.Map[uint64, uint64]
	length atomic.Uint32
}

// NewSwissLockFreeMapUint64 creates a new SwissLockFreeMapUint64 with the specified initial length.
// The length is used to preallocate the map size for better performance.
// It is not a hard limit, but a hint to the underlying swiss map.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *SwissLockFreeMapUint64: A pointer to the newly created SwissLockFreeMapUint64 instance.
func NewSwissLockFreeMapUint64(length int) *SwissLockFreeMapUint64 {
	return &SwissLockFreeMapUint64{
		m:      swiss.NewMap[uint64, uint64](uint32(length)),
		length: atomic.Uint32{},
	}
}

// Map returns the underlying swiss map used by SwissLockFreeMapUint64.
// It provides access to the map for operations that do not require locking.
//
// Returns:
//   - *swiss.Map[uint64, uint64]: A pointer to the underlying swiss map.
//
// Note: This method does not lock the map, so it is not suitable for concurrent access.
func (s *SwissLockFreeMapUint64) Map() *swiss.Map[uint64, uint64] {
	return s.m
}

// Exists checks if the given hash exists in the map.
//
// Params:
//   - hash: The hash to check for existence in the map.
//
// Returns:
//   - bool: True if the hash exists in the map, false otherwise.
//
// Note: This method does not lock the map, so it is not suitable for concurrent access.
func (s *SwissLockFreeMapUint64) Exists(hash uint64) bool {
	_, ok := s.m.Get(hash)
	return ok
}

// Put adds a new hash with an associated uint64 value to the map.
// It checks if the hash already exists in the map and returns an error if it does.
// If the hash does not exist, it adds the hash and increments the length of the map.
//
// Params:
//   - hash: The hash to add to the map.
//   - n: The uint64 value to associate with the hash.
//
// Returns:
//   - error: An error if the hash already exists in the map, nil otherwise.
//
// Note: This method does not lock the map, so it is not suitable for concurrent access.
func (s *SwissLockFreeMapUint64) Put(hash uint64, n uint64) error {
	exists := s.m.Has(hash)
	if exists {
		return fmt.Errorf("hash already exists in map")
	}

	s.m.Put(hash, n)
	s.length.Add(1)

	return nil
}

// Get retrieves the uint64 value associated with the given hash from the map.
//
// Params:
//   - hash: The hash to retrieve from the map.
//
// Returns:
//   - uint64: The value associated with the hash, or 0 if the hash does not exist.
//   - bool: True if the hash was found in the map, false otherwise.
//
// Note: This method does not lock the map, so it is not suitable for concurrent access.
func (s *SwissLockFreeMapUint64) Get(hash uint64) (uint64, bool) {
	n, ok := s.m.Get(hash)
	if !ok {
		return 0, false
	}

	return n, true
}

// Length returns the current number of hashes in the map.
//
// Returns:
//   - int: The number of hashes currently stored in the map.
//
// Note: This method uses atomic operations to retrieve the length, making it safe for concurrent access.
func (s *SwissLockFreeMapUint64) Length() int {
	return int(s.length.Load())
}

// SplitSwissMap is a map that splits the data into multiple buckets to reduce contention.
// It uses SwissMapUint64 for each bucket to store the hashes and their associated uint64 values.
// Since SwissMapUint64 is concurrent-safe, SplitSwissMap can handle concurrent access without additional locks.
type SplitSwissMap struct {
	m           map[uint16]*SwissMapUint64
	nrOfBuckets uint16
}

// NewSplitSwissMap creates a new SplitSwissMap with the specified initial length.
// The length is used to preallocate the size of each bucket.
// It divides the length by the number of buckets to determine the size of each bucket.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *SplitSwissMap: A pointer to the newly created SplitSwissMap instance.
//
// Note: The number of buckets is fixed at 1024, and the length is divided by this number to determine the size of each bucket.
func NewSplitSwissMap(length int) *SplitSwissMap {
	m := &SplitSwissMap{
		m:           make(map[uint16]*SwissMapUint64, 1024),
		nrOfBuckets: 1024,
	}

	for i := uint16(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewSwissMapUint64(uint32(math.Ceil(float64(length) / float64(m.nrOfBuckets))))
	}

	return m
}

// Buckets returns the number of buckets in the SplitSwissMap.
func (g *SplitSwissMap) Buckets() uint16 {
	return g.nrOfBuckets
}

// Exists checks if the given hash exists in the map.
// It calculates the bucket index using the Bytes2Uint16Buckets function and checks the corresponding bucket.
//
// Params:
//   - hash: The hash to check for existence in the map.
//
// Returns:
//   - bool: True if the hash exists in the map, false otherwise.
func (g *SplitSwissMap) Exists(hash chainhash.Hash) bool {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Exists(hash)
}

// Get retrieves the uint64 value associated with the given hash from the map.
// It calculates the bucket index using the Bytes2Uint16Buckets function and retrieves the value from the corresponding bucket.
//
// Params:
//   - hash: The hash to retrieve from the map.
//
// Returns:
//   - uint64: The value associated with the hash, or 0 if the hash does not exist.
//   - bool: True if the hash was found in the map, false otherwise.
func (g *SplitSwissMap) Get(hash chainhash.Hash) (uint64, bool) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Get(hash)
}

// Put adds a new hash with an associated uint64 value to the map.
// It calculates the bucket index using the Bytes2Uint16Buckets function and adds the hash to the corresponding bucket.
// It checks if the hash already exists in the bucket and returns an error if it does.
//
// Params:
//   - hash: The hash to add to the map.
//   - n: The uint64 value to associate with the hash.
//
// Returns:
//   - error: An error if the hash already exists in the map, nil otherwise.
func (g *SplitSwissMap) Put(hash chainhash.Hash, n uint64) error {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Put(hash, n)
}

// PutMulti adds multiple hashes with an associated uint64 value to the map.
// It iterates over the hashes, calculates the bucket index for each hash using the Bytes2Uint16Buckets function,
// and adds each hash to the corresponding bucket.
// It checks if any of the hashes already exist in the bucket and returns an error if any do.
//
// Params:
//   - hashes: A slice of hashes to add to the map.
//   - n: The uint64 value to associate with each hash.
//
// Returns:
//   - error: An error if any of the hashes already exist in the map, nil otherwise.
func (g *SplitSwissMap) PutMulti(hashes []chainhash.Hash, n uint64) (err error) {
	for _, hash := range hashes {
		if err = g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Put(hash, n); err != nil {
			return fmt.Errorf("failed to put multi in bucket %d: %w", Bytes2Uint16Buckets(hash, g.nrOfBuckets), err)
		}
	}

	return nil
}

// PutMultiBucket adds multiple hashes with an associated uint64 value to a specific bucket.
// It checks if the bucket exists and then adds the hashes directly to that bucket.
//
// Params:
//   - bucket: The bucket index to add the hashes to.
//   - hashes: A slice of hashes to add to the specified bucket.
//   - n: The uint64 value to associate with each hash.
//
// Returns:
//   - error: An error if the bucket does not exist or if there is an issue adding the hashes, nil otherwise.
func (g *SplitSwissMap) PutMultiBucket(bucket uint16, hashes []chainhash.Hash, n uint64) error {
	if bucket > g.nrOfBuckets {
		return fmt.Errorf("bucket %d does not exist, max bucket is %d", bucket, g.nrOfBuckets)
	}

	return g.m[bucket].PutMulti(hashes, n)
}

// Keys returns a slice of all hashes currently stored in the map.
// It iterates over all buckets and collects the keys from each bucket.
// The order of keys is not guaranteed.
//
// Returns:
//   - []chainhash.Hash: A slice containing all the hashes in the map.
func (g *SplitSwissMap) Keys() []chainhash.Hash {
	keys := make([]chainhash.Hash, 0, g.Length())

	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		keys = append(keys, g.m[i].Keys()...)
	}

	return keys
}

// Length returns the current number of hashes in the map.
// It iterates over all buckets and sums their lengths to get the total count.
//
// Returns:
//   - int: The number of hashes currently stored in the map.
func (g *SplitSwissMap) Length() int {
	length := 0

	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		length += g.m[i].Length()
	}

	return length
}

// Delete removes a hash from the map.
// It calculates the bucket index using the Bytes2Uint16Buckets function and checks the corresponding bucket for the hash.
//
// Params:
//   - hash: The hash to remove from the map.
//
// Returns:
//   - error: An error if the hash does not exist in the map or if the bucket does not exist, nil otherwise.
func (g *SplitSwissMap) Delete(hash chainhash.Hash) error {
	bucket := Bytes2Uint16Buckets(hash, g.nrOfBuckets)

	if _, ok := g.m[bucket]; !ok {
		return fmt.Errorf("bucket %d does not exist", bucket)
	}

	if !g.m[bucket].Exists(hash) {
		return fmt.Errorf("hash %s does not exist in bucket %d", hash, bucket)
	}

	return g.m[bucket].Delete(hash)
}

// Map returns the underlying map of all buckets used by SplitSwissMap.
//
// Returns:
//   - TxMap: A map where the keys are bucket indices and the values are pointers to SwissMapUint64 instances.
func (g *SplitSwissMap) Map() *SwissMapUint64 {
	m := NewSwissMapUint64(uint32(g.Length())) // nolint:gosec
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		keys := g.m[i].Keys()
		for _, key := range keys {
			val, _ := g.m[i].Get(key)
			_ = m.Put(key, val)
		}
	}

	return m
}

// SplitSwissMapUint64 is a map that splits the data into multiple buckets to reduce contention.
// It uses SwissMapUint64 for each bucket to store the hashes and their associated uint64 values.
// The number of buckets is fixed at 1024, and the length is divided by this number to determine the size of each bucket.
type SplitSwissMapUint64 struct {
	m           map[uint16]*SwissMapUint64
	nrOfBuckets uint16
}

// NewSplitSwissMapUint64 creates a new SplitSwissMapUint64 with the specified initial length.
// The length is used to preallocate the size of each bucket.
// It divides the length by the number of buckets to determine the size of each bucket.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *SplitSwissMapUint64: A pointer to the newly created SplitSwissMapUint64 instance.
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

// Exists checks if the given hash exists in the map.
// It calculates the bucket index using the Bytes2Uint16Buckets function and checks the corresponding bucket.
//
// Params:
//   - hash: The hash to check for existence in the map.
//
// Returns:
//   - bool: True if the hash exists in the map, false otherwise.
func (g *SplitSwissMapUint64) Exists(hash chainhash.Hash) bool {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Exists(hash)
}

// Map returns the underlying map of buckets used by SplitSwissMapUint64.
//
// Returns:
//   - map[uint16]*SwissMapUint64: A map where the keys are bucket indices and the values are pointers to SwissMapUint64 instances.
func (g *SplitSwissMapUint64) Map() map[uint16]*SwissMapUint64 {
	return g.m
}

// Put adds a new hash with an associated uint64 value to the map.
// It calculates the bucket index using the Bytes2Uint16Buckets function and adds the hash to the corresponding bucket.
// It checks if the hash already exists in the bucket and returns an error if it does.
//
// Params:
//   - hash: The hash to add to the map.
//   - n: The uint64 value to associate with the hash.
//
// Returns:
//   - error: An error if the hash already exists in the map, nil otherwise.
func (g *SplitSwissMapUint64) Put(hash chainhash.Hash, n uint64) error {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Put(hash, n)
}

// PutMulti adds multiple hashes with an associated uint64 value to the map.
// It iterates over the hashes, calculates the bucket index for each hash using the Bytes2Uint16Buckets function,
// and adds each hash to the corresponding bucket.
// It checks if any of the hashes already exist in the bucket and returns an error if any do.
//
// Params:
//   - hashes: A slice of hashes to add to the map.
//   - n: The uint64 value to associate with each hash.
//
// Returns:
//   - error: An error if any of the hashes already exist in the map, nil otherwise.
func (g *SplitSwissMapUint64) PutMulti(hashes []chainhash.Hash, n uint64) error {
	for _, hash := range hashes {
		if err := g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Put(hash, n); err != nil {
			return fmt.Errorf("failed to put multi in bucket %d: %w", Bytes2Uint16Buckets(hash, g.nrOfBuckets), err)
		}
	}

	return nil
}

// Get retrieves the uint64 value associated with the given hash from the map.
// It calculates the bucket index using the Bytes2Uint16Buckets function and retrieves the value from the corresponding bucket.
//
// Params:
//   - hash: The hash to retrieve from the map.
//
// Returns:
//   - uint64: The value associated with the hash, or 0 if the hash does not exist.
//   - bool: True if the hash was found in the map, false otherwise.
func (g *SplitSwissMapUint64) Get(hash chainhash.Hash) (uint64, bool) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Get(hash)
}

// Length returns the current number of hashes in the map.
// It iterates over all buckets and sums their lengths to get the total count.
//
// Returns:
//   - int: The number of hashes currently stored in the map.
func (g *SplitSwissMapUint64) Length() int {
	length := 0
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		length += g.m[i].length
	}

	return length
}

// Delete removes a hash from the map.
// It calculates the bucket index using the Bytes2Uint16Buckets function and checks the corresponding bucket for the hash.
// If the hash does not exist, it returns an error.
//
// Params:
//   - hash: The hash to remove from the map.
//
// Returns:
//   - error: An error if the hash does not exist in the map or if the bucket does not exist, nil otherwise.
func (g *SplitSwissMapUint64) Delete(hash chainhash.Hash) error {
	bucket := Bytes2Uint16Buckets(hash, g.nrOfBuckets)

	if _, ok := g.m[bucket]; !ok {
		return fmt.Errorf("bucket %d does not exist", bucket)
	}

	if !g.m[bucket].Exists(hash) {
		return fmt.Errorf("hash %s does not exist in bucket %d", hash, bucket)
	}

	return g.m[bucket].Delete(hash)
}

// SplitSwissLockFreeMapUint64 is a map that splits the data into multiple buckets to reduce contention.
// It uses SwissLockFreeMapUint64 for each bucket to store the hashes and their associated uint64 values.
type SplitSwissLockFreeMapUint64 struct {
	m           map[uint64]*SwissLockFreeMapUint64
	nrOfBuckets uint64
}

// NewSplitSwissLockFreeMapUint64 creates a new SplitSwissLockFreeMapUint64 with the specified initial length.
// The length is used to preallocate the size of each bucket.
// It divides the length by the number of buckets to determine the size of each bucket.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *SplitSwissLockFreeMapUint64: A pointer to the newly created SplitSwissLockFreeMapUint64 instance.
func NewSplitSwissLockFreeMapUint64(length int) *SplitSwissLockFreeMapUint64 {
	m := &SplitSwissLockFreeMapUint64{
		m:           make(map[uint64]*SwissLockFreeMapUint64, 256),
		nrOfBuckets: 1024,
	}

	for i := uint64(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewSwissLockFreeMapUint64(length / int(m.nrOfBuckets)) // nolint:gosec
	}

	return m
}

// Exists checks if the given hash exists in the map.
// It calculates the bucket index using the modulo operation and checks the corresponding bucket.
//
// Params:
//   - hash: The hash to check for existence in the map.
//
// Returns:
//   - bool: True if the hash exists in the map, false otherwise.
//
// Note: This method does not lock the map, so it is not suitable for concurrent access.
func (g *SplitSwissLockFreeMapUint64) Exists(hash uint64) bool {
	return g.m[hash%g.nrOfBuckets].Exists(hash)
}

// Map returns the underlying map of buckets used by SplitSwissLockFreeMapUint64.
// It provides access to the map for operations that do not require locking.
//
// Returns:
//   - map[uint64]*SwissLockFreeMapUint64: A map where the keys are bucket indices and the values are pointers to SwissLockFreeMapUint64 instances.
//
// Note: This method does not lock the map, so it is not suitable for concurrent access.
func (g *SplitSwissLockFreeMapUint64) Map() map[uint64]*SwissLockFreeMapUint64 {
	return g.m
}

// Put adds a new hash with an associated uint64 value to the map.
// It calculates the bucket index using the modulo operation and adds the hash to the corresponding bucket.
// It checks if the hash already exists in the bucket and returns an error if it does.
//
// Params:
//   - hash: The hash to add to the map.
//   - n: The uint64 value to associate with the hash.
//
// Returns:
//   - error: An error if the hash already exists in the map, nil otherwise.
//
// Note: This method does not lock the map, so it is not suitable for concurrent access.
func (g *SplitSwissLockFreeMapUint64) Put(hash, n uint64) error {
	return g.m[hash%g.nrOfBuckets].Put(hash, n)
}

// Get retrieves the uint64 value associated with the given hash from the map.
// It calculates the bucket index using the modulo operation and retrieves the value from the corresponding bucket.
//
// Params:
//   - hash: The hash to retrieve from the map.
//
// Returns:
//   - uint64: The value associated with the hash, or 0 if the hash does not exist.
//   - bool: True if the hash was found in the map, false otherwise.
//
// Note: This method does not lock the map, so it is not suitable for concurrent access.
func (g *SplitSwissLockFreeMapUint64) Get(hash uint64) (uint64, bool) {
	return g.m[hash%g.nrOfBuckets].Get(hash)
}

// Keys returns a slice of all hashes currently stored in the map.
// It iterates over all buckets and collects the keys from each bucket.
// The order of keys is not guaranteed.
//
// Returns:
//   - []chainhash.Hash: A slice containing all the hashes in the map.
//
// Note: This method does not lock the map, so it is not suitable for concurrent access.
func (g *SplitSwissMapUint64) Keys() []chainhash.Hash {
	keys := make([]chainhash.Hash, 0, g.Length())

	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		keys = append(keys, g.m[i].Keys()...)
	}

	return keys
}

// Length returns the current number of hashes in the map.
// It iterates over all buckets and sums their lengths to get the total count.
// It uses atomic operations to ensure thread safety.
//
// Returns:
//   - int: The number of hashes currently stored in the map.
func (g *SplitSwissLockFreeMapUint64) Length() int {
	length := 0
	for i := uint64(0); i <= g.nrOfBuckets; i++ {
		length += int(g.m[i].length.Load())
	}

	return length
}

// Bytes2Uint16Buckets converts the first two bytes of a chainhash.Hash to a uint16 value
// and returns the result modulo the specified value.
// This function is used to determine the bucket index for a given hash in a split map.
//
// Params:
//   - b: The chainhash.Hash to convert.
//   - mod: The value to use for the modulo operation.
//
// Returns:
//   - uint16: The resulting value after conversion and modulo operation.
func Bytes2Uint16Buckets(b chainhash.Hash, mod uint16) uint16 {
	return (uint16(b[0])<<8 | uint16(b[1])) % mod
}

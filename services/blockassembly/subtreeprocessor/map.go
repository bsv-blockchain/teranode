package subtreeprocessor

import (
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/dolthub/swiss"
)

type SplitSwissMap struct {
	m           map[uint16]*swiss.Map[chainhash.Hash, struct{}]
	mu          map[uint16]*sync.RWMutex
	nrOfBuckets uint16
}

// NewSplitSwissMap creates a new SplitSwissMap with the specified number of buckets and total length.
// The length is divided equally among the buckets.
// This map is safe for concurrent writes, but does not lock when reading.
//
// Parameters:
//   - nrOfBuckets: The number of buckets to split the map into.
//   - length: The total expected length of the map.
//
// Returns:
//   - A pointer to the newly created SplitSwissMap.
func NewSplitSwissMap(nrOfBuckets uint16, length int) *SplitSwissMap {
	m := make(map[uint16]*swiss.Map[chainhash.Hash, struct{}], nrOfBuckets)
	mu := make(map[uint16]*sync.RWMutex, nrOfBuckets)
	for i := uint16(0); i < nrOfBuckets; i++ {
		m[i] = swiss.NewMap[chainhash.Hash, struct{}](uint32(length / int(nrOfBuckets)))
		mu[i] = &sync.RWMutex{}
	}

	return &SplitSwissMap{
		m:           m,
		mu:          mu,
		nrOfBuckets: nrOfBuckets,
	}
}

func (s *SplitSwissMap) Exists(hash chainhash.Hash) bool {
	_, ok := s.m[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)].Get(hash)
	return ok
}

func (s *SplitSwissMap) Length() int {
	var length int

	for bucket := uint16(0); bucket < s.nrOfBuckets; bucket++ {
		s.mu[bucket].Lock()
		length += s.m[bucket].Count()
		s.mu[bucket].Unlock()
	}

	return length
}

func (s *SplitSwissMap) Buckets() uint16 {
	return s.nrOfBuckets
}

func (s *SplitSwissMap) Put(hash chainhash.Hash) error {
	bucket := txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)

	s.mu[bucket].Lock()
	defer s.mu[bucket].Unlock()

	s.m[bucket].Put(hash, struct{}{})

	return nil
}

func (s *SplitSwissMap) PutMultiBucket(bucket uint16, hashes []chainhash.Hash) error {
	s.mu[bucket].Lock()
	defer s.mu[bucket].Unlock()

	for _, hash := range hashes {
		s.m[bucket].Put(hash, struct{}{})
	}

	return nil
}

func (s *SplitSwissMap) Iter(f func(hash chainhash.Hash, v struct{}) bool) {
	for _, swissMap := range s.m {
		swissMap.Iter(f)
	}
}

type SplitTxInpointsMap struct {
	m           map[uint16]*txmap.SyncedMap[chainhash.Hash, *subtreepkg.TxInpoints]
	nrOfBuckets uint16
}

func NewSplitTxInpointsMap(nrOfBuckets uint16) *SplitTxInpointsMap {
	m := make(map[uint16]*txmap.SyncedMap[chainhash.Hash, *subtreepkg.TxInpoints], nrOfBuckets)
	for i := uint16(0); i < nrOfBuckets; i++ {
		m[i] = txmap.NewSyncedMap[chainhash.Hash, *subtreepkg.TxInpoints]()
	}

	return &SplitTxInpointsMap{
		m:           m,
		nrOfBuckets: nrOfBuckets,
	}
}

func (s *SplitTxInpointsMap) Delete(hash chainhash.Hash) bool {
	return s.m[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)].Delete(hash)
}

func (s *SplitTxInpointsMap) Exists(hash chainhash.Hash) bool {
	return s.m[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)].Exists(hash)
}

func (s *SplitTxInpointsMap) Get(hash chainhash.Hash) (*subtreepkg.TxInpoints, bool) {
	return s.m[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)].Get(hash)
}

func (s *SplitTxInpointsMap) Length() int {
	length := 0

	for _, syncedMap := range s.m {
		length += syncedMap.Length()
	}

	return length
}

func (s *SplitTxInpointsMap) Set(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) {
	s.m[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)].Set(hash, inpoints)
}

func (s *SplitTxInpointsMap) SetIfNotExists(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) (*subtreepkg.TxInpoints, bool) {
	return s.m[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)].SetIfNotExists(hash, inpoints)
}

func (s *SplitTxInpointsMap) Clear() {
	for _, syncedMap := range s.m {
		syncedMap.Clear()
	}
}

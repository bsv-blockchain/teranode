package model

import (
	"sync"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
)

type SplitSyncedParentMap struct {
	m           map[uint16]*sync.Map
	nrOfBuckets uint16
}

func NewSplitSyncedParentMap(nrOfBuckets uint16) *SplitSyncedParentMap {
	m := make(map[uint16]*sync.Map, nrOfBuckets)
	for i := uint16(0); i < nrOfBuckets; i++ {
		m[i] = &sync.Map{}
	}

	return &SplitSyncedParentMap{
		m:           m,
		nrOfBuckets: nrOfBuckets,
	}
}

func (s *SplitSyncedParentMap) SetIfNotExists(inpoint subtreepkg.Inpoint) bool {
	_, loaded := s.m[txmap.Bytes2Uint16Buckets(inpoint.Hash, s.nrOfBuckets)].LoadOrStore(inpoint, struct{}{})

	return !loaded
}

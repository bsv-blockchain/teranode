package main

import (
	"hash/fnv"

	"github.com/dolthub/swiss"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

const (
	// Number of shards
	numShards = 1
	// Size of each shard
	shardSize = 1024 * 1024 // 10GB
	// Number of hash functions
	// numHashFuncs = 5
)

type MemoryFilter struct {
	memMap *swiss.Map[chainhash.Hash, struct{}]
	size   uint
}

type ShardedMemoryFilter struct {
	shards [numShards]*MemoryFilter
}

func NewMemoryFilter(m uint) *MemoryFilter {
	return &MemoryFilter{
		memMap: swiss.NewMap[chainhash.Hash, struct{}](uint32(m)),
		size:   m,
	}
}

func NewShardedMemoryFilter() *ShardedMemoryFilter {
	sbf := &ShardedMemoryFilter{}
	for i := range sbf.shards {
		sbf.shards[i] = NewMemoryFilter(shardSize)
	}
	return sbf
}

func (b *MemoryFilter) Add(data []byte) {
	key, _ := chainhash.NewHash(data)
	b.memMap.Put(*key, struct{}{})
}

func (b *MemoryFilter) Test(data []byte) bool {
	key, _ := chainhash.NewHash(data)
	_, ok := b.memMap.Get(*key)

	return ok
}

func hashValue1(data []byte) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write(data)
	return hash.Sum64()
}

func (s *ShardedMemoryFilter) Add(data []byte) {
	hash := hashValue1(data)
	i := hash % uint64(numShards)
	s.shards[i].Add(data)
}

func (s *ShardedMemoryFilter) Test(data []byte) bool {
	hash := hashValue1(data)
	i := hash % uint64(numShards)
	return s.shards[i].Test(data)
}

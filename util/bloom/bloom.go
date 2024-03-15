package bloom

import (
	"hash/fnv"
	"math/big"

	"github.com/spaolacci/murmur3"
)

const (
	// Number of shards
	numShards = 1
	// Size of each shard
	shardSize = 10_000_000_000 // 10GB
	// Number of hash functions
	numHashFuncs = 2
)

// RESULTS

// 1 hash function

// num shards 1, shard size 20_000_000_000
// Number of txids: 1_000_000
// Time to add txids: 12.81812975s
// Time to test txids: 320.557917ms
// Number of false positives: 6

// num shards 1, shard size 1_000_000_000
// Number of txids: 1_000_000
// Time to add txids: 204.39475ms
// Time to test txids: 140.310583ms
// Number of false positives: 82

// num shards 1, shard size 10_000_000_000
// Number of txids: 100_000_000
// Time to add txids: 9.055712208s
// Time to test txids: 26.02134875s
// Number of false positives: 89739

// 5 hash functions

// num shards 1, shard size 10_000_000_000
// Number of txids: 100_000_000
// Time to add txids: 14.188045167s
// Time to test txids: 29.697035959s
// Number of false positives: 0

// 10 hash functions

// num shards 1, shard size 10_000_000_000
//  Number of txids: 100_000_000
// Time to add txids: 16.338301209s
// Time to test txids: 27.772361833s
// Number of false positives: 0

// num shards 2, shard size 10_000_000_000
// Number of txids: 100_000_000
// Time to add txids: 23.072574291s
// Time to test txids: 36.129520042s
// Number of false positives: 0

type BloomFilter struct {
	bitset *big.Int
	size   uint
}

type ShardedBloomFilter struct {
	shards [numShards]*BloomFilter
}

func NewBloomFilter(m uint) *BloomFilter {
	return &BloomFilter{
		bitset: big.NewInt(0),
		size:   m,
	}
}

func NewShardedBloomFilter() *ShardedBloomFilter {
	sbf := &ShardedBloomFilter{}
	for i := range sbf.shards {
		sbf.shards[i] = NewBloomFilter(shardSize)
	}
	return sbf
}

func hashValue1(data []byte) uint64 {
	hash := fnv.New64a()
	hash.Write(data)
	return hash.Sum64()
}

func hashValue2(data []byte) uint64 {
	hash := murmur3.New64()
	hash.Write(data)
	return hash.Sum64()
}

func (b *BloomFilter) Add(data []byte) {
	hash1 := hashValue1(data)
	hash2 := hashValue2(data)

	for i := 0; i < numHashFuncs; i++ {
		pos := (hash1 + uint64(i)*hash2) % uint64(b.size)
		b.bitset.SetBit(b.bitset, int(pos), 1)
	}
}

func (b *BloomFilter) Test(data []byte) bool {
	hash1 := hashValue1(data)
	hash2 := hashValue2(data)

	for i := 0; i < numHashFuncs; i++ {
		pos := (hash1 + uint64(i)*hash2) % uint64(b.size)
		if b.bitset.Bit(int(pos)) != 1 {
			return false
		}
	}

	return true
}

func (s *ShardedBloomFilter) Add(data []byte) {
	hash := hashValue1(data)
	i := hash % uint64(numShards)
	s.shards[i].Add(data)
}

func (s *ShardedBloomFilter) Test(data []byte) bool {
	hash := hashValue1(data)
	i := hash % uint64(numShards)
	return s.shards[i].Test(data)
}

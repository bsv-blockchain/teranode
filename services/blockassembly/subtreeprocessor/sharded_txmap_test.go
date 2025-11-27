package subtreeprocessor

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmappkg "github.com/bsv-blockchain/go-tx-map"
	"github.com/stretchr/testify/require"
)

func TestShardedTxMap_BasicOperations(t *testing.T) {
	stm := NewShardedTxMap()

	// Test initial state
	require.Equal(t, 0, stm.Length())

	// Generate test hashes
	hash1 := chainhash.HashH([]byte("tx1"))
	hash2 := chainhash.HashH([]byte("tx2"))
	hash3 := chainhash.HashH([]byte("tx3"))

	inpoints1 := subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{hash2}}
	inpoints2 := subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{hash3}}

	// Test SetIfNotExists - new key
	val, wasSet := stm.SetIfNotExists(hash1, inpoints1)
	require.True(t, wasSet)
	require.Equal(t, inpoints1, val)
	require.Equal(t, 1, stm.Length())

	// Test SetIfNotExists - existing key
	val, wasSet = stm.SetIfNotExists(hash1, inpoints2)
	require.False(t, wasSet)
	require.Equal(t, inpoints1, val) // Should return original value

	// Test Get - existing key
	val, found := stm.Get(hash1)
	require.True(t, found)
	require.Equal(t, inpoints1, val)

	// Test Get - non-existing key
	_, found = stm.Get(hash2)
	require.False(t, found)

	// Add more entries
	stm.SetIfNotExists(hash2, inpoints2)
	require.Equal(t, 2, stm.Length())

	// Test Delete
	deleted := stm.Delete(hash1)
	require.True(t, deleted)
	require.Equal(t, 1, stm.Length())

	_, found = stm.Get(hash1)
	require.False(t, found)

	// Test Clear
	stm.SetIfNotExists(hash1, inpoints1)
	stm.SetIfNotExists(hash3, inpoints1)
	require.Equal(t, 3, stm.Length())

	stm.Clear()
	require.Equal(t, 0, stm.Length())
}

func TestShardedTxMap_ShardDistribution(t *testing.T) {
	stm := NewShardedTxMap()

	// Add many entries and verify they're distributed across shards
	numEntries := 10000
	hashes := make([]chainhash.Hash, numEntries)
	for i := 0; i < numEntries; i++ {
		hashes[i] = chainhash.HashH([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
		stm.SetIfNotExists(hashes[i], subtreepkg.TxInpoints{})
	}

	require.Equal(t, numEntries, stm.Length())

	// Check that entries are distributed across multiple shards
	nonEmptyShards := 0
	for _, shard := range stm.shards {
		if shard.Length() > 0 {
			nonEmptyShards++
		}
	}

	// With 10k entries and 1024 shards, we expect entries in most shards
	// (approximately 10 entries per shard on average)
	require.Greater(t, nonEmptyShards, 500, "entries should be distributed across many shards")
}

func TestShardedTxMap_Keys(t *testing.T) {
	stm := NewShardedTxMap()

	hashes := make([]chainhash.Hash, 100)
	for i := 0; i < 100; i++ {
		hashes[i] = chainhash.HashH([]byte{byte(i)})
		stm.SetIfNotExists(hashes[i], subtreepkg.TxInpoints{})
	}

	keys := stm.Keys()
	require.Len(t, keys, 100)

	// Verify all original hashes are in the keys
	keySet := make(map[chainhash.Hash]bool)
	for _, k := range keys {
		keySet[k] = true
	}
	for _, h := range hashes {
		require.True(t, keySet[h], "hash should be in keys")
	}
}

func TestShardedTxMap_Iterate(t *testing.T) {
	stm := NewShardedTxMap()

	// Add entries
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		hash := chainhash.HashH([]byte{byte(i)})
		stm.SetIfNotExists(hash, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{hash}})
	}

	// Count entries via iteration
	count := 0
	stm.Iterate(func(key chainhash.Hash, value subtreepkg.TxInpoints) bool {
		count++
		return true
	})
	require.Equal(t, numEntries, count)

	// Test early termination
	count = 0
	stm.Iterate(func(key chainhash.Hash, value subtreepkg.TxInpoints) bool {
		count++
		return count < 50
	})
	require.Equal(t, 50, count)
}

func TestShardedTxMap_ConcurrentAccess(t *testing.T) {
	stm := NewShardedTxMap()

	numGoroutines := 100
	entriesPerGoroutine := 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < entriesPerGoroutine; i++ {
				// Create unique hash per goroutine+index
				hash := chainhash.HashH([]byte{byte(goroutineID), byte(goroutineID >> 8), byte(i), byte(i >> 8)})
				stm.SetIfNotExists(hash, subtreepkg.TxInpoints{})
			}
		}(g)
	}

	wg.Wait()

	// All entries should be present
	require.Equal(t, numGoroutines*entriesPerGoroutine, stm.Length())
}

// BenchmarkShardedTxMapSetIfNotExists benchmarks the sharded map implementation
func BenchmarkShardedTxMapSetIfNotExists(b *testing.B) {
	stm := NewShardedTxMap()

	hashes := make([]chainhash.Hash, b.N)
	for i := 0; i < b.N; i++ {
		hashes[i] = chainhash.HashH([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
	}
	emptyInpoints := subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		stm.SetIfNotExists(hashes[i], emptyInpoints)
	}
}

// BenchmarkNonShardedTxMapSetIfNotExists benchmarks the original non-sharded implementation
func BenchmarkNonShardedTxMapSetIfNotExists(b *testing.B) {
	txMap := txmappkg.NewSyncedMap[chainhash.Hash, subtreepkg.TxInpoints]()

	hashes := make([]chainhash.Hash, b.N)
	for i := 0; i < b.N; i++ {
		hashes[i] = chainhash.HashH([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
	}
	emptyInpoints := subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txMap.SetIfNotExists(hashes[i], emptyInpoints)
	}
}

// BenchmarkShardedVsNonShardedParallel compares performance under parallel load
func BenchmarkShardedVsNonShardedParallel(b *testing.B) {
	b.Run("Sharded", func(b *testing.B) {
		stm := NewShardedTxMap()
		emptyInpoints := subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}}

		b.ResetTimer()
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				hash := chainhash.HashH([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
				stm.SetIfNotExists(hash, emptyInpoints)
				i++
			}
		})
	})

	b.Run("NonSharded", func(b *testing.B) {
		txMap := txmappkg.NewSyncedMap[chainhash.Hash, subtreepkg.TxInpoints]()
		emptyInpoints := subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}}

		b.ResetTimer()
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				hash := chainhash.HashH([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
				txMap.SetIfNotExists(hash, emptyInpoints)
				i++
			}
		})
	})
}

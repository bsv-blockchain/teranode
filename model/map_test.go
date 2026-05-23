package model

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

func TestNewSplitSyncedParentMap(t *testing.T) {
	t.Run("creates map with correct number of buckets", func(t *testing.T) {
		buckets := uint16(16)
		m := NewSplitSyncedParentMap(buckets)

		require.NotNil(t, m)
		require.Equal(t, buckets, m.nrOfBuckets)
		require.Len(t, m.buckets, int(buckets))

		// Verify all buckets are initialized
		for i := uint16(0); i < buckets; i++ {
			require.NotNil(t, m.buckets[i].m)
		}
	})

	t.Run("creates map with single bucket", func(t *testing.T) {
		m := NewSplitSyncedParentMap(1)

		require.NotNil(t, m)
		require.Equal(t, uint16(1), m.nrOfBuckets)
		require.Len(t, m.buckets, 1)
	})

	t.Run("creates map with 256 buckets", func(t *testing.T) {
		m := NewSplitSyncedParentMap(256)

		require.NotNil(t, m)
		require.Equal(t, uint16(256), m.nrOfBuckets)
		require.Len(t, m.buckets, 256)
	})

	t.Run("creates map with pre-allocated capacity", func(t *testing.T) {
		m := NewSplitSyncedParentMap(16, 1600)

		require.NotNil(t, m)
		require.Equal(t, uint16(16), m.nrOfBuckets)
		require.Len(t, m.buckets, 16)
	})
}

func TestSplitSyncedParentMap_SetIfNotExists(t *testing.T) {
	t.Run("returns true for new inpoint", func(t *testing.T) {
		m := NewSplitSyncedParentMap(16)

		hash := chainhash.HashH([]byte("test-hash-1"))
		inpoint := subtreepkg.Inpoint{
			Hash:  hash,
			Index: 0,
		}

		result := m.SetIfNotExists(inpoint)
		require.True(t, result, "SetIfNotExists should return true for new inpoint")
	})

	t.Run("returns false for existing inpoint", func(t *testing.T) {
		m := NewSplitSyncedParentMap(16)

		hash := chainhash.HashH([]byte("test-hash-2"))
		inpoint := subtreepkg.Inpoint{
			Hash:  hash,
			Index: 0,
		}

		// First insertion should succeed
		result1 := m.SetIfNotExists(inpoint)
		require.True(t, result1, "First SetIfNotExists should return true")

		// Second insertion of same inpoint should fail
		result2 := m.SetIfNotExists(inpoint)
		require.False(t, result2, "Second SetIfNotExists should return false for duplicate")
	})

	t.Run("different index same hash are different inpoints", func(t *testing.T) {
		m := NewSplitSyncedParentMap(16)

		hash := chainhash.HashH([]byte("test-hash-3"))
		inpoint1 := subtreepkg.Inpoint{
			Hash:  hash,
			Index: 0,
		}
		inpoint2 := subtreepkg.Inpoint{
			Hash:  hash,
			Index: 1,
		}

		result1 := m.SetIfNotExists(inpoint1)
		require.True(t, result1, "SetIfNotExists should return true for index 0")

		result2 := m.SetIfNotExists(inpoint2)
		require.True(t, result2, "SetIfNotExists should return true for index 1 (different inpoint)")
	})

	t.Run("different hash same index are different inpoints", func(t *testing.T) {
		m := NewSplitSyncedParentMap(16)

		hash1 := chainhash.HashH([]byte("test-hash-4a"))
		hash2 := chainhash.HashH([]byte("test-hash-4b"))
		inpoint1 := subtreepkg.Inpoint{
			Hash:  hash1,
			Index: 0,
		}
		inpoint2 := subtreepkg.Inpoint{
			Hash:  hash2,
			Index: 0,
		}

		result1 := m.SetIfNotExists(inpoint1)
		require.True(t, result1, "SetIfNotExists should return true for hash1")

		result2 := m.SetIfNotExists(inpoint2)
		require.True(t, result2, "SetIfNotExists should return true for hash2 (different inpoint)")
	})

	t.Run("handles many unique inpoints", func(t *testing.T) {
		m := NewSplitSyncedParentMap(256)

		const numInpoints = 10000
		for i := 0; i < numInpoints; i++ {
			hash := chainhash.HashH([]byte("inpoint-" + string(rune(i))))
			inpoint := subtreepkg.Inpoint{
				Hash:  hash,
				Index: uint32(i),
			}

			result := m.SetIfNotExists(inpoint)
			require.True(t, result, "SetIfNotExists should return true for inpoint %d", i)
		}
	})
}

func TestSplitSyncedParentMap_Concurrent(t *testing.T) {
	t.Run("handles concurrent writes of unique inpoints", func(t *testing.T) {
		m := NewSplitSyncedParentMap(256)

		const numGoroutines = 100
		const inpointsPerGoroutine = 1000

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for g := 0; g < numGoroutines; g++ {
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < inpointsPerGoroutine; i++ {
					hash := chainhash.HashH([]byte("goroutine-" + string(rune(goroutineID)) + "-inpoint-" + string(rune(i))))
					inpoint := subtreepkg.Inpoint{
						Hash:  hash,
						Index: uint32(i),
					}

					result := m.SetIfNotExists(inpoint)
					require.True(t, result, "SetIfNotExists should return true for unique inpoint")
				}
			}(g)
		}

		wg.Wait()
	})

	t.Run("detects duplicates under concurrent access", func(t *testing.T) {
		m := NewSplitSyncedParentMap(256)

		const numGoroutines = 100

		// Create a single inpoint that all goroutines will try to insert
		hash := chainhash.HashH([]byte("shared-inpoint"))
		sharedInpoint := subtreepkg.Inpoint{
			Hash:  hash,
			Index: 0,
		}

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		successCount := make(chan bool, numGoroutines)

		for g := 0; g < numGoroutines; g++ {
			go func() {
				defer wg.Done()
				result := m.SetIfNotExists(sharedInpoint)
				successCount <- result
			}()
		}

		wg.Wait()
		close(successCount)

		// Count successes - exactly one goroutine should succeed
		successes := 0
		for result := range successCount {
			if result {
				successes++
			}
		}

		require.Equal(t, 1, successes, "Exactly one goroutine should succeed in setting the shared inpoint")
	})

	t.Run("handles mixed unique and duplicate inpoints concurrently", func(t *testing.T) {
		m := NewSplitSyncedParentMap(256)

		const numGoroutines = 50
		const inpointsPerGoroutine = 100

		// Pre-generate some shared hashes that will be duplicates
		sharedHashes := make([]chainhash.Hash, 10)
		for i := 0; i < 10; i++ {
			sharedHashes[i] = chainhash.HashH([]byte("shared-" + string(rune(i))))
		}

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for g := 0; g < numGoroutines; g++ {
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < inpointsPerGoroutine; i++ {
					var inpoint subtreepkg.Inpoint
					if i < 10 {
						// Use shared hash - will be duplicate across goroutines
						inpoint = subtreepkg.Inpoint{
							Hash:  sharedHashes[i],
							Index: 0,
						}
					} else {
						// Use unique hash
						hash := chainhash.HashH([]byte("unique-" + string(rune(goroutineID)) + "-" + string(rune(i))))
						inpoint = subtreepkg.Inpoint{
							Hash:  hash,
							Index: uint32(i),
						}
					}

					// Just call - don't check result for shared hashes as it's race-dependent
					m.SetIfNotExists(inpoint)
				}
			}(g)
		}

		wg.Wait()
	})
}

func TestSplitSyncedParentMap_BucketDistribution(t *testing.T) {
	t.Run("distributes inpoints across buckets", func(t *testing.T) {
		const numBuckets = uint16(16)
		m := NewSplitSyncedParentMap(numBuckets)

		const numInpoints = 1000
		for i := 0; i < numInpoints; i++ {
			hash := chainhash.HashH([]byte("distribution-test-" + string(rune(i))))
			inpoint := subtreepkg.Inpoint{
				Hash:  hash,
				Index: uint32(i),
			}
			m.SetIfNotExists(inpoint)
		}

		// Check that multiple buckets have entries (not all in one bucket)
		bucketsWithEntries := 0
		for i := uint16(0); i < numBuckets; i++ {
			if m.buckets[i].m.Count() > 0 {
				bucketsWithEntries++
			}
		}

		// With 1000 random hashes distributed across 16 buckets,
		// we should have entries in most buckets
		require.Greater(t, bucketsWithEntries, 10, "Inpoints should be distributed across multiple buckets")
	})
}

func BenchmarkSplitSyncedParentMap_SetIfNotExists(b *testing.B) {
	// Pre-generate inpoints
	inpoints := make([]subtreepkg.Inpoint, b.N)
	for i := 0; i < b.N; i++ {
		hash := chainhash.HashH([]byte("bench-" + string(rune(i))))
		inpoints[i] = subtreepkg.Inpoint{
			Hash:  hash,
			Index: uint32(i),
		}
	}

	b.Run("256_buckets", func(b *testing.B) {
		m := NewSplitSyncedParentMap(256)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.SetIfNotExists(inpoints[i%len(inpoints)])
		}
	})

	b.Run("16_buckets", func(b *testing.B) {
		m := NewSplitSyncedParentMap(16)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.SetIfNotExists(inpoints[i%len(inpoints)])
		}
	})

	b.Run("1_bucket", func(b *testing.B) {
		m := NewSplitSyncedParentMap(1)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.SetIfNotExists(inpoints[i%len(inpoints)])
		}
	})
}

func BenchmarkSplitSyncedParentMap_ConcurrentSetIfNotExists(b *testing.B) {
	// Pre-generate inpoints
	const numInpoints = 100000
	inpoints := make([]subtreepkg.Inpoint, numInpoints)
	for i := 0; i < numInpoints; i++ {
		hash := chainhash.HashH([]byte("bench-concurrent-" + string(rune(i))))
		inpoints[i] = subtreepkg.Inpoint{
			Hash:  hash,
			Index: uint32(i),
		}
	}

	b.Run("256_buckets_parallel", func(b *testing.B) {
		m := NewSplitSyncedParentMap(256)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.SetIfNotExists(inpoints[i%numInpoints])
				i++
			}
		})
	})

	b.Run("16_buckets_parallel", func(b *testing.B) {
		m := NewSplitSyncedParentMap(16)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.SetIfNotExists(inpoints[i%numInpoints])
				i++
			}
		})
	})

	b.Run("1_bucket_parallel", func(b *testing.B) {
		m := NewSplitSyncedParentMap(1)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.SetIfNotExists(inpoints[i%numInpoints])
				i++
			}
		})
	})
}

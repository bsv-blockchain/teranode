package pruner

import (
	"crypto/rand"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// makeHash fills a chainhash.Hash with deterministic bytes derived from b.
// We avoid putting too little entropy in the hash because the cuckoo filter
// derives its fingerprint+index from the bytes — short-distance hashes
// collide more often and inflate the false-positive rate in tests.
func makeHash(b byte) chainhash.Hash {
	var h chainhash.Hash
	for i := 0; i < len(h); i++ {
		h[i] = b ^ byte(i*131)
	}
	return h
}

// randomHash returns a chainhash.Hash filled with cryptographically random
// bytes — useful for tests that need to exercise the filter at scale without
// pathological FP behaviour.
func randomHash(t *testing.T) chainhash.Hash {
	t.Helper()
	var h chainhash.Hash
	_, err := rand.Read(h[:])
	require.NoError(t, err)
	return h
}

func TestPrunedTxSet_AddAndContains(t *testing.T) {
	set := NewPrunedTxSet(16, 4096)

	h1 := makeHash(0x01)
	h2 := makeHash(0x02)

	set.Add(h1)
	set.Add(h2)

	require.True(t, set.Contains(h1))
	require.True(t, set.Contains(h2))
	// We do NOT assert Contains(h3)==false: the cuckoo filter has a small
	// chance of false positives. Correctness of "not added" lookups is best
	// validated statistically in TestPrunedTxSet_FalsePositiveRate.
}

func TestPrunedTxSet_CheckAndRemove(t *testing.T) {
	set := NewPrunedTxSet(16, 4096)

	h1 := makeHash(0x01)
	h2 := makeHash(0x02)

	set.Add(h1)
	set.Add(h2)

	require.True(t, set.CheckAndRemove(h1))
	// h2 still present
	require.True(t, set.Contains(h2))
}

func TestPrunedTxSet_Len_AfterRemove(t *testing.T) {
	set := NewPrunedTxSet(16, 4096)

	require.Equal(t, 0, set.Len())

	set.Add(makeHash(0x01))
	set.Add(makeHash(0x02))
	require.Equal(t, 2, set.Len())

	set.CheckAndRemove(makeHash(0x01))
	require.Equal(t, 1, set.Len())
}

func TestPrunedTxSet_ConcurrentAccess(t *testing.T) {
	set := NewPrunedTxSet(256, 1_048_576)
	const numGoroutines = 100
	const opsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	for g := 0; g < numGoroutines; g++ {
		go func(base int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				var h chainhash.Hash
				val := uint16(base*opsPerGoroutine + i)
				h[0] = byte(val >> 8)
				h[1] = byte(val)
				set.Add(h)
			}
		}(g)
	}

	for g := 0; g < numGoroutines; g++ {
		go func(base int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				var h chainhash.Hash
				val := uint16(base*opsPerGoroutine + i)
				h[0] = byte(val >> 8)
				h[1] = byte(val)
				set.CheckAndRemove(h)
			}
		}(g)
	}

	wg.Wait()
	// No assertion on final count — just verifying no data races or panics.
}

func TestPrunedTxSet_ShardDistribution(t *testing.T) {
	set := NewPrunedTxSet(256, 1_048_576)

	// Add hashes with different first bytes so they distribute across shards.
	for i := 0; i < 256; i++ {
		set.Add(makeHash(byte(i)))
	}

	// Every added hash must be findable.
	for i := 0; i < 256; i++ {
		require.True(t, set.Contains(makeHash(byte(i))))
	}

	// Removing all should drop the Len to roughly zero (some collisions may
	// leave residual fingerprints; allow a small slack).
	for i := 0; i < 256; i++ {
		set.CheckAndRemove(makeHash(byte(i)))
	}
	require.LessOrEqual(t, set.Len(), 8)
}

func TestPrunedTxSet_SimulateChainPruning(t *testing.T) {
	// Tight chain: A -> B -> C -> D, all pruned in the same session.
	// Reader Adds all four TXIDs before processor starts; processor's
	// CheckAndRemove for each parent should find it.
	set := NewPrunedTxSet(16, 4096)

	txA := makeHash(0x0A)
	txB := makeHash(0x0B)
	txC := makeHash(0x0C)
	txD := makeHash(0x0D)

	set.Add(txA)
	set.Add(txB)
	set.Add(txC)
	set.Add(txD)

	require.True(t, set.CheckAndRemove(txA), "parent A should be found")
	require.True(t, set.CheckAndRemove(txB), "parent B should be found")
	require.True(t, set.CheckAndRemove(txC), "parent C should be found")

	// D has no child in this session — should still be present.
	require.True(t, set.Contains(txD))
}

func TestPrunedTxSet_RotatesUnderLoad(t *testing.T) {
	// With the two-generation design, sustained Adds beyond the per-
	// generation capacity should rotate generations rather than fail.
	// We verify Rotations() climbs and InsertFailures() stays at zero.
	set := NewPrunedTxSet(4, 1024)

	for i := 0; i < 10000; i++ {
		set.Add(randomHash(t))
	}

	require.Positive(t, set.Rotations(),
		"expected at least one generation rotation under sustained load")
	require.Zero(t, set.InsertFailures(),
		"two-generation design should not surface insert failures in normal operation")
}

func TestPrunedTxSet_DefaultCapacity(t *testing.T) {
	// maxEntries<=0 falls back to defaultPrunedTxSetCapacity. We don't
	// actually construct with maxEntries=0 here — doing so would allocate
	// ~1 GiB of cuckoo memory and OOM CI. Instead, sanity-check that the
	// default constant is the expected order of magnitude.
	require.Equal(t, 2_000_000_000, defaultPrunedTxSetCapacity,
		"default capacity should be 2B entries (~2 GiB at ~1 B/entry)")
}

func TestPrunedTxSet_FalsePositiveRate(t *testing.T) {
	// Verify the FP rate is within the documented cuckoo bound. With 8-bit
	// fingerprints and 4-slot buckets the theoretical FP rate is ~3.1%.
	// We allow up to 6% to keep the test stable.
	set := NewPrunedTxSet(256, 10_000_000)

	const inserted = 100_000
	addedHashes := make([]chainhash.Hash, inserted)
	for i := 0; i < inserted; i++ {
		addedHashes[i] = randomHash(t)
		set.Add(addedHashes[i])
	}

	// All inserted hashes must be present.
	for _, h := range addedHashes {
		require.True(t, set.Contains(h))
	}

	// Random hashes that were not added should mostly miss.
	const trials = 100_000
	falsePositives := 0
	for i := 0; i < trials; i++ {
		h := randomHash(t)
		if set.Contains(h) {
			falsePositives++
		}
	}
	rate := float64(falsePositives) / float64(trials)
	require.Less(t, rate, 0.06, "false-positive rate %v exceeds 6%% bound", rate)
}

package cuckoo

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestH32_BasicInsertLookupDelete exercises the core operations on a
// modest filter. Probabilistic tolerance is applied where the cuckoo
// false-positive rate could in principle affect a Lookup of an unknown key.
func TestH32_BasicInsertLookupDelete(t *testing.T) {
	cf := NewH32(1024)

	var h1, h2 [32]byte
	for i := range h1 {
		h1[i] = byte(i + 1)
	}
	for i := range h2 {
		h2[i] = byte(i + 100)
	}

	require.True(t, cf.Insert(&h1))
	require.True(t, cf.Insert(&h2))
	require.True(t, cf.Lookup(&h1))
	require.True(t, cf.Lookup(&h2))

	require.True(t, cf.Delete(&h1))
	require.True(t, cf.Delete(&h2))

	// After deletion, count should drop. Lookup may still return true for
	// deleted entries if a fingerprint collision left a residual — only
	// assert that Count tracks our explicit Inserts/Deletes.
	require.Equal(t, 0, cf.Count())
}

// TestH32_FalsePositiveRate verifies that the FP rate on random misses
// stays within the documented cuckoo bound (~3.1% theoretical; allow 6%).
func TestH32_FalsePositiveRate(t *testing.T) {
	const capacity = 1_000_000
	cf := NewH32(capacity)

	// Insert 100K random hashes.
	const inserted = 100_000
	added := make([][32]byte, inserted)
	for i := range added {
		_, err := rand.Read(added[i][:])
		require.NoError(t, err)
		require.True(t, cf.Insert(&added[i]), "Insert should succeed at <10%% load")
	}

	// Verify all inserted are present.
	for i := range added {
		require.True(t, cf.Lookup(&added[i]))
	}

	// Probe with fresh random hashes that were never inserted.
	const trials = 100_000
	falsePositives := 0
	for i := 0; i < trials; i++ {
		var h [32]byte
		_, err := rand.Read(h[:])
		require.NoError(t, err)
		if cf.Lookup(&h) {
			falsePositives++
		}
	}
	rate := float64(falsePositives) / float64(trials)
	require.Less(t, rate, 0.06, "false-positive rate %v exceeds 6%% bound", rate)
}

// TestH32_FillsUpAndFailsGracefully forces saturation and verifies
// Insert returns false rather than panicking or corrupting state.
func TestH32_FillsUpAndFailsGracefully(t *testing.T) {
	cf := NewH32(64)

	failures := 0
	successes := 0
	for i := 0; i < 1024; i++ {
		var h [32]byte
		_, err := rand.Read(h[:])
		require.NoError(t, err)
		if cf.Insert(&h) {
			successes++
		} else {
			failures++
		}
	}
	require.Positive(t, failures, "expected some Insert failures at heavy overload")
	require.Positive(t, successes, "expected some Insert successes before saturation")
}

// TestH32_SaturationLatchesAndShortCircuits verifies that once the
// eviction loop has exhausted MaxKicks, the saturated flag latches true
// and subsequent Inserts return false without re-running eviction. This
// is the fix for the CPU-melt observed on dev-scale-1 when a 2 GiB
// cuckoo at 92% load fell into eviction-loop hell at 1.7M TPS.
func TestH32_SaturationLatchesAndShortCircuits(t *testing.T) {
	cf := NewH32(64)

	// Fill the filter aggressively until eviction starts failing.
	for i := 0; i < 1024; i++ {
		var h [32]byte
		_, err := rand.Read(h[:])
		require.NoError(t, err)
		cf.Insert(&h)
		if cf.Saturated() {
			break
		}
	}
	require.True(t, cf.Saturated(), "expected saturated flag to latch after eviction failure")

	// Pre-generate hashes OUTSIDE the timed region so the measurement
	// reflects the saturated-Insert fast path and not the cost of
	// crypto/rand.Read (which dominates if it stays inside the loop and
	// makes this assertion flaky in CI).
	const n = 100_000
	hashes := make([][32]byte, n)
	for i := 0; i < n; i++ {
		_, err := rand.Read(hashes[i][:])
		require.NoError(t, err)
	}

	// Time a batch of Inserts that we expect to fail. With saturation
	// latched, these should all return immediately (no eviction churn).
	start := time.Now()
	for i := 0; i < n; i++ {
		cf.Insert(&hashes[i])
	}
	elapsed := time.Since(start)

	// Sanity bound: if the short-circuit is broken and eviction runs for
	// every Insert, we'd see ~100K × 500 kicks × ~30 ns = ~1.5 s. With
	// the short-circuit, this loop should complete in well under 100 ms
	// even on a modest machine. Allow generous slack for CI variance.
	require.Less(t, elapsed, time.Second,
		"saturated Insert short-circuit appears broken: %d Inserts took %v", n, elapsed)
}

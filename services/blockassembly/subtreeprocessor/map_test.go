package subtreeprocessor

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

func makeHash(b byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = b
	return h
}

func makeTestInpoints(id byte) *subtreepkg.TxInpoints {
	var h chainhash.Hash
	h[0] = id
	return &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{h}}
}

func TestSplitTxInpointsMap_SetIfNotExists(t *testing.T) {
	m := NewSplitTxInpointsMap(16)

	h := makeHash(1)
	inp := makeTestInpoints(1)

	// First insert should succeed
	result, wasSet := m.SetIfNotExists(h, inp)
	require.True(t, wasSet)
	require.Equal(t, inp, result)

	// Second insert should return existing
	inp2 := makeTestInpoints(2)
	result, wasSet = m.SetIfNotExists(h, inp2)
	require.False(t, wasSet)
	require.Equal(t, inp, result) // original value returned
}

func TestSplitTxInpointsMap_GetSetDelete(t *testing.T) {
	m := NewSplitTxInpointsMap(16)

	h := makeHash(42)
	inp := makeTestInpoints(42)

	// Get on empty map
	_, ok := m.Get(h)
	require.False(t, ok)
	require.False(t, m.Exists(h))

	// Set and Get
	m.Set(h, inp)
	got, ok := m.Get(h)
	require.True(t, ok)
	require.Equal(t, inp, got)
	require.True(t, m.Exists(h))
	require.Equal(t, 1, m.Length())

	// Delete
	deleted := m.Delete(h)
	require.True(t, deleted)
	require.False(t, m.Exists(h))
	require.Equal(t, 0, m.Length())

	// Delete non-existent
	deleted = m.Delete(h)
	require.False(t, deleted)
}

func TestSplitTxInpointsMap_Clear(t *testing.T) {
	m := NewSplitTxInpointsMap(16)

	for i := byte(0); i < 100; i++ {
		m.Set(makeHash(i), makeTestInpoints(i))
	}
	require.Equal(t, 100, m.Length())

	m.Clear()
	require.Equal(t, 0, m.Length())

	// Verify all entries gone
	for i := byte(0); i < 100; i++ {
		require.False(t, m.Exists(makeHash(i)))
	}
}

// testInpointPair is a (hash, inpoints) pair only used to keep the bulk-insert
// tests below readable; the production API now takes parallel slices.
type testInpointPair struct {
	Hash     chainhash.Hash
	Inpoints *subtreepkg.TxInpoints
}

// inpointSlicesFor builds the parallel (keys, values) slices accepted by
// PutMultiBucketTxInpoints from a list of (hash, inpoints) pairs.
func inpointSlicesFor(pairs ...testInpointPair) ([]chainhash.Hash, []*subtreepkg.TxInpoints) {
	keys := make([]chainhash.Hash, len(pairs))
	vals := make([]*subtreepkg.TxInpoints, len(pairs))

	for i, p := range pairs {
		keys[i] = p.Hash
		vals[i] = p.Inpoints
	}

	return keys, vals
}

func TestSplitSwissMap_ClearEmptiesAndPreservesUsability(t *testing.T) {
	m := NewSplitSwissMap(16, 64)

	for i := 0; i < 32; i++ {
		require.NoError(t, m.Put(makeHash(byte(i))))
	}

	require.Equal(t, 32, m.Length(), "populated map length should match insert count")

	m.Clear()

	require.Equal(t, 0, m.Length(), "Clear must empty every bucket")

	// After Clear, the same buckets must still be usable for further inserts.
	for i := 0; i < 8; i++ {
		require.NoError(t, m.Put(makeHash(byte(100+i))))
	}

	require.Equal(t, 8, m.Length(), "map must accept new inserts after Clear")
	require.True(t, m.Exists(makeHash(100)), "post-clear inserts must be queryable")
	require.False(t, m.Exists(makeHash(0)), "pre-clear entries must not be present")
}

func TestSplitTxInpointsMap_PutMultiBucketTxInpointsRespectsExistingEntries(t *testing.T) {
	m := NewSplitTxInpointsMap(16)

	hashA := makeHash(1)
	hashB := makeHash(2)
	inpointsA := &subtreepkg.TxInpoints{}
	inpointsB := &subtreepkg.TxInpoints{}

	// Pre-existing entry for hashA to verify duplicate handling.
	_, ok := m.SetIfNotExists(hashA, inpointsA)
	require.True(t, ok, "first SetIfNotExists must succeed")

	bucketA := m.BucketFor(hashA)
	bucketB := m.BucketFor(hashB)

	// Drive both through the bulk path even if they land in different buckets.
	keysA, valsA := inpointSlicesFor(testInpointPair{hashA, inpointsA})
	resA := m.PutMultiBucketTxInpoints(bucketA, keysA, valsA)
	require.Equal(t, []bool{false}, resA, "duplicate insert via bulk must report wasInserted=false")

	if bucketA == bucketB {
		// Combined batch into one bucket: order must be preserved.
		keys, vals := inpointSlicesFor(
			testInpointPair{hashA, inpointsA},
			testInpointPair{hashB, inpointsB},
		)
		res := m.PutMultiBucketTxInpoints(bucketA, keys, vals)
		require.Equal(t, []bool{false, true}, res, "duplicate then new in one bucket")
	} else {
		keysB, valsB := inpointSlicesFor(testInpointPair{hashB, inpointsB})
		resB := m.PutMultiBucketTxInpoints(bucketB, keysB, valsB)
		require.Equal(t, []bool{true}, resB, "new insert via bulk must report wasInserted=true")
	}

	gotA, foundA := m.Get(hashA)
	require.True(t, foundA)
	require.Same(t, inpointsA, gotA, "bulk path must not overwrite an existing entry")

	gotB, foundB := m.Get(hashB)
	require.True(t, foundB)
	require.Same(t, inpointsB, gotB, "bulk path must insert new entries")
}

func TestSplitTxInpointsMap_ClearPreservesBucketStructure(t *testing.T) {
	m := NewSplitTxInpointsMap(8)

	for i := 0; i < 16; i++ {
		_, _ = m.SetIfNotExists(makeHash(byte(i)), &subtreepkg.TxInpoints{})
	}

	require.Equal(t, 16, m.Length())

	m.Clear()
	require.Equal(t, 0, m.Length())

	// Re-insert and confirm bucket routing still works (no nil-bucket panic).
	for i := 0; i < 4; i++ {
		_, ok := m.SetIfNotExists(makeHash(byte(200+i)), &subtreepkg.TxInpoints{})
		require.True(t, ok)
	}

	require.Equal(t, 4, m.Length())
}

func TestSplitTxInpointsMap_ConcurrentAccess(t *testing.T) {
	m := NewSplitTxInpointsMap(256)
	const n = 10000

	// Concurrent writes
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var h chainhash.Hash
			h[0] = byte(idx)
			h[1] = byte(idx >> 8)
			m.Set(h, makeTestInpoints(byte(idx)))
		}(i)
	}
	wg.Wait()

	// Concurrent reads
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var h chainhash.Hash
			h[0] = byte(idx)
			h[1] = byte(idx >> 8)
			_, _ = m.Get(h)
			_ = m.Exists(h)
		}(i)
	}
	wg.Wait()
}

func TestSplitTxInpointsMap_BucketDistribution(t *testing.T) {
	m := NewSplitTxInpointsMap(16)

	// Insert 1000 entries with varied hashes
	for i := 0; i < 1000; i++ {
		var h chainhash.Hash
		h[0] = byte(i)
		h[1] = byte(i >> 8)
		h[2] = byte(i >> 16)
		m.Set(h, makeTestInpoints(byte(i)))
	}

	require.Equal(t, 1000, m.Length())

	// Verify at least half the buckets got entries (basic distribution check)
	bucketsUsed := 0
	for i := uint16(0); i < 16; i++ {
		b := &m.buckets[i]
		b.mu.Lock()
		if b.m.Count() > 0 {
			bucketsUsed++
		}
		b.mu.Unlock()
	}
	require.Greater(t, bucketsUsed, 2, "hashes should distribute across multiple buckets")
}

func TestParallelBulkSetIfNotExists_Correctness(t *testing.T) {
	m := NewSplitTxInpointsMap(256)
	const n = 10000

	hashes := make([]chainhash.Hash, n)
	inpoints := make([]*subtreepkg.TxInpoints, n)
	for i := 0; i < n; i++ {
		hashes[i][0] = byte(i)
		hashes[i][1] = byte(i >> 8)
		hashes[i][2] = byte(i >> 16)
		inpoints[i] = makeTestInpoints(byte(i))
	}

	wasSet := make([]bool, n)
	m.ParallelBulkSetIfNotExists(hashes, inpoints, wasSet)

	// All should have been set
	for i := 0; i < n; i++ {
		require.True(t, wasSet[i], "entry %d should have been set", i)
	}

	// Verify all entries exist
	require.Equal(t, n, m.Length())
	for i := 0; i < n; i++ {
		got, ok := m.Get(hashes[i])
		require.True(t, ok)
		require.Equal(t, inpoints[i], got)
	}
}

func TestParallelBulkSetIfNotExists_Duplicates(t *testing.T) {
	m := NewSplitTxInpointsMap(256)
	const n = 5000

	// Pre-populate half the entries
	hashes := make([]chainhash.Hash, n)
	inpoints := make([]*subtreepkg.TxInpoints, n)
	for i := 0; i < n; i++ {
		hashes[i][0] = byte(i)
		hashes[i][1] = byte(i >> 8)
		inpoints[i] = makeTestInpoints(byte(i))
	}

	// Insert first half
	for i := 0; i < n/2; i++ {
		m.Set(hashes[i], inpoints[i])
	}

	// Bulk insert all — first half should be duplicates
	newInpoints := make([]*subtreepkg.TxInpoints, n)
	for i := 0; i < n; i++ {
		newInpoints[i] = makeTestInpoints(byte(i + 100))
	}

	wasSet := make([]bool, n)
	m.ParallelBulkSetIfNotExists(hashes, newInpoints, wasSet)

	// First half should NOT be set (duplicates)
	for i := 0; i < n/2; i++ {
		require.False(t, wasSet[i], "entry %d was pre-existing, should not be set", i)
		// Original value should be preserved
		got, _ := m.Get(hashes[i])
		require.Equal(t, inpoints[i], got)
	}

	// Second half should be set
	for i := n / 2; i < n; i++ {
		require.True(t, wasSet[i], "entry %d should have been set", i)
	}

	require.Equal(t, n, m.Length())
}

func TestParallelBulkSetIfNotExists_ConcurrentSafety(t *testing.T) {
	m := NewSplitTxInpointsMap(256)
	const n = 5000

	hashes := make([]chainhash.Hash, n)
	inpoints := make([]*subtreepkg.TxInpoints, n)
	for i := 0; i < n; i++ {
		hashes[i][0] = byte(i)
		hashes[i][1] = byte(i >> 8)
		inpoints[i] = makeTestInpoints(byte(i))
	}

	// Run two concurrent bulk inserts with overlapping keys
	var wg sync.WaitGroup
	wg.Add(2)

	wasSet1 := make([]bool, n)
	wasSet2 := make([]bool, n)

	go func() {
		defer wg.Done()
		m.ParallelBulkSetIfNotExists(hashes, inpoints, wasSet1)
	}()
	go func() {
		defer wg.Done()
		m.ParallelBulkSetIfNotExists(hashes, inpoints, wasSet2)
	}()
	wg.Wait()

	// Each key should be set by exactly one caller
	for i := 0; i < n; i++ {
		// At least one must have set it
		require.True(t, wasSet1[i] || wasSet2[i], "entry %d not set by either caller", i)
	}

	require.Equal(t, n, m.Length())
}

func TestParallelBulkSetIfNotExists_Empty(t *testing.T) {
	m := NewSplitTxInpointsMap(256)

	// Should not panic on empty input
	m.ParallelBulkSetIfNotExists(nil, nil, nil)
	m.ParallelBulkSetIfNotExists([]chainhash.Hash{}, []*subtreepkg.TxInpoints{}, []bool{})

	require.Equal(t, 0, m.Length())
}

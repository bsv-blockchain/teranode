package subtreeprocessor

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/stretchr/testify/require"
)

// TestSplitSwissMap_FreezeExistsReturnsCorrectResults verifies that freezing
// the map does not change lookup results: present hashes stay present, absent
// hashes stay absent, before and after Freeze.
func TestSplitSwissMap_FreezeExistsReturnsCorrectResults(t *testing.T) {
	m := NewSplitSwissMap(16, 1024)

	present := genBenchHashes(512, 10)
	absent := genBenchHashes(512, 20)

	for _, h := range present {
		require.NoError(t, m.Put(h))
	}

	for _, h := range present {
		require.True(t, m.Exists(h), "pre-freeze: expected %s to exist", h)
	}

	for _, h := range absent {
		require.False(t, m.Exists(h), "pre-freeze: expected %s to be absent", h)
	}

	m.Freeze()

	for _, h := range present {
		require.True(t, m.Exists(h), "post-freeze: expected %s to exist", h)
	}

	for _, h := range absent {
		require.False(t, m.Exists(h), "post-freeze: expected %s to be absent", h)
	}

	require.Equal(t, len(present), m.Length())
}

// TestSplitSwissMap_PutAfterFreezeReturnsError verifies that all write paths
// fail loudly on a frozen map instead of silently corrupting the unlocked
// read path.
func TestSplitSwissMap_PutAfterFreezeReturnsError(t *testing.T) {
	m := NewSplitSwissMap(16, 1024)

	h := genBenchHashes(1, 30)[0]
	require.NoError(t, m.Put(h))

	m.Freeze()

	err := m.Put(genBenchHashes(1, 31)[0])
	require.Error(t, err)

	extra := genBenchHashes(8, 32)
	bucket := txmap.Bytes2Uint16Buckets(extra[0], m.Buckets())

	err = m.PutMultiBucket(bucket, extra)
	require.Error(t, err)

	// The frozen map must be unchanged by the rejected writes.
	require.Equal(t, 1, m.Length())
	require.True(t, m.Exists(h))
}

// TestSplitSwissMap_ClearUnfreezesForPoolReuse verifies the pooled-map cycle
// used by CreateTransactionMap: fill → Freeze → Clear → fill again. Clear must
// un-freeze, or the next block's inserts would all fail.
func TestSplitSwissMap_ClearUnfreezesForPoolReuse(t *testing.T) {
	m := NewSplitSwissMap(16, 1024)

	first := genBenchHashes(256, 40)
	for _, h := range first {
		require.NoError(t, m.Put(h))
	}

	m.Freeze()
	m.Clear()

	require.Equal(t, 0, m.Length())

	second := genBenchHashes(256, 41)
	for _, h := range second {
		require.NoError(t, m.Put(h), "Put after Clear must succeed (pool reuse)")
	}

	for _, h := range second {
		require.True(t, m.Exists(h))
	}

	for _, h := range first {
		require.False(t, m.Exists(h), "cleared hash must be gone")
	}

	// A second freeze cycle must work the same way.
	m.Freeze()
	require.Error(t, m.Put(genBenchHashes(1, 42)[0]))
	require.True(t, m.Exists(second[0]))
}

// TestSplitSwissMap_FrozenConcurrentReads builds the map concurrently, freezes
// it, then hammers Exists from many goroutines. Run with -race: the frozen
// read path takes no locks, which is only safe because freezing happens after
// all writers have finished.
func TestSplitSwissMap_FrozenConcurrentReads(t *testing.T) {
	const total = 64 * 1024

	hashes := genBenchHashes(total, 50)
	m := NewSplitSwissMap(64, total)

	// Concurrent build via PutMultiBucket, one goroutine per bucket group
	// (mirrors the single-writer-per-bucket production pattern).
	grouped := make(map[uint16][]chainhash.Hash)
	for _, h := range hashes {
		bucket := txmap.Bytes2Uint16Buckets(h, m.Buckets())
		grouped[bucket] = append(grouped[bucket], h)
	}

	var buildWg sync.WaitGroup
	for bucket, bucketHashes := range grouped {
		buildWg.Add(1)

		go func(bucket uint16, bucketHashes []chainhash.Hash) {
			defer buildWg.Done()
			// t.Errorf, not require: require's FailNow is only safe on the
			// test goroutine (matches the read goroutines below).
			if err := m.PutMultiBucket(bucket, bucketHashes); err != nil {
				t.Errorf("PutMultiBucket failed: %v", err)
			}
		}(bucket, bucketHashes)
	}

	buildWg.Wait()
	m.Freeze()

	absent := genBenchHashes(total, 51)

	var readWg sync.WaitGroup
	for w := 0; w < 16; w++ {
		readWg.Add(1)

		go func(offset int) {
			defer readWg.Done()

			for i := 0; i < total; i++ {
				idx := (i + offset) % total
				if !m.Exists(hashes[idx]) {
					t.Errorf("expected %s to exist", hashes[idx])
					return
				}

				if m.Exists(absent[idx]) {
					t.Errorf("expected %s to be absent", absent[idx])
					return
				}
			}
		}(w * 1024)
	}

	readWg.Wait()
	require.Equal(t, total, m.Length())
}

package subtreeprocessor

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/stretchr/testify/require"
)

// bucketizeForTest groups hashes per bucket the way
// DeserializeHashesFromReaderIntoBuckets hands them to the insert stage.
func bucketizeForTest(hashes []chainhash.Hash, nrOfBuckets uint16) map[uint16][]chainhash.Hash {
	out := make(map[uint16][]chainhash.Hash)
	for _, h := range hashes {
		bucket := txmap.Bytes2Uint16Buckets(h, nrOfBuckets)
		out[bucket] = append(out[bucket], h)
	}

	return out
}

// TestBucketInserter_AllHashesInserted submits many subtree-shaped batches
// from concurrent goroutines (the CreateTransactionMap shape) and verifies
// full map content after closeAndWait.
func TestBucketInserter_AllHashesInserted(t *testing.T) {
	const (
		numSubtrees = 32
		perSubtree  = 4096
	)

	m := NewSplitSwissMap(64, numSubtrees*perSubtree)
	ins := newBucketInserter(m, 8)

	all := genBenchHashes(numSubtrees*perSubtree, 60)

	var wg sync.WaitGroup
	for s := 0; s < numSubtrees; s++ {
		wg.Add(1)

		go func(s int) {
			defer wg.Done()

			buckets := bucketizeForTest(all[s*perSubtree:(s+1)*perSubtree], m.Buckets())
			// t.Errorf, not require: this runs in a worker goroutine and
			// require's FailNow is only safe on the test goroutine.
			if err := ins.submit(buckets); err != nil {
				t.Errorf("submit failed: %v", err)
			}
		}(s)
	}

	wg.Wait()
	require.NoError(t, ins.closeAndWait())

	require.Equal(t, numSubtrees*perSubtree, m.Length())

	for _, h := range all {
		require.True(t, m.Exists(h), "expected %s to exist", h)
	}
}

// TestBucketInserter_DuplicateHashesIdempotent verifies that the same hashes
// submitted twice (e.g. a tx appearing in two announced subtrees) result in a
// single map entry each.
func TestBucketInserter_DuplicateHashesIdempotent(t *testing.T) {
	const total = 2048

	m := NewSplitSwissMap(16, total)
	ins := newBucketInserter(m, 4)

	hashes := genBenchHashes(total, 61)
	buckets := bucketizeForTest(hashes, m.Buckets())

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			if err := ins.submit(buckets); err != nil {
				t.Errorf("submit failed: %v", err)
			}
		}()
	}

	wg.Wait()
	require.NoError(t, ins.closeAndWait())
	require.Equal(t, total, m.Length())
}

// TestBucketInserter_EmptySubmitAndClampedWorkers covers the degenerate
// configs: zero/negative worker counts clamp to a working pool, empty submits
// are no-ops, and immediate closeAndWait succeeds.
func TestBucketInserter_EmptySubmitAndClampedWorkers(t *testing.T) {
	m := NewSplitSwissMap(16, 16)

	ins := newBucketInserter(m, 0)
	require.NoError(t, ins.submit(map[uint16][]chainhash.Hash{}))
	require.NoError(t, ins.submit(nil))
	require.NoError(t, ins.closeAndWait())
	require.Equal(t, 0, m.Length())

	// More workers than buckets must clamp rather than spawn idle workers
	// indexing out of range.
	m2 := NewSplitSwissMap(4, 1024)
	ins2 := newBucketInserter(m2, 1000)

	hashes := genBenchHashes(512, 62)
	require.NoError(t, ins2.submit(bucketizeForTest(hashes, m2.Buckets())))
	require.NoError(t, ins2.closeAndWait())
	require.Equal(t, 512, m2.Length())
}

// TestBucketInserter_SubmitAfterCloseErrors verifies misuse fails loudly
// instead of panicking on a closed channel.
func TestBucketInserter_SubmitAfterCloseErrors(t *testing.T) {
	m := NewSplitSwissMap(16, 16)
	ins := newBucketInserter(m, 2)

	require.NoError(t, ins.closeAndWait())

	hashes := genBenchHashes(16, 63)
	err := ins.submit(bucketizeForTest(hashes, m.Buckets()))
	require.Error(t, err)
}

// TestBucketInserter_FrozenMapSurfacesError verifies the error-drain path: a
// frozen map makes every PutMultiBucket fail; closeAndWait must surface the
// error and submitters must not deadlock on full channels.
func TestBucketInserter_FrozenMapSurfacesError(t *testing.T) {
	const total = 64 * 1024

	m := NewSplitSwissMap(16, total)
	m.Freeze()

	ins := newBucketInserter(m, 2)

	hashes := genBenchHashes(total, 64)
	buckets := bucketizeForTest(hashes, m.Buckets())

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			// submit may or may not error depending on timing; the contract
			// is only that it never deadlocks and closeAndWait reports it.
			_ = ins.submit(buckets)
		}()
	}

	wg.Wait()
	require.Error(t, ins.closeAndWait())
}

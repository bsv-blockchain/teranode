package subtreeprocessor

import (
	"os"
	"os/exec"
	"runtime"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

const peakChildEnv = "SORTED_HASH_SET_PEAK_CHILD"

// TestSortedHashSet_FreezePeakLiveHeap pins that Freeze never holds every hash
// twice: each partition's sorted array replaces its build chunks, so the live
// heap during Freeze stays near the frozen size. A single array for the whole set
// allocated up front, with every partition's chunks still live, makes it ~2x.
// The live heap is sampled with forced GCs while Freeze runs on one worker, in a
// child process: the heap is process-wide, and goroutines other tests leave behind
// would add to it.
func TestSortedHashSet_FreezePeakLiveHeap(t *testing.T) {
	skipUnlessHeavyTests(t) // allocates ~200 MB

	if os.Getenv(peakChildEnv) == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestSortedHashSet_FreezePeakLiveHeap$", "-test.count=1", "-test.v")
		cmd.Env = append(os.Environ(), peakChildEnv+"=1")

		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
		require.Contains(t, string(out), "--- PASS: TestSortedHashSet_FreezePeakLiveHeap")

		return
	}

	const n = 1 << 22 // 128 MiB of hashes in 64 partitions of 64K

	s := NewSortedHashSet(n)

	func() { // the input slice is garbage before the measurement
		h := randHashes(n, 7)

		for i := range h {
			require.NoError(t, s.Put(h[i]))
		}
	}()

	runtime.GC()

	var ms runtime.MemStats

	runtime.ReadMemStats(&ms)
	before := ms.HeapAlloc

	var (
		done atomic.Bool
		peak uint64
		ret  = make(chan struct{})
	)

	go func() {
		defer close(ret)

		for !done.Load() {
			runtime.GC()
			runtime.ReadMemStats(&ms)
			peak = max(peak, ms.HeapAlloc)
		}
	}()

	require.NoError(t, s.Freeze(1))
	done.Store(true)
	<-ret

	hashBytes := uint64(n * chainhash.HashSize)
	require.Less(t, peak, before+hashBytes/4,
		"peak live heap %d MiB during Freeze, %d MiB before: the hashes were held twice", peak>>20, before>>20)
	require.Equal(t, n, s.Length())
}

// PutMultiBucket refuses a hash that does not belong to the partition it is given,
// instead of letting Freeze index out of range on the block assembly goroutine.
func TestSortedHashSet_PutMultiBucketWrongPartition(t *testing.T) {
	s := NewSortedHashSet(1 << 20)
	require.Greater(t, s.Partitions(), 1)

	h := randHashes(64, 3)
	part := s.Partition(h[0])

	var other chainhash.Hash

	for _, x := range h {
		if s.Partition(x) != part {
			other = x
			break
		}
	}

	require.NotEqual(t, chainhash.Hash{}, other, "fixture: a hash of another partition")

	err := s.PutMultiBucket(part, []chainhash.Hash{h[0], other})
	require.Error(t, err)
	require.Contains(t, err.Error(), other.String())

	require.NoError(t, s.Freeze(2))
	require.False(t, s.Exists(other))
}

func TestSortedHashPartSize(t *testing.T) {
	require.Equal(t, uintptr(64), unsafe.Sizeof(sortedHashPart{}))
}

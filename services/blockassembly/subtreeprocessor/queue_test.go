package subtreeprocessor

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_queue(t *testing.T) {
	q := NewLockFreeQueue()

	enqueueItems(t, q, 1, 10)

	items := 0

	for {
		node, txInpoints, time64, found := q.dequeue(0)
		if !found {
			break
		}

		assert.Equal(t, uint64(items), node.Fee) // nolint:gosec
		assert.Equal(t, subtree.TxInpoints{}, txInpoints)
		assert.Greater(t, time64, int64(0))

		items++
	}

	assert.True(t, q.IsEmpty())

	assert.Equal(t, 10, items)

	enqueueItems(t, q, 1, 10)

	items = 0

	for {
		node, txInpoints, time64, found := q.dequeue(0)
		if !found {
			break
		}

		assert.Equal(t, uint64(items), node.Fee) // nolint:gosec
		assert.Equal(t, subtree.TxInpoints{}, txInpoints)
		assert.Greater(t, time64, int64(0))

		items++
	}

	assert.True(t, q.IsEmpty())

	assert.Equal(t, 10, items)
}

func Test_queueWithTime(t *testing.T) {
	q := NewLockFreeQueue()

	enqueueItems(t, q, 1, 10)

	validFromMillis := time.Now().Add(-100 * time.Millisecond).UnixMilli()
	_, _, _, found := q.dequeue(validFromMillis)
	require.False(t, found)

	time.Sleep(50 * time.Millisecond)

	validFromMillis = time.Now().Add(-100 * time.Millisecond).UnixMilli()
	_, _, _, found = q.dequeue(validFromMillis)
	require.False(t, found)

	time.Sleep(100 * time.Millisecond)

	items := 0
	validFromMillis = time.Now().Add(-100 * time.Millisecond).UnixMilli()

	for {
		node, txInpoints, time64, found := q.dequeue(validFromMillis)
		if !found {
			break
		}

		assert.Equal(t, uint64(items), node.Fee) // nolint:gosec
		assert.Equal(t, subtree.TxInpoints{}, txInpoints)
		assert.Greater(t, time64, int64(0))

		items++
	}

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10, items)

	enqueueItems(t, q, 1, 10)

	validFromMillis = time.Now().Add(-100 * time.Millisecond).UnixMilli()
	_, _, _, found = q.dequeue(validFromMillis)
	require.False(t, found)

	time.Sleep(50 * time.Millisecond)

	validFromMillis = time.Now().Add(-100 * time.Millisecond).UnixMilli()
	_, _, _, found = q.dequeue(validFromMillis)
	require.False(t, found)

	time.Sleep(100 * time.Millisecond)

	items = 0
	validFromMillis = time.Now().Add(-100 * time.Millisecond).UnixMilli()

	for {
		node, txInpoints, time64, found := q.dequeue(validFromMillis)
		if !found {
			break
		}

		assert.Equal(t, uint64(items), node.Fee)
		assert.Equal(t, subtree.TxInpoints{}, txInpoints)
		assert.Greater(t, time64, int64(0))

		items++
	}

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10, items)
}

func Test_queue2Threads(t *testing.T) {
	q := NewLockFreeQueue()

	enqueueItems(t, q, 2, 10)

	items := 0

	for {
		node, _, time64, found := q.dequeue(0)
		if !found {
			break
		}

		items++

		t.Logf("Item: %d, %d\n", node.Fee, time64)
	}

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 20, items)

	enqueueItems(t, q, 2, 10)

	items = 0

	for {
		node, _, time64, found := q.dequeue(0)
		if !found {
			break
		}

		items++

		t.Logf("Item: %d, %d\n", node.Fee, time64)
	}

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 20, items)
}

func Test_queueLarge(t *testing.T) {
	runtime.GC()

	q := NewLockFreeQueue()

	enqueueItems(t, q, 1, 10_000_000)

	startTime := time.Now()

	items := 0

	for {
		_, _, _, found := q.dequeue(0)
		if !found {
			break
		}

		items++
	}

	t.Logf("Time empty %d items: %s\n", items, time.Since(startTime))
	t.Logf("Mem used for queue: %s\n", printAlloc())

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10_000_000, items)

	runtime.GC()

	enqueueItems(t, q, 1_000, 10_000)

	startTime = time.Now()

	items = 0

	for {
		_, _, _, found := q.dequeue(0)
		if !found {
			break
		}

		items++
	}

	t.Logf("Time empty %d items: %s\n", items, time.Since(startTime))
	t.Logf("Mem used after dequeue: %s\n", printAlloc())
	runtime.GC()
	t.Logf("Mem used after dequeue after GC: %s\n", printAlloc())

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10_000_000, items)
}

// enqueueItems adds test items to a queue for testing.
//
// Parameters:
//   - t: Testing instance
//   - q: Queue to populate
//   - threads: Number of concurrent threads
//   - iter: Number of iterations per thread
func enqueueItems(t *testing.T, q *LockFreeQueue, threads, iter int) {
	startTime := time.Now()

	var wg sync.WaitGroup

	for n := 0; n < threads; n++ {
		wg.Add(1)

		go func(n int) {
			defer wg.Done()

			for i := 0; i < iter; i++ {
				u := (n * iter) + i
				q.enqueue(subtree.Node{
					Hash:        chainhash.Hash{},
					Fee:         uint64(u),
					SizeInBytes: 0,
				}, subtree.TxInpoints{})
			}
		}(n)
	}

	wg.Wait()
	t.Logf("Time queue %d items: %s\n", threads*iter, time.Since(startTime))
}

// Benchmark functions for performance testing

// BenchmarkQueue tests queue performance.
func BenchmarkQueue(b *testing.B) {
	q := NewLockFreeQueue()

	b.ResetTimer()

	go func() {
		for {
			_, _, _, found := q.dequeue(0)
			if !found {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		q.enqueue(subtree.Node{
			Hash:        chainhash.Hash{},
			Fee:         uint64(i),
			SizeInBytes: 0,
		}, subtree.TxInpoints{})
	}
}

// BenchmarkAtomicPointer tests atomic pointer operations.
func BenchmarkAtomicPointer(b *testing.B) {
	var v atomic.Pointer[TxIDAndFee]

	t1 := &TxIDAndFee{
		node: subtree.Node{
			Hash:        chainhash.Hash{},
			Fee:         1,
			SizeInBytes: 0,
		},
	}
	t2 := &TxIDAndFee{
		node: subtree.Node{
			Hash:        chainhash.Hash{},
			Fee:         1,
			SizeInBytes: 0,
		},
	}

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			v.Swap(t1)
		} else {
			v.Swap(t2)
		}
	}
}

// printAlloc formats memory allocation information for testing.
//
// Returns:
//   - string: Formatted memory allocation string
func printAlloc() string {
	var m runtime.MemStats

	runtime.ReadMemStats(&m)

	return fmt.Sprintf("%d MB", m.Alloc/(1024*1024))
}

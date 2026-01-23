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

	enqueueBatches(t, q, 1, 10)

	batches := 0
	totalTxs := 0

	for {
		batch, found := q.dequeueBatch(0)
		if !found {
			break
		}

		assert.Greater(t, batch.time, int64(0))
		totalTxs += len(batch.nodes)
		batches++
	}

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10, batches)
	assert.Equal(t, 10, totalTxs) // each batch has 1 tx

	enqueueBatches(t, q, 1, 10)

	batches = 0
	totalTxs = 0

	for {
		batch, found := q.dequeueBatch(0)
		if !found {
			break
		}

		assert.Greater(t, batch.time, int64(0))
		totalTxs += len(batch.nodes)
		batches++
	}

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10, batches)
	assert.Equal(t, 10, totalTxs)
}

func Test_queueWithTime(t *testing.T) {
	q := NewLockFreeQueue()

	enqueueBatches(t, q, 1, 10)

	validFromMillis := time.Now().Add(-100 * time.Millisecond).UnixMilli()
	_, found := q.dequeueBatch(validFromMillis)
	require.False(t, found)

	time.Sleep(50 * time.Millisecond)

	validFromMillis = time.Now().Add(-100 * time.Millisecond).UnixMilli()
	_, found = q.dequeueBatch(validFromMillis)
	require.False(t, found)

	time.Sleep(100 * time.Millisecond)

	batches := 0
	validFromMillis = time.Now().Add(-100 * time.Millisecond).UnixMilli()

	for {
		batch, found := q.dequeueBatch(validFromMillis)
		if !found {
			break
		}

		assert.Greater(t, batch.time, int64(0))
		batches++
	}

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10, batches)

	enqueueBatches(t, q, 1, 10)

	validFromMillis = time.Now().Add(-100 * time.Millisecond).UnixMilli()
	_, found = q.dequeueBatch(validFromMillis)
	require.False(t, found)

	time.Sleep(50 * time.Millisecond)

	validFromMillis = time.Now().Add(-100 * time.Millisecond).UnixMilli()
	_, found = q.dequeueBatch(validFromMillis)
	require.False(t, found)

	time.Sleep(100 * time.Millisecond)

	batches = 0
	validFromMillis = time.Now().Add(-100 * time.Millisecond).UnixMilli()

	for {
		batch, found := q.dequeueBatch(validFromMillis)
		if !found {
			break
		}

		assert.Greater(t, batch.time, int64(0))
		batches++
	}

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10, batches)
}

func Test_queue2Threads(t *testing.T) {
	q := NewLockFreeQueue()

	enqueueBatches(t, q, 2, 10)

	batches := 0

	for {
		batch, found := q.dequeueBatch(0)
		if !found {
			break
		}

		batches++

		t.Logf("Batch: time=%d, txs=%d\n", batch.time, len(batch.nodes))
	}

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 20, batches)

	enqueueBatches(t, q, 2, 10)

	batches = 0

	for {
		batch, found := q.dequeueBatch(0)
		if !found {
			break
		}

		batches++

		t.Logf("Batch: time=%d, txs=%d\n", batch.time, len(batch.nodes))
	}

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 20, batches)
}

func Test_queueLarge(t *testing.T) {
	runtime.GC()

	q := NewLockFreeQueue()

	enqueueBatches(t, q, 1, 10_000_000)

	startTime := time.Now()

	batches := 0

	for {
		_, found := q.dequeueBatch(0)
		if !found {
			break
		}

		batches++
	}

	t.Logf("Time empty %d batches: %s\n", batches, time.Since(startTime))
	t.Logf("Mem used for queue: %s\n", printAlloc())

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10_000_000, batches)

	runtime.GC()

	enqueueBatches(t, q, 1_000, 10_000)

	startTime = time.Now()

	batches = 0

	for {
		_, found := q.dequeueBatch(0)
		if !found {
			break
		}

		batches++
	}

	t.Logf("Time empty %d batches: %s\n", batches, time.Since(startTime))
	t.Logf("Mem used after dequeue: %s\n", printAlloc())
	runtime.GC()
	t.Logf("Mem used after dequeue after GC: %s\n", printAlloc())

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10_000_000, batches)
}

// enqueueBatches adds test batches to a queue for testing.
// Each batch contains a single transaction for testing simplicity.
//
// Parameters:
//   - t: Testing instance
//   - q: Queue to populate
//   - threads: Number of concurrent threads
//   - iter: Number of iterations per thread (each iteration enqueues one batch)
func enqueueBatches(t *testing.T, q *LockFreeQueue, threads, iter int) {
	startTime := time.Now()

	var wg sync.WaitGroup

	for n := 0; n < threads; n++ {
		wg.Add(1)

		go func(n int) {
			defer wg.Done()

			for i := 0; i < iter; i++ {
				u := (n * iter) + i
				// Each batch contains a single transaction
				q.enqueueBatch(
					[]subtree.Node{{
						Hash:        chainhash.Hash{},
						Fee:         uint64(u),
						SizeInBytes: 0,
					}},
					[]*subtree.TxInpoints{{}},
				)
			}
		}(n)
	}

	wg.Wait()
	t.Logf("Time queue %d batches: %s\n", threads*iter, time.Since(startTime))
}

// Benchmark functions for performance testing

// BenchmarkQueue tests queue performance.
func BenchmarkQueue(b *testing.B) {
	q := NewLockFreeQueue()

	b.ResetTimer()

	go func() {
		for {
			_, found := q.dequeueBatch(0)
			if !found {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		q.enqueueBatch(
			[]subtree.Node{{
				Hash:        chainhash.Hash{},
				Fee:         uint64(i),
				SizeInBytes: 0,
			}},
			[]*subtree.TxInpoints{{}},
		)
	}
}

// BenchmarkAtomicPointer tests atomic pointer operations.
func BenchmarkAtomicPointer(b *testing.B) {
	var v atomic.Pointer[TxBatch]

	t1 := &TxBatch{
		nodes: []subtree.Node{{
			Hash:        chainhash.Hash{},
			Fee:         1,
			SizeInBytes: 0,
		}},
	}
	t2 := &TxBatch{
		nodes: []subtree.Node{{
			Hash:        chainhash.Hash{},
			Fee:         1,
			SizeInBytes: 0,
		}},
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

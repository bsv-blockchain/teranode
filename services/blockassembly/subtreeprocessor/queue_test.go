package subtreeprocessor

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_queue(t *testing.T) {
	q := NewLockFreeQueue()

	enqueueItems(t, q, 1, 10)

	items := 0
	for {
		item := q.dequeue(0)
		if item == nil {
			break
		}
		assert.Equal(t, uint64(items), item.node.Fee)
		items++
		//t.Logf("Item: %d\n", item.node.Fee)
	}
	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10, items)

	enqueueItems(t, q, 1, 10)

	items = 0
	for {
		item := q.dequeue(0)
		if item == nil {
			break
		}
		assert.Equal(t, uint64(items), item.node.Fee)
		items++
		//t.Logf("Item: %d\n", item.node.Fee)
	}
	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10, items)
}

func Test_queueWithTime(t *testing.T) {
	q := NewLockFreeQueue()

	enqueueItems(t, q, 1, 10)

	validFromMillis := time.Now().Add(-100 * time.Millisecond).UnixMilli()
	item := q.dequeue(validFromMillis)
	require.Nil(t, item)

	time.Sleep(50 * time.Millisecond)

	validFromMillis = time.Now().Add(-100 * time.Millisecond).UnixMilli()
	item = q.dequeue(validFromMillis)
	require.Nil(t, item)

	time.Sleep(100 * time.Millisecond)

	items := 0
	validFromMillis = time.Now().Add(-100 * time.Millisecond).UnixMilli()
	for {
		item = q.dequeue(validFromMillis)
		if item == nil {
			break
		}
		assert.Equal(t, uint64(items), item.node.Fee)
		items++
		//t.Logf("Item: %d\n", item.node.Fee)
	}
	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10, items)

	enqueueItems(t, q, 1, 10)

	validFromMillis = time.Now().Add(-100 * time.Millisecond).UnixMilli()
	item = q.dequeue(validFromMillis)
	require.Nil(t, item)

	time.Sleep(50 * time.Millisecond)

	validFromMillis = time.Now().Add(-100 * time.Millisecond).UnixMilli()
	item = q.dequeue(validFromMillis)
	require.Nil(t, item)

	time.Sleep(100 * time.Millisecond)

	items = 0
	validFromMillis = time.Now().Add(-100 * time.Millisecond).UnixMilli()
	for {
		item = q.dequeue(validFromMillis)
		if item == nil {
			break
		}
		assert.Equal(t, uint64(items), item.node.Fee)
		items++
		//t.Logf("Item: %d\n", item.node.Fee)
	}
	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10, items)
}

func Test_queue2Threads(t *testing.T) {
	q := NewLockFreeQueue()

	enqueueItems(t, q, 2, 10)

	items := 0
	for {
		item := q.dequeue(0)
		if item == nil {
			break
		}
		items++
		t.Logf("Item: %d\n", item.node.Fee)
	}
	assert.True(t, q.IsEmpty())
	assert.Equal(t, 20, items)

	enqueueItems(t, q, 2, 10)

	items = 0
	for {
		item := q.dequeue(0)
		if item == nil {
			break
		}
		items++
		t.Logf("Item: %d\n", item.node.Fee)
	}
	assert.True(t, q.IsEmpty())
	assert.Equal(t, 20, items)
}

func Test_queueLarge(t *testing.T) {
	runtime.GC()
	q := NewLockFreeQueue()

	enqueueItems(t, q, 10_000, 1_000)

	startTime := time.Now()
	items := 0
	for {
		item := q.dequeue(0)
		if item == nil {
			break
		}
		items++
	}
	t.Logf("Time empty %d items: %s\n", items, time.Since(startTime))
	t.Logf("Mem used for queue: %s\n", printAlloc())

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10_000_000, items)

	runtime.GC()

	enqueueItems(t, q, 10_000, 1_000)

	startTime = time.Now()
	items = 0
	for {
		item := q.dequeue(0)
		if item == nil {
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

func enqueueItems(t *testing.T, q *LockFreeQueue, threads, iter int) {
	startTime := time.Now()
	var wg sync.WaitGroup
	for n := 0; n < threads; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < iter; i++ {
				u := (n * iter) + i
				q.enqueue(&txIDAndFee{
					node: util.SubtreeNode{
						Hash:        chainhash.Hash{},
						Fee:         uint64(u),
						SizeInBytes: 0,
					},
				})
			}
		}(n)
	}
	wg.Wait()
	t.Logf("Time queue %d items: %s\n", threads*iter, time.Since(startTime))
}

func printAlloc() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return fmt.Sprintf("%d MB", m.Alloc/(1024*1024))
}

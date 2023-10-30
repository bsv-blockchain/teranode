package batcher

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
)

func Test_queue(t *testing.T) {
	q := NewLockFreeQueue()

	enqueueItems(t, q, 1, 10)

	items := 0
	for {
		item := q.dequeue()
		if item == nil {
			break
		}

		value, _ := binary.Uvarint(item.value)
		assert.Equal(t, uint64(items), value)
		items++
	}
	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10, items)

	enqueueItems(t, q, 1, 10)

	items = 0
	for {
		item := q.dequeue()
		if item == nil {
			break
		}
		value, _ := binary.Uvarint(item.value)
		assert.Equal(t, uint64(items), value)
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
		item := q.dequeue()
		if item == nil {
			break
		}
		items++
		//value, _ := binary.Uvarint(item.value)
		//t.Logf("Item: %d\n", value)
	}
	assert.True(t, q.IsEmpty())
	assert.Equal(t, 20, items)

	enqueueItems(t, q, 2, 10)

	items = 0
	for {
		item := q.dequeue()
		if item == nil {
			break
		}
		items++
		//value, _ := binary.Uvarint(item.value)
		//t.Logf("Item: %d\n", value)
	}
	assert.True(t, q.IsEmpty())
	assert.Equal(t, 20, items)
}

func Test_queueLarge(t *testing.T) {
	q := NewLockFreeQueue()

	enqueueItems(t, q, 10_000, 1_000)

	startTime := time.Now()
	items := 0
	for {
		item := q.dequeue()
		if item == nil {
			break
		}
		items++
	}
	t.Logf("Time empty %d items: %s\n", items, time.Since(startTime))
	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10_000_000, items)

	enqueueItems(t, q, 10_000, 1_000)

	startTime = time.Now()
	items = 0
	for {
		item := q.dequeue()
		if item == nil {
			break
		}
		items++
	}
	t.Logf("Time empty %d items: %s\n", items, time.Since(startTime))
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
				hash := make([]byte, 32)
				binary.PutUvarint(hash, uint64((n*threads)+u))
				value := make([]byte, 8)
				binary.PutUvarint(value, uint64(u))
				q.enqueue(&BatchItem{
					hash:  chainhash.Hash(hash),
					value: value,
				})
			}
		}(n)
	}
	wg.Wait()
	t.Logf("Time: %s\n", time.Since(startTime))
}

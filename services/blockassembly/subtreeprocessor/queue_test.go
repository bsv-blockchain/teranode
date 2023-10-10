package subtreeprocessor

import (
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/util"
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
		items++
		//t.Logf("Item: %d\n", item.fee)
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
		items++
		//t.Logf("Item: %d\n", item.fee)
	}
	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10, items)
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
				q.enqueue(&txIDAndFee{
					node: &util.SubtreeNode{
						Hash:        chainhash.Hash{},
						Fee:         uint64(u),
						SizeInBytes: 0,
					},
					waitCh: nil,
				})
			}
		}(n)
	}
	wg.Wait()
	t.Logf("Time: %s\n", time.Since(startTime))
}

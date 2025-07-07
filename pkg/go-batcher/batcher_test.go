package batcher

import (
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type batchStoreItem struct {
}

func TestNew(t *testing.T) {
	batchSize := 100
	batchTimeout := 100 * time.Millisecond
	sendStoreBatch := func(batch []*batchStoreItem) {
		// Simulate sending the batch
	}

	storeBatcher := New[batchStoreItem](batchSize, batchTimeout, sendStoreBatch, true)
	require.NotNil(t, storeBatcher)
	assert.Equal(t, storeBatcher.size, batchSize)
	assert.Equal(t, storeBatcher.timeout, batchTimeout)
}

func TestPut(t *testing.T) {
	batchSize := 100
	batchTimeout := 100 * time.Second

	countedItems := atomic.Int64{}
	sendStoreBatch := func(batch []*batchStoreItem) {
		countedItems.Add(int64(len(batch)))
	}

	storeBatcher := New[batchStoreItem](batchSize, batchTimeout, sendStoreBatch, true)

	for i := 0; i < 12; i++ {
		storeBatcher.Put(&batchStoreItem{})
	}

	time.Sleep(10 * time.Millisecond)

	storeBatcher.Trigger()

	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, int64(12), countedItems.Load())
}

func Benchmark_Batcher(b *testing.B) {
	b.Run("Benchmark_Batcher", func(b *testing.B) {
		batchSent := make(chan bool, 100)
		sendStoreBatch := func(batch []*batchStoreItem) {
			batchSent <- true
		}
		batchSize := 100
		storeBatcher := New[batchStoreItem](batchSize, 100, sendStoreBatch, true)

		expectedItems := 1_000_000
		expectedBatches := math.Ceil(float64(expectedItems) / float64(batchSize))
		g := errgroup.Group{}
		g.Go(func() error {
			for <-batchSent {
				expectedBatches--
				if expectedBatches == 0 {
					return nil
				}
			}

			return nil
		})

		for i := 0; i < expectedItems; i++ {
			storeBatcher.Put(&batchStoreItem{})
		}

		if err := g.Wait(); err != nil {
			b.Fatalf("Error in batcher: %v", err)
		}
	})
}

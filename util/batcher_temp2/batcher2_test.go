package batcher2

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func Test_Batcher2(t *testing.T) {
	t.Run("Test_Batcher2", func(t *testing.T) {

	})
}

type batchStoreItem struct {
	// TODO
}

func Benchmark_Batcher2(b *testing.B) {
	b.Run("Benchmark_Batcher2", func(b *testing.B) {
		batchSent := make(chan bool, 100)
		sendStoreBatch := func(batch []*batchStoreItem) {
			batchSent <- true
		}
		batchSize := 100

		storeBatcher := New[batchStoreItem](
			WithMaxItems[batchStoreItem](batchSize),
			WithTimeout[batchStoreItem](100*time.Millisecond),
			WithBatchFunc[batchStoreItem](sendStoreBatch),
			WithBackground[batchStoreItem](true),
		)

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

func Test_Batcher2MaxBytes(t *testing.T) {
	batchItemCount := make(chan int)

	sendStoreBatch := func(batch []*batchStoreItem) {
		batchItemCount <- len(batch)
	}

	batchSize := 100
	maxBytes := 2000

	storeBatcher := New[batchStoreItem](
		WithMaxItems[batchStoreItem](batchSize),
		WithMaxBytes[batchStoreItem](maxBytes),
		WithBatchFunc[batchStoreItem](sendStoreBatch),
		WithBackground[batchStoreItem](true),
	)

	go func() {
		storeBatcher.Put(&batchStoreItem{}, maxBytes+1) // Put an item with more bytes than the limit
	}()

	count := <-batchItemCount

	assert.Equal(t, 1, count)
}

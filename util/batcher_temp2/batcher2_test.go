package batcher2

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

type batchStoreItem struct{}

func Benchmark_Batcher2(b *testing.B) {
	b.Run("Benchmark_Batcher2", func(b *testing.B) {
		batchSent := make(chan bool, 100)
		sendStoreBatch := func(batch []*batchStoreItem) {
			batchSent <- true
		}
		batchSize := 100

		storeBatcher := New[batchStoreItem](
			sendStoreBatch,
			WithMaxItems[batchStoreItem](batchSize),
			WithTimeout[batchStoreItem](100*time.Millisecond),
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

func Test_Batcher2Defaults(t *testing.T) {
	batchItemCount := make(chan int)

	sendStoreBatch := func(batch []*batchStoreItem) {
		batchItemCount <- len(batch)
	}

	storeBatcher := New[batchStoreItem](
		sendStoreBatch,
	)

	go func() {
		storeBatcher.Put(&batchStoreItem{})
	}()

	count := <-batchItemCount

	assert.Equal(t, 1, count)
}

func Test_Batcher2With2Items(t *testing.T) {
	batchItemCount := make(chan int)

	sendStoreBatch := func(batch []*batchStoreItem) {
		batchItemCount <- len(batch)
	}

	storeBatcher := New[batchStoreItem](
		sendStoreBatch,
		WithMaxItems[batchStoreItem](3),
	)

	go func() {
		storeBatcher.Put(&batchStoreItem{})
		storeBatcher.Put(&batchStoreItem{})
		storeBatcher.Put(&batchStoreItem{})
	}()

	count := <-batchItemCount

	assert.Equal(t, 3, count)
}

func Test_Batcher2MaxBytes(t *testing.T) {
	batchItemCount := make(chan int)

	sendStoreBatch := func(batch []*batchStoreItem) {
		batchItemCount <- len(batch)
	}

	batchSize := 100
	maxBytes := 2000

	storeBatcher := New[batchStoreItem](
		sendStoreBatch,
		WithMaxItems[batchStoreItem](batchSize),
		WithMaxBytes[batchStoreItem](maxBytes),
		WithBackground[batchStoreItem](true),
	)

	go func() {
		storeBatcher.Put(&batchStoreItem{}, maxBytes+1) // Put an item with more bytes than the limit
	}()

	count := <-batchItemCount

	assert.Equal(t, 1, count)
}

func Test_Batcher2_Trigger(t *testing.T) {
	batchItemCount := make(chan int)

	sendStoreBatch := func(batch []*batchStoreItem) {
		batchItemCount <- len(batch)
	}

	batchSize := 50

	storeBatcher := New[batchStoreItem](
		sendStoreBatch,
		WithMaxItems[batchStoreItem](batchSize+1),
		WithBackground[batchStoreItem](true),
	)

	go func() {
		// Add items to the batch
		for i := 0; i < batchSize; i++ {
			storeBatcher.Put(&batchStoreItem{})
		}

		// We need to wait for the batcher to process these 50 items before we send the trigger.
		// This is because the trigger channel is in the same select block as the Put channel.
		time.Sleep(10 * time.Millisecond)

		// Trigger processing manually
		storeBatcher.Trigger()
	}()

	// We should have at least 1 batch processed immediately
	count := <-batchItemCount

	assert.Equal(t, batchSize, count, "Trigger did not force immediate batch processing")
}

package batcher

import (
	"math"
	"testing"

	"golang.org/x/sync/errgroup"
)

type batchStoreItem struct {
}

func Benchmark_Batcher2(b *testing.B) {
	b.Run("Benchmark_Batcher2", func(b *testing.B) {
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

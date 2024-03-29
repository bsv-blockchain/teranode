package main

import (
	"fmt"
	batcher "github.com/bitcoin-sv/ubsv/util/batcher_temp"
	"sync"
	"time"
)

type batchItem struct {
	value uint32
	done  chan error
}

func main() {
	batchSize := 100
	duration := 100 * time.Millisecond

	// create batcher
	sendBatch := func(batch []*batchItem) {
		// send batch
		uints := make([]uint32, len(batch))
		for i, item := range batch {
			uints[i] = item.value
		}
		// fmt.Println("Sending batch: ", uints)

		time.Sleep(10 * time.Millisecond)
		for _, item := range batch {
			if item.value == 333 {
				item.done <- fmt.Errorf("error for: %d", item.value)
				continue
			}
			item.done <- nil
		}
	}

	b := batcher.New[batchItem](batchSize, duration, sendBatch, false)

	// add items to batcher
	var wg sync.WaitGroup
	doneUints := make(map[uint32]struct{})
	doneUintsMu := sync.Mutex{}
	for i := uint32(0); i < 1000; i++ {
		wg.Add(1)
		// this simulates separate calls to the batcher
		// in the real world, this would be separate calls from a grpc go routine
		go func(i uint32) {
			defer wg.Done()

			done := make(chan error)
			b.Put(&batchItem{value: i, done: done})
			err := <-done
			if err != nil {
				fmt.Println("Error received: ", err)
			}

			doneUintsMu.Lock()
			doneUints[i] = struct{}{}
			doneUintsMu.Unlock()

			//fmt.Println("Received done signal for: ", i)
		}(i)
	}

	wg.Wait()

	fmt.Println("All done signals received: ", len(doneUints))
}

package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

func main() {
	ctx := context.Background()
	ch := make(chan int)

	subtree := util.NewTreeByLeafCount(1024 * 1024)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case n := <-ch:
				b := make([]byte, 8)
				binary.LittleEndian.PutUint64(b, uint64(n))
				hash, _ := chainhash.NewHash(b)
				_ = subtree.AddNode(*hash, uint64(n), uint64(n))
			}
		}
	}()

	startTime := time.Now()
	var wg sync.WaitGroup
	for n := 0; n < 1_000; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < 1_000; i++ {
				ch <- i * n
			}
		}(n)
	}
	wg.Wait()
	fmt.Printf("Time: %s\n", time.Since(startTime))
}

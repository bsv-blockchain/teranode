package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func Test_atomic_pointer(t *testing.T) {
	var p = atomic.Pointer[uint64]{}

	startTime := time.Now()
	var wg sync.WaitGroup
	for n := 0; n < 30_000; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < 1_000; i++ {
				u := uint64(i * n)
				p.Store(&u)
			}
		}(n)
	}
	wg.Wait()
	fmt.Printf("Time: %s\n", time.Since(startTime))
}

func Test_atomic_pointer_swap(t *testing.T) {
	var p = atomic.Pointer[uint64]{}

	startTime := time.Now()
	var wg sync.WaitGroup
	for n := 0; n < 30_000; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < 1_000; i++ {
				u := uint64(i * n)
				_ = p.Swap(&u)
			}
		}(n)
	}
	wg.Wait()
	fmt.Printf("Time: %s\n", time.Since(startTime))
}

func Test_main(t *testing.T) {
	ctx := context.Background()
	ch := make(chan int, 1000)

	subtree, err := util.NewTreeByLeafCount(1024 * 1024)
	require.NoError(t, err)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case n := <-ch:
				b := make([]byte, 32)
				binary.LittleEndian.PutUint64(b, uint64(n))
				hash := chainhash.Hash(b)
				_ = subtree.AddNode(hash, uint64(n), uint64(n))
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

func Test_main_batched(t *testing.T) {
	ctx := context.Background()
	ch := make(chan []int, 1000)

	subtree, err := util.NewTreeByLeafCount(1024 * 1024)
	require.NoError(t, err)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ns := <-ch:
				for _, n := range ns {
					b := make([]byte, 32)
					binary.LittleEndian.PutUint64(b, uint64(n))
					hash := chainhash.Hash(b)
					_ = subtree.AddNode(hash, uint64(n), uint64(n))
				}
			}
		}
	}()

	startTime := time.Now()
	var wg sync.WaitGroup
	for n := 0; n < 1_000; n++ {
		wg.Add(1)
		batch := make([]int, 0, 100)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < 1_000; i++ {
				batch = append(batch, i*n)
				if len(batch) == 100 {
					ch <- batch
					batch = make([]int, 0, 100)
				}
			}
			if len(batch) > 0 {
				ch <- batch
				batch = make([]int, 0, 100)
			}
		}(n)
	}
	wg.Wait()
	fmt.Printf("Time: %s\n", time.Since(startTime))
}

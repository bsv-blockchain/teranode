//go:build manual_tests

package main

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var maxTxsPerSecond = 5000_000
var minTxsPerSecond = 3000_000
var testDuration = 120
var adjustmentPeriod = 10 * time.Second
var initialTxBatchSize = 1024

func TestRateLimiter(t *testing.T) {
	r := NewRateLimiter(initialTxBatchSize)

	go func() {
		for {
			time.Sleep(adjustmentPeriod)
			r.AdjustTxBatchSize()
		}
	}()

	for i := 0; i < testDuration; i++ {

		go func() {
			numTransactions := rand.Intn(maxTxsPerSecond) + minTxsPerSecond + 1
			for j := 0; j < numTransactions; j++ {
				r.AddTransaction("tx")
			}
			r.mux.Lock()
			fmt.Printf("Second: %d, Transactions: %d, TxBatchSize: %d, TxBatches: %d\n", i+1, numTransactions, r.txBatchSize, len(r.txBatches))
			r.mux.Unlock()
		}()

		time.Sleep(time.Second)

	}
}

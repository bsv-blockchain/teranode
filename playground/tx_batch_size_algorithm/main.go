//go:build manual_tests

package main

import (
	"fmt"
	"math"
	"sync"
	"time"
)

var fuzziness = 0.25 // 25% fuzziness
var fuzzinessRatio = fuzziness / 2.0

type TxBatch struct {
	Transactions []string
}

type RateLimiter struct {
	txBatches      []*TxBatch
	currTxBatchIdx int
	txBatchSize    int
	lastAdjust     time.Time
	mux            sync.Mutex
}

func NewRateLimiter(initialTxBatchSize int) *RateLimiter {
	return &RateLimiter{
		txBatches:      make([]*TxBatch, 0),
		currTxBatchIdx: -1,
		txBatchSize:    initialTxBatchSize,
		lastAdjust:     time.Now(),
	}
}

func (r *RateLimiter) AddTransaction(tx string) {
	r.mux.Lock()
	defer r.mux.Unlock()
	if len(r.txBatches) == 0 || len(r.txBatches[r.currTxBatchIdx].Transactions) == r.txBatchSize {
		r.txBatches = append(r.txBatches, &TxBatch{make([]string, 0, r.txBatchSize)})
		r.currTxBatchIdx++
	}
	r.txBatches[r.currTxBatchIdx].Transactions = append(r.txBatches[r.currTxBatchIdx].Transactions, tx)
}

// Adjust the size of the batches. Called when a block is mined.
func (r *RateLimiter) AdjustTxBatchSize() {
	r.mux.Lock()
	defer r.mux.Unlock()
	elapsed := time.Since(r.lastAdjust).Seconds()
	rate := float64(len(r.txBatches)) / elapsed
	// fuzziness
	fmt.Printf("rate: %f\n", rate)
	fmt.Printf("fuzzinessRatio: %f\n", fuzzinessRatio)

	if rate-fuzzinessRatio > 1.0 {
		r.txBatchSize = int(math.Pow(2, math.Ceil(math.Log2(rate*float64(r.txBatchSize)))))
	} else if rate+fuzzinessRatio < 1.0 && r.txBatchSize > 10 {
		r.txBatchSize = int(math.Pow(2, math.Floor(math.Log2(rate*float64(r.txBatchSize)))))
		if r.txBatchSize < 10 {
			r.txBatchSize = 10
		}
	}

	r.lastAdjust = time.Now()
	r.txBatches = make([]*TxBatch, 0)
	r.currTxBatchIdx = -1
}

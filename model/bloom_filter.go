package model

import (
	"context"
	"sync"
	"time"

	"github.com/greatroar/blobloom"
	"github.com/libsv/go-bt/v2/chainhash"
)

type BlockBloomFilter struct {
	Filter       *blobloom.Filter
	BlockHash    *chainhash.Hash
	CreationTime time.Time
}

type BloomStats struct {
	QueryCounter         uint64
	PositiveCounter      uint64
	FalsePositiveCounter uint64
	mu                   sync.Mutex
}

func NewBloomStats() *BloomStats {
	return &BloomStats{
		QueryCounter:         0,
		PositiveCounter:      0,
		FalsePositiveCounter: 0,
	}
}

func (bs *BloomStats) BloomFilterStatsProcessor(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if prometheusBloomQueryCounter != nil {
					bs.mu.Lock()
					prometheusBloomQueryCounter.Set(float64(bs.QueryCounter))
					prometheusBloomPositiveCounter.Set(float64(bs.PositiveCounter))
					prometheusBloomFalsePositiveCounter.Set(float64(bs.FalsePositiveCounter))
					bs.mu.Unlock()
				}
			}
		}
	}()
}

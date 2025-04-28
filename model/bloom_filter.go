package model

import (
	"bytes"
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
	BlockHeight  uint32
}

func (bbf *BlockBloomFilter) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)
	_, err := blobloom.Dump(buf, bbf.Filter, "filter")

	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (bbf *BlockBloomFilter) Deserialize(data []byte) error {
	buf := bytes.NewBuffer(data)

	l, err := blobloom.NewLoader(buf)
	if err != nil {
		return err
	}

	bbf.Filter, err = l.Load(bbf.Filter)
	if err != nil {
		return err
	}

	return nil
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

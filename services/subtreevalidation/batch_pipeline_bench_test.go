package subtreevalidation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
)

// BenchmarkBatchPipeline contrasts the wall-clock of the old sequential
// per-batch shape (load then process, no overlap) with runLoadProcessPipeline
// for a range of LOAD/PROCESS latency mixes. LOAD models the read-only subtree
// fetch+decode; PROCESS models processTransactionsInLevels. The pipeline
// overlaps LOAD(N+1) with PROCESS(N), so its wall-clock approaches
// loadDelay + numBatches*max(load,process) instead of numBatches*(load+process).
//
// The injected latencies are a modelling choice; the deterministic win is that
// the inter-batch LOAD no longer serializes in front of PROCESS. Run with
// -benchtime=1x for a clean single-pass wall-clock comparison.
func BenchmarkBatchPipeline(b *testing.B) {
	const numBatches = 16

	mixes := []struct {
		name        string
		loadDelay   time.Duration
		processTime time.Duration
	}{
		{"load_lt_process", 2 * time.Millisecond, 6 * time.Millisecond},
		{"load_eq_process", 4 * time.Millisecond, 4 * time.Millisecond},
		{"load_gt_process", 6 * time.Millisecond, 2 * time.Millisecond},
	}

	for _, m := range mixes {
		load := func(_ context.Context, _ int) ([]*bt.Tx, []*bt.Arena, error) {
			time.Sleep(m.loadDelay)
			return nil, nil, nil
		}
		process := func(_ int, _ []*bt.Tx, _ []*bt.Arena) error {
			time.Sleep(m.processTime)
			return nil
		}
		noopRelease := func([]*bt.Arena) {}

		b.Run(fmt.Sprintf("%s/sequential", m.name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for batch := 0; batch < numBatches; batch++ {
					txs, arenas, _ := load(context.Background(), batch)
					_ = process(batch, txs, arenas)
					noopRelease(arenas)
				}
			}
		})

		b.Run(fmt.Sprintf("%s/pipelined", m.name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = runLoadProcessPipeline(context.Background(), numBatches, load, process, noopRelease)
			}
		})
	}
}

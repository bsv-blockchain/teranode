package aerospike_test

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// BenchmarkBatchPreviousOutputsDecorateDrainMode measures the concurrent
// BatchPreviousOutputsDecorate path (the #893 errgroup fan-out) with the
// outpoint batcher's drain mode OFF (default) vs ON, across a range of
// transaction counts.
//
// This exists to settle the #865 review question: Oli's concern is that
// flipping drain mode on the concurrent path "inverts the win" from #893 by
// shrinking the wide BatchOperate calls. Drain mode does not dispatch one
// outpoint per Put — it greedily drains whatever is queued (up to the size
// cap) and fires without waiting on the 10ms timer — so whether it actually
// narrows the per-batch record count under concurrency is timing-dependent
// and only a benchmark can decide it.
//
// Realism note: sendOutpointBatch de-dupes outpoints by parent txHash, so to
// make batch *width* map to real BatchOperate record counts every input
// references a DISTINCT parent tx. Both modes run the identical input through
// the identical code path; only UtxoStore.OutpointBatcherDrainMode differs.
//
// Run with:
//
//	go test -run '^$' -bench BenchmarkBatchPreviousOutputsDecorateDrainMode \
//	  -benchmem -benchtime 50x -tags aerospike ./stores/utxo/aerospike/
func BenchmarkBatchPreviousOutputsDecorateDrainMode(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	logger := ulogger.NewErrorTestLogger(b)

	// Tiers chosen to span the regimes: low concurrency (where drain mode is
	// meant to shine) up to the 2856-tx production block Oli's #893 commit
	// message cites.
	tiers := []int{16, 64, 256, 1024, 2856}
	maxTxs := tiers[len(tiers)-1]

	for _, drain := range []bool{false, true} {
		drain := drain

		tSettings := test.CreateBaseTestSettings(b)
		tSettings.UtxoStore.OutpointBatcherDrainMode = drain

		store, ctx, deferFn := initAerospikeBench(b, tSettings, logger)

		// Create a pool of distinct parent txs: two per child so every input
		// hits a different parent record. Setup only, not timed.
		parents := make([]*chainhash.Hash, maxTxs*2)
		for i := range parents {
			parent, err := bt.NewTxFromString(coinbaseTx.String())
			require.NoError(b, err)
			parent.Outputs[0].Satoshis += uint64(i) // unique txid

			_, err = store.Create(ctx, parent, 0)
			require.NoError(b, err)

			parents[i] = parent.TxIDChainHash()
		}

		for _, tier := range tiers {
			tier := tier

			// Build child txs: each spends vout 0 of two distinct parents.
			children := make([]*bt.Tx, tier)
			for j := 0; j < tier; j++ {
				child := &bt.Tx{}
				for _, p := range []*chainhash.Hash{parents[2*j], parents[2*j+1]} {
					in := &bt.Input{PreviousTxOutIndex: 0}
					_ = in.PreviousTxIDAdd(p)
					child.Inputs = append(child.Inputs, in)
				}
				children[j] = child
			}

			// Correctness gate: prove the path actually decorates before timing.
			require.NoError(b, store.BatchPreviousOutputsDecorate(ctx, children))
			for _, child := range children {
				for i, in := range child.Inputs {
					require.NotNil(b, in.PreviousTxScript, "input %d not decorated", i)
				}
			}

			// Warm up the connection pool / batcher workers for THIS tier so
			// the timed loop measures steady state, not cold-start. Cold pools
			// produced wildly bimodal first-tier numbers without this.
			for w := 0; w < 10; w++ {
				for _, child := range children {
					for _, in := range child.Inputs {
						in.PreviousTxScript = nil
					}
				}
				require.NoError(b, store.BatchPreviousOutputsDecorate(ctx, children))
			}

			b.Run(fmt.Sprintf("drain=%v/txs=%d", drain, tier), func(b *testing.B) {
				b.ReportAllocs()

				for n := 0; n < b.N; n++ {
					b.StopTimer()
					// Decorate skips inputs that already have a script; clear
					// them so each iteration does the full lookup work.
					for _, child := range children {
						for _, in := range child.Inputs {
							in.PreviousTxScript = nil
						}
					}
					b.StartTimer()

					require.NoError(b, store.BatchPreviousOutputsDecorate(ctx, children))
				}
			})
		}

		deferFn()
	}
}

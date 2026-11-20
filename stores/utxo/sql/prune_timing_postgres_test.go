package sql

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/stretchr/testify/require"
)

// seedPruneTimingStore stores one block's worth of prunable work and returns the number
// of transactions the prune is expected to delete.
//
//   - children: each spends one output of two different surviving parents (a parent
//     keeps an unspent output).
//   - pairs: a parent whose only output is spent by a child, both of them candidates.
//
// Every candidate is mined and stamped below the prune height.
func seedPruneTimingStore(ctx context.Context, t *testing.T, store *Store, run, children, pairs int) int {
	t.Helper()

	const address = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"

	newParent := func(label string, i, outputs int) *bt.Tx {
		tx := bt.NewTx()
		prev := chainhash.HashH([]byte(fmt.Sprintf("prune-timing-%d-%s-%d", run, label, i)))
		require.NoError(t, tx.From(prev.String(), 0, "51", 100000))
		tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})

		for o := 0; o < outputs; o++ {
			require.NoError(t, tx.PayToAddress(address, 4000))
		}

		return tx
	}

	var (
		mu         sync.Mutex
		all        []*chainhash.Hash
		candidates []*chainhash.Hash
	)

	record := func(tx *bt.Tx, candidate bool) {
		mu.Lock()
		defer mu.Unlock()

		all = append(all, tx.TxIDChainHash())

		if candidate {
			candidates = append(candidates, tx.TxIDChainHash())
		}
	}

	spendOf := func(inputs ...*bt.Tx) *bt.Tx {
		child := bt.NewTx()

		for _, p := range inputs {
			require.NoError(t, child.From(p.TxID(), 0, p.Outputs[0].LockingScript.String(), p.Outputs[0].Satoshis))
		}

		for i := range child.Inputs {
			child.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
		}

		require.NoError(t, child.PayToAddress(address, 3000))

		return child
	}

	work := make(chan func() error, children+pairs)

	for i := 0; i < children; i++ {
		i := i

		work <- func() error {
			a, b := newParent("a", i, 2), newParent("b", i, 2)

			for _, p := range []*bt.Tx{a, b} {
				if _, err := store.Create(ctx, p, 1000); err != nil {
					return err
				}

				record(p, false)
			}

			child := spendOf(a, b)

			if _, _, err := store.SpendAndCreate(ctx, child, 1000); err != nil {
				return err
			}

			record(child, true)

			return nil
		}
	}

	for i := 0; i < pairs; i++ {
		i := i

		work <- func() error {
			g := newParent("g", i, 1)

			if _, err := store.Create(ctx, g, 1000); err != nil {
				return err
			}

			h := spendOf(g)

			if _, _, err := store.SpendAndCreate(ctx, h, 1000); err != nil {
				return err
			}

			record(g, true)
			record(h, true)

			return nil
		}
	}

	close(work)

	errs := make(chan error, 8)

	var wg sync.WaitGroup

	for w := 0; w < 8; w++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for fn := range work {
				if err := fn(); err != nil {
					errs <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	_, err := store.SetMinedMulti(ctx, all, utxo.MinedBlockInfo{BlockID: uint32(1000 + run), BlockHeight: 1000, OnLongestChain: true})
	require.NoError(t, err)

	for _, h := range candidates {
		_, err := store.db.ExecContext(ctx, "UPDATE transactions SET delete_at_height = 1100 WHERE hash = $1", h[:])
		require.NoError(t, err)
	}

	return len(candidates)
}

// TestPruneTiming_Postgres times one prune of a block's worth of candidates: children
// of surviving parents, and parent and child pairs that are both deleted. It only
// measures. It runs when TERANODE_SQL_TIMING is set, with TERANODE_PRUNE_TIMING_CHILDREN,
// TERANODE_PRUNE_TIMING_PAIRS and TERANODE_PRUNE_TIMING_RUNS (default 3) to size it, and
// logs each run, the median and the best, every run over fresh rows.
func TestPruneTiming_Postgres(t *testing.T) {
	if os.Getenv("TERANODE_SQL_TIMING") == "" {
		t.Skip("set TERANODE_SQL_TIMING=1 to run the prune timing")
	}

	children, pairs := 2000, 500

	if v := os.Getenv("TERANODE_PRUNE_TIMING_CHILDREN"); v != "" {
		_, err := fmt.Sscan(v, &children)
		require.NoError(t, err)
	}

	if v := os.Getenv("TERANODE_PRUNE_TIMING_PAIRS"); v != "" {
		_, err := fmt.Sscan(v, &pairs)
		require.NoError(t, err)
	}

	runs := 3

	if v := os.Getenv("TERANODE_PRUNE_TIMING_RUNS"); v != "" {
		_, err := fmt.Sscan(v, &runs)
		require.NoError(t, err)
	}

	store, ctx := setupPostgresStore(t)
	store.settings.Pruner.UTXODefensiveEnabled = false

	best := time.Duration(1<<63 - 1)
	timings := make([]time.Duration, 0, runs)

	for run := 0; run < runs; run++ {
		want := seedPruneTimingStore(ctx, t, store, run, children, pairs)

		ResetPrunerServiceForTests()

		provider, ok := any(store).(pruner.PrunerServiceProvider)
		require.True(t, ok)

		svc, err := provider.GetPrunerService()
		require.NoError(t, err)

		svc.Start(ctx)

		require.NoError(t, store.SetBlockHeight(1300))

		start := time.Now()
		deleted, err := svc.Prune(ctx, 1300, "prune-timing")
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.EqualValues(t, want, deleted)

		t.Logf("run %d: pruned %d candidates (%d children, %d pairs) in %s", run, deleted, children, pairs, elapsed)

		timings = append(timings, elapsed)

		if elapsed < best {
			best = elapsed
		}
	}

	sort.Slice(timings, func(i, j int) bool { return timings[i] < timings[j] })

	t.Logf("prune of %d children and %d pairs over %d runs: median %s, best %s", children, pairs, runs, timings[len(timings)/2], best)
}

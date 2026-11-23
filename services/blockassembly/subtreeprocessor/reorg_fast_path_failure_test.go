package subtreeprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// Issue 2269, plan PR 6: once the fast path has removed the lost block's coinbase there is no rollback,
// only a reset. The reset sweeps the installed subtrees for unmined transactions, so whatever is not in
// them at the moment of a failure must be swept some other way: effective D1 is flagged mined in the
// store from the coinbase removal until it is in the installed subtrees.

// requireResetRecoversEveryUnminedTx resets after a failed fast path and requires the sweep to have
// marked every transaction that is unmined after the reorg (the pending ones the winners did not mine
// and the lost ones no winner has) as not on the longest chain, which is what lets the reload find them.
func requireResetRecoversEveryUnminedTx(t *testing.T, sc *reorgScenario) {
	t.Helper()

	require.True(t, sc.stp.failedForkReorgNeedsReset.Load(), "a failed fork reorg waits for a reset")

	require.NoError(t, sc.stp.reset(sc.genesis, []*model.Block{sc.lost}, sc.forward, false, func() error { return nil }))

	swept := make(map[chainhash.Hash]struct{})
	for _, h := range sc.utxo.marked(false) {
		swept[h] = struct{}{}
	}

	for _, h := range sc.expectedOrderAfter() {
		require.Contains(t, swept, h, "unmined transaction %s is swept", h)
	}

	for _, h := range sc.rawD1() {
		require.Contains(t, swept, h, "lost transaction %s no winner has is swept", h)
	}

	require.False(t, sc.stp.failedForkReorgNeedsReset.Load())
	require.Empty(t, sc.stp.reorgPendingD1, "the reset that swept the record clears it")
}

// A failure between the coinbase removal and the end of the build: the old subtrees are put back and
// hold the pending set, and effective D1 is in no subtree at all.
func TestReorgScenario_FastPathFailureBeforeTheBuildEndsInAResetThatSweepsEffectiveD1(t *testing.T) {
	first := fastPathFirstBeat(t, smallScenarioConfig())

	for _, c := range []struct {
		name string
		beat int
	}{{"after the coinbase removal", first}, {"after the D1 insert", first + 1}} {
		t.Run(c.name, func(t *testing.T) {
			sc := newReorgScenario(t, smallScenarioConfig())
			sc.stp.settings.BlockAssembly.ReorgFastPath = true

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			beats := 0

			sc.stp.SetProgressHook(func() {
				if beats++; beats == c.beat {
					cancel()
				}
			})

			err := sc.stp.reorgBlocks(ctx, []*model.Block{sc.lost}, sc.forward)

			require.Error(t, err)
			require.NotEmpty(t, sc.stp.reorgPendingD1, "effective D1 is recorded for the reset")
			require.Equal(t, sc.pendingOrder(), txHashes(assemblyNodes(sc.stp)), "the pending set is back in the subtrees")
			require.Empty(t, sc.utxo.marked(true))

			requireResetRecoversEveryUnminedTx(t, sc)
		})
	}
}

// A failure after the build, in the awaited stores before the marks (PR 2b): effective D1 is installed,
// nothing is marked, and the reset recovers everything.
func TestReorgScenario_FastPathFailedStoreEndsInAResetThatSweepsTheInstalledSubtrees(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.reorgStoreDeadline = 10 * time.Second
	cfg.storeConsumer = failAwaited

	sc := newReorgScenario(t, cfg)

	var rec fastPathRun

	err := recordingFastPath(&rec)(t, sc)

	require.Error(t, err)
	require.True(t, rec.applied, "the failure is after the fast path applied")
	require.Empty(t, sc.stp.reorgPendingD1, "effective D1 is installed, nothing to record")
	require.Empty(t, sc.utxo.marked(true))
	require.Empty(t, sc.utxo.marked(false), "nothing is marked before the stores are stored")

	requireResetRecoversEveryUnminedTx(t, sc)
}

// Failures inside the in-place delete and the drain can only come from the map or the store; the
// reorg returns them and the reset recovers, as above. A failing coinbase application of a winning block
// is the one a test can reach: the first winning block's coinbase is already in the store.
func TestReorgScenario_FastPathFailedCoinbaseApplicationEndsInAResetThatSweepsTheInstalledSubtrees(t *testing.T) {
	first := fastPathFirstBeat(t, smallScenarioConfig())

	sc := newReorgScenario(t, smallScenarioConfig())
	sc.stp.settings.BlockAssembly.ReorgFastPath = true

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	beats := 0

	sc.stp.SetProgressHook(func() {
		// the beat before the first winning block's coinbase is applied
		if beats++; beats == first+3 {
			cancel()
		}
	})

	err := sc.stp.reorgBlocks(ctx, []*model.Block{sc.lost}, sc.forward)
	if err == nil {
		t.Skip("the store ignored the cancelled context; the failure cannot be injected this way")
	}

	require.Empty(t, sc.stp.reorgPendingD1)
	requireResetRecoversEveryUnminedTx(t, sc)
}

// Map writes of the fast path: effective D1 and the drained arrivals are the only inserts, and the delete
// set (the hits) is the only set of deletes, whatever the size of the pending set. The slow path inserts
// every survivor into a fresh map, which grows with the pending set.
func TestReorgScenario_FastPathMapWritesDoNotGrowWithThePendingSet(t *testing.T) {
	const (
		hits     = 300
		arrivals = 40
	)

	type counted struct {
		inserted, setIfNotExists, set, deleted int64
		effectiveD1                            int
	}

	run := func(t *testing.T, pending int) counted {
		cfg := smallScenarioConfig()
		cfg.pending = pending
		cfg.pendingInWinners = float64(hits) / float64(pending)
		cfg.hitsAtFront = true
		cfg.pendingChains, cfg.removeMapPending = 0, 20
		cfg.subtreeSize = 1024

		sc := newReorgScenario(t, cfg)
		sc.stp.settings.BlockAssembly.ReorgFastPath = true
		require.Zero(t, sc.stp.settings.BlockAssembly.DoubleSpendWindow)

		counting := newCountingTxMap(sc.stp.currentTxMap)
		sc.stp.currentTxMap = counting

		// arrivals queued before the reorg are taken by the head drain
		enqueueAt := time.Now()
		sc.stp.queue.clock = fixedClock{t: enqueueAt}
		sc.stp.clock = fixedClock{t: enqueueAt.Add(time.Millisecond)}

		nodes := make([]subtreepkg.Node, arrivals)
		inpoints := make([]*subtreepkg.TxInpoints, arrivals)

		for k := range arrivals {
			id := kindIntake | scenarioID(k)
			p := voutZeroInpoints(sc.confirmedParent(id))
			nodes[k], inpoints[k] = subtreepkg.Node{Hash: sc.hash(id), Fee: 1, SizeInBytes: 250}, &p
		}

		sc.stp.AddBatch(nodes, inpoints)

		var rec fastPathRun

		require.NoError(t, recordingFastPath(&rec)(t, sc))
		require.True(t, rec.applied, "declined: %v", rec.reasons)

		c := counting.counts()

		effective := 0

		for _, h := range sc.rawD1() {
			if !sc.inPending(h) {
				effective++
			}
		}

		return counted{c.inserted, c.setIfNotExists, c.set, c.delete, effective}
	}

	small, large := run(t, 4_000), run(t, 16_000)

	for _, c := range []counted{small, large} {
		require.Equal(t, int64(c.effectiveD1+arrivals), c.inserted, "effective D1 plus the drained arrivals")
		require.Equal(t, c.inserted, c.setIfNotExists, "nothing is inserted twice")
		require.Zero(t, c.set)
		require.Equal(t, int64(hits+20), c.deleted, "the hits and the removeMap entries, once each")
	}

	require.Equal(t, small.inserted, large.inserted)
	require.Equal(t, small.deleted, large.deleted)
}

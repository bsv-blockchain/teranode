package subtreeprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
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

	require.Error(t, sc.stp.reorgBlocks(ctx, []*model.Block{sc.lost}, sc.forward), "the cancelled context fails the coinbase application")
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

// The awaited stores of PR 2b hold for the fast path too: its subtrees are stored before the first
// longest-chain mark and announced after it, and a failed store fails the reorg before any mark.
func TestReorgScenario_FastPathStoresBeforeTheMarksAndAnnouncesAfter(t *testing.T) {
	sc, r, _ := newAwaitedStoreScenario(t, 5*time.Second, answerStored)

	var rec fastPathRun

	require.NoError(t, recordingFastPath(&rec)(t, sc))
	require.True(t, rec.applied, "declined: %v", rec.reasons)

	r.mu.Lock()
	defer r.mu.Unlock()

	require.NotEmpty(t, r.awaited, "the reorg waits for the stores of the subtrees it built")

	for _, marks := range r.awaited {
		require.Zero(t, marks, "every awaited store is sent before the first longest-chain mark")
	}

	require.NotEmpty(t, r.announced)

	for _, marks := range r.announced {
		require.Positive(t, marks, "nothing is announced before the marks")
	}
}

func TestReorgScenario_FastPathFailedStoreFailsTheReorgBeforeTheMarks(t *testing.T) {
	sc, r, logger := newAwaitedStoreScenario(t, 5*time.Second, failAwaited)

	var rec fastPathRun

	require.Error(t, recordingFastPath(&rec)(t, sc))
	require.True(t, rec.applied, "declined: %v", rec.reasons)

	require.Zero(t, sc.utxo.markCount(true)+sc.utxo.markCount(false), "no longest-chain mark is written")
	require.True(t, sc.stp.failedForkReorgNeedsReset.Load(), "only a reset may follow")
	require.NotContains(t, logger.phases("reorgBlocks"), "processed_at", "no forward block is marked processed")

	r.mu.Lock()
	defer r.mu.Unlock()

	require.Empty(t, r.announced, "nothing is announced")
}

// Design section 3.2: every node of the installed subtrees has an entry in the tx map at every instant.
// The meta of a subtree that holds an effective D1 transaction reads its inpoints from the map, and a
// reset or a removal between two steps would find the node without one.
func TestReorgScenario_FastPathEveryInstalledNodeIsInTheMapAtEveryBeat(t *testing.T) {
	for _, disk := range []bool{false, true} {
		cfg := smallScenarioConfig()
		cfg.diskTxMap = disk

		sc := newReorgScenario(t, cfg)
		sc.stp.settings.BlockAssembly.ReorgFastPath = true

		beats, checked := 0, 0

		sc.stp.SetProgressHook(func() {
			beats++

			if sc.stp.reorgPendingD1 == nil && checked == 0 {
				return // before the fast path
			}

			missing := 0

			for _, node := range assemblyNodes(sc.stp) {
				if node.Hash != subtreepkg.CoinbasePlaceholderHashValue && !sc.stp.currentTxMap.Exists(node.Hash) {
					missing++
				}
			}

			require.Zero(t, missing, "beat %d: installed nodes without a tx map entry", beats)

			checked++
		})

		require.NoError(t, sc.run(t))
		require.GreaterOrEqual(t, checked, 4, "the fast path beats at least four times")
	}
}

// coinbaseDeleteStore runs onDelete when the coinbase of the lost block is deleted, the last step of
// removeCoinbaseUtxos, and returns what it returns.
type coinbaseDeleteStore struct {
	utxo.Store

	onDelete func() error
}

func (s *coinbaseDeleteStore) Delete(ctx context.Context, hash *chainhash.Hash) error {
	if err := s.onDelete(); err != nil {
		return err
	}

	return s.Store.Delete(ctx, hash)
}

// The coinbase removal is the first change the fast path makes to the store, and effective D1 is flagged
// mined from there on, in no installed subtree: a failure inside the removal, or the length check right
// after it, must still leave effective D1 recorded for the reset.
func TestReorgScenario_FastPathFailureInsideTheCoinbaseRemovalEndsInAResetThatSweepsEffectiveD1(t *testing.T) {
	for name, inject := range map[string]func(sc *reorgScenario) error{
		"the removal fails": func(*reorgScenario) error { return errors.NewStorageError("injected coinbase delete failure") },
		"the removal takes a transaction out of the assembly": func(sc *reorgScenario) error {
			sc.stp.currentTxMap.Delete(sc.pendingOrder()[0])

			return nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			sc := newReorgScenario(t, smallScenarioConfig())
			real := sc.stp.utxoStore
			failing := true

			sc.stp.utxoStore = &coinbaseDeleteStore{Store: real, onDelete: func() error {
				if failing {
					return inject(sc)
				}

				return nil
			}}

			require.Error(t, recordingFastPath(&fastPathRun{})(t, sc))
			require.NotEmpty(t, sc.stp.reorgPendingD1, "effective D1 is recorded before the coinbase is touched")

			failing = false

			requireResetRecoversEveryUnminedTx(t, sc)
		})
	}
}

// Design section 5 item 10: a removal accepted after the flag pass is not in the flags. The first
// template may contain the transaction, as it can after any Remove; the processor applies the removal
// when it resumes its removal channel.
func TestReorgScenario_FastPathRemovalAcceptedAfterTheFlagPassIsAppliedOnTheNextDequeue(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())
	sc.stp.settings.BlockAssembly.ReorgFastPath = true

	var removed chainhash.Hash

	for _, h := range sc.pendingOrder() {
		if !sc.inWinners(h) && !sc.inLost(h) {
			removed = h

			break
		}
	}

	require.NotZero(t, removed)

	fired := false

	sc.stp.SetProgressHook(func() {
		// the first beat of the fast path is after the flag pass
		if !fired && sc.stp.reorgPendingD1 != nil {
			fired = true

			require.NoError(t, sc.stp.Remove(t.Context(), removed))
		}
	})

	var rec fastPathRun

	require.NoError(t, recordingFastPath(&rec)(t, sc))
	require.True(t, rec.applied, "declined: %v", rec.reasons)
	require.True(t, fired)

	require.Contains(t, txHashes(assemblyNodes(sc.stp)), removed, "the first template may hold the transaction")

	select {
	case h := <-sc.stp.removeTxCh:
		require.Equal(t, removed, h)
		require.NoError(t, sc.stp.removeTxFromSubtrees(t.Context(), h))
	case <-time.After(5 * time.Second):
		t.Fatal("the removal reached no channel")
	}

	require.NotContains(t, txHashes(assemblyNodes(sc.stp)), removed)
	require.False(t, sc.stp.currentTxMap.Exists(removed))
	requireAssemblyValid(t, sc.stp, sc.onChainAfter)
}

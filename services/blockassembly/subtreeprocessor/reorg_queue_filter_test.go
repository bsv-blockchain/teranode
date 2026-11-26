package subtreeprocessor

import (
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// A fork reorg drains the queue before it moves back, so what it filters
// against the forward blocks afterwards is what arrived during the reorg. A
// batch that arrives before a forward block's step must be filtered against
// that block, not only against the last one: otherwise a tx the earlier block
// mined goes back into block assembly and the node mines it again.
func TestReorgBlocks_Fork_FiltersQueueArrivalsAgainstEachForwardBlock(t *testing.T) {
	for name, opts := range catchUpTxMapVariants(t) {
		t.Run(name, func(t *testing.T) {
			forkReorgFiltersQueueArrivals(t, opts...)
		})
	}
}

func forkReorgFiltersQueueArrivals(t *testing.T, opts ...Options) {
	stp, store, subtreeStore, chain := newResetWALTestProcessor(t, false, opts...)

	minedInFirstTx, _ := createResetWALConflict(t, store, 21)
	minedInLastTx, _ := createResetWALConflict(t, store, 22)
	minedInFirst, minedInLast := *minedInFirstTx.TxIDChainHash(), *minedInLastTx.TxIDChainHash()
	unmined := chainhash.HashH([]byte("fork-reorg-queued-not-mined"))

	back := moveToBlockAbove(t, stp, chain)
	chain.moveForward[0].Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, minedInFirstTx)}
	chain.moveForward[1].Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, minedInLastTx)}

	// Stamp the batch a millisecond before any drain, so every drain after it
	// is unambiguously free to take it.
	enqueueAt := time.Now()
	stp.queue.clock = fixedClock{t: enqueueAt}
	stp.clock = fixedClock{t: enqueueAt.Add(time.Millisecond)}

	// The move back has ended and the first forward block is next: the drain
	// before the move back has run, so only a forward step's drain sees this.
	fired := beforeForwardBlockOn(stp, chain.genesis, func() {
		stp.AddBatch(
			[]subtreepkg.Node{
				{Hash: minedInFirst, Fee: 1, SizeInBytes: 1},
				{Hash: minedInLast, Fee: 1, SizeInBytes: 1},
				{Hash: unmined, Fee: 1, SizeInBytes: 1},
			},
			[]*subtreepkg.TxInpoints{inpointsPtr(), inpointsPtr(), inpointsPtr()},
		)
	})

	require.NoError(t, stp.reorgBlocks(t.Context(), []*model.Block{back}, chain.moveForward))
	require.True(t, *fired, "the batch must arrive after the move back and before the first forward block")

	require.Equal(t, chain.moveForward[1].Header.Hash(), stp.GetCurrentBlockHeader().Hash())
	require.Zero(t, stp.queue.length(), "the reorg must drain what arrived during it")

	hashes := collectSubtreeHashes(stp)
	require.NotContains(t, hashes, minedInFirst, "a queued tx the first forward block mined must not re-enter block assembly")
	require.False(t, stp.currentTxMap.Exists(minedInFirst), "nor stay in the tx map")
	require.NotContains(t, hashes, minedInLast, "a queued tx the last forward block mined must not re-enter block assembly")
	require.Contains(t, hashes, unmined, "a queued tx no block mined must be kept")
	require.Equal(t, uint64(len(hashes)), stp.txCount.Load(), "the transaction count must equal what the subtrees hold") // nolint:gosec
}

// A batch that reaches the queue in the same millisecond as forward block A's
// drain must be filtered against block A. The old exclusive cutoff held it
// back, and block B's step then drained it against block B alone and brought
// back a tx that block A mined.
func TestReorgBlocks_Fork_SameMillisecondBatchIsFilteredAgainstItsForwardBlock(t *testing.T) {
	cases := map[string]struct {
		window     time.Duration
		firstDrain time.Duration // the drain clock at block A's step, after the enqueue
		laterDrain time.Duration // the drain clock at block B's step
	}{
		"same_millisecond_at_zero_window": {window: 0, firstDrain: 0, laterDrain: time.Millisecond},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			stp, store, subtreeStore, chain := newResetWALTestProcessor(t, false)
			stp.settings.BlockAssembly.DoubleSpendWindow = tc.window

			minedInFirstTx, _ := createResetWALConflict(t, store, 23)
			minedInLastTx, _ := createResetWALConflict(t, store, 24)
			minedInFirst := *minedInFirstTx.TxIDChainHash()
			unmined := chainhash.HashH([]byte("fork-reorg-same-ms-not-mined"))

			back := moveToBlockAbove(t, stp, chain)
			chain.moveForward[0].Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, minedInFirstTx)}
			chain.moveForward[1].Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, minedInLastTx)}

			enqueueAt := time.Now()
			stp.queue.clock = fixedClock{t: enqueueAt}

			// Before block A's step the batch arrives, at a drain clock in the same
			// millisecond, which the old exclusive cutoff held back. Before block B's
			// step the clock moves on, so B's drain would take it if A's had not.
			var enqueued, advanced bool

			stp.SetProgressHook(func() {
				switch {
				case !enqueued && stp.currentBlockHeader.Load().Hash().IsEqual(chain.genesis.Hash()):
					enqueued = true
					stp.clock = fixedClock{t: enqueueAt.Add(tc.firstDrain)}
					stp.AddBatch(
						[]subtreepkg.Node{{Hash: minedInFirst, Fee: 1, SizeInBytes: 1}, {Hash: unmined, Fee: 1, SizeInBytes: 1}},
						[]*subtreepkg.TxInpoints{inpointsPtr(), inpointsPtr()},
					)
				case enqueued && !advanced && stp.currentBlockHeader.Load().Hash().IsEqual(chain.moveForward[0].Header.Hash()):
					advanced = true
					stp.clock = fixedClock{t: enqueueAt.Add(tc.laterDrain)}
				}
			})

			require.NoError(t, stp.reorgBlocks(t.Context(), []*model.Block{back}, chain.moveForward))
			require.True(t, enqueued && advanced, "the batch must arrive before block A's step and the clock move before block B's")

			hashes := collectSubtreeHashes(stp)
			require.NotContains(t, hashes, minedInFirst, "a same-millisecond tx the first forward block mined must not re-enter block assembly")
			require.Contains(t, hashes, unmined, "a same-millisecond tx no block mined must be kept")
		})
	}
}

// A transaction handed to block assembly after its own forward block's step, and before a later
// forward block's step, is dropped on arrival by the earlier block's kept set. B's drain alone only
// knows B, so without the kept set the transaction A mined goes back into block assembly. With a
// double-spend window the batch arrives before A's step instead, the window holds it through both
// drains, and the kept set must still drop it when the dequeue loop places it.
func TestReorgBlocks_Fork_LateArrivalAfterItsForwardBlockStaysOut(t *testing.T) {
	cases := map[string]struct {
		window, retention time.Duration
		reEnters          bool
	}{
		"kept_set_drops_it":                       {retention: 10 * time.Second},
		"retention_off_lets_it_back":              {retention: 0, reEnters: true},
		"held_by_window_below_retention":          {window: 2 * time.Second, retention: 10 * time.Second},
		"held_by_window_above_retention":          {window: 15 * time.Second, retention: 10 * time.Second},
		"held_by_window_retention_off_lets_it_in": {window: 2 * time.Second, retention: 0, reEnters: true},
	}

	for variant, opts := range catchUpTxMapVariants(t) {
		for name, tc := range cases {
			t.Run(variant+"/"+name, func(t *testing.T) {
				stp, store, subtreeStore, chain := newResetWALTestProcessor(t, false, opts...)
				stp.settings.BlockAssembly.DoubleSpendWindow = tc.window
				stp.settings.BlockAssembly.MinedTxSetRetention = tc.retention

				minedInFirstTx, _ := createResetWALConflict(t, store, 25)
				minedInLastTx, _ := createResetWALConflict(t, store, 26)
				minedInFirst := *minedInFirstTx.TxIDChainHash()
				unmined := chainhash.HashH([]byte("fork-reorg-late-not-mined"))

				back := moveToBlockAbove(t, stp, chain)
				chain.moveForward[0].Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, minedInFirstTx)}
				chain.moveForward[1].Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, minedInLastTx)}

				enqueueAt := time.Now()

				var nowNanos atomic.Int64
				nowNanos.Store(enqueueAt.Add(time.Millisecond).UnixNano())
				stp.clock = clockFunc(func() time.Time { return time.Unix(0, nowNanos.Load()) })
				stp.queue.clock = fixedClock{t: enqueueAt}

				// Without a window: block A's step, and its drain, are done; block B's is next.
				// With one: the move back is done; block A's step is next.
				arriveBefore := chain.moveForward[0].Header
				if tc.window > 0 {
					arriveBefore = chain.genesis
				}

				fired := beforeForwardBlockOn(stp, arriveBefore, func() {
					stp.AddBatch(
						[]subtreepkg.Node{{Hash: minedInFirst, Fee: 1, SizeInBytes: 1}, {Hash: unmined, Fee: 1, SizeInBytes: 1}},
						[]*subtreepkg.TxInpoints{inpointsPtr(), inpointsPtr()},
					)
				})

				require.NoError(t, stp.reorgBlocks(t.Context(), []*model.Block{back}, chain.moveForward))
				require.True(t, *fired, "the batch must arrive before the step it is meant to precede")

				if tc.window > 0 {
					require.Equal(t, int64(2), stp.queue.length(), "precondition: the forward drains leave the batch inside the window")

					stp.Start(t.Context())
					// Past the window and past the retention alone, so only the window's share of the set's life keeps the mined transaction out.
					nowNanos.Store(enqueueAt.Add(max(tc.window, tc.retention) + time.Second).UnixNano())

					require.Eventually(t, func() bool {
						return stp.queue.length() == 0 && stp.GetCurrentTxMap().Exists(unmined)
					}, 5*time.Second, time.Millisecond, "the dequeue loop must place the unmined transaction")
					require.Equal(t, tc.reEnters, stp.GetCurrentTxMap().Exists(minedInFirst))

					return
				}

				require.Zero(t, stp.queue.length(), "the last step's drain takes the late batch")

				hashes := collectSubtreeHashes(stp)
				require.Equal(t, tc.reEnters, slices.Contains(hashes, minedInFirst))
				require.Equal(t, tc.reEnters, stp.currentTxMap.Exists(minedInFirst))
				require.Contains(t, hashes, unmined, "a late transaction no block mined must be kept")
				require.Equal(t, uint64(len(hashes)), stp.txCount.Load(), "the transaction count must equal what the subtrees hold") // nolint:gosec
			})
		}
	}
}

// A reorg forgets a moved-back block's set, so a late hand-off of a transaction that block mined,
// and no forward block mines, is placed. A late hand-off of a forward block's transaction stays out
// even when the next step takes longer than the retention, and the expired set is dropped when the
// next block's set is kept.
func TestReorgBlocks_Fork_ForgetsMovedBackAndChecksLateHandOffOnArrival(t *testing.T) {
	stp, store, subtreeStore, chain := newResetWALTestProcessor(t, false)
	stp.settings.BlockAssembly.MinedTxSetRetention = 10 * time.Second

	firstTx, _ := createResetWALConflict(t, store, 27)
	lastTx, _ := createResetWALConflict(t, store, 28)
	minedInFirst := *firstTx.TxIDChainHash()
	onlyInBack := chainhash.HashH([]byte("mined-only-by-the-moved-back-block"))

	back := moveToBlockAbove(t, stp, chain)
	// As if block assembly had moved forward onto the block it now moves back.
	stp.keepMinedSet(back, frozenHashSet(t, onlyInBack))

	chain.moveForward[0].Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, firstTx)}
	chain.moveForward[1].Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, lastTx)}

	now := time.Now()
	stp.queue.clock = fixedClock{t: now}
	stp.clock = clockFunc(func() time.Time { return now.Add(time.Millisecond) })

	var firedBack, firedFirst bool

	stp.SetProgressHook(func() {
		tip := stp.currentBlockHeader.Load().Hash()

		switch {
		case !firedBack && tip.IsEqual(chain.genesis.Hash()):
			firedBack = true

			stp.AddBatch([]subtreepkg.Node{{Hash: onlyInBack, Fee: 1, SizeInBytes: 1}}, []*subtreepkg.TxInpoints{inpointsPtr()})
		case !firedFirst && tip.IsEqual(chain.moveForward[0].Header.Hash()):
			firedFirst = true

			stp.AddBatch([]subtreepkg.Node{{Hash: minedInFirst, Fee: 1, SizeInBytes: 1}}, []*subtreepkg.TxInpoints{inpointsPtr()})

			// The next step takes an hour, far past the retention.
			now = now.Add(time.Hour)
		}
	})

	require.NoError(t, stp.reorgBlocks(t.Context(), []*model.Block{back}, chain.moveForward))
	require.True(t, firedBack && firedFirst)

	hashes := collectSubtreeHashes(stp)
	require.Contains(t, hashes, onlyInBack, "the moved-back block's set must not keep its transaction out")
	require.NotContains(t, hashes, minedInFirst, "a late hand-off of the first forward block's transaction stays out")
	require.Equal(t, 1, stp.keptMinedSetCount(), "a set past its retention is dropped when the next block's set is kept")

	now = now.Add(10 * time.Second)
	stp.pruneMinedSets()
	require.Zero(t, stp.keptMinedSetCount())
}

// A catch-up drains only at its last block. A late hand-off of an earlier block's transaction that
// arrives between the steps stays out, even when the last step takes longer than the retention.
func TestReorgBlocks_CatchUp_LateHandOffIsCheckedOnArrival(t *testing.T) {
	cases := map[string]struct {
		retention time.Duration
		reEnters  bool
	}{
		"kept_set_drops_it":          {retention: 10 * time.Second},
		"retention_off_lets_it_back": {retention: 0, reEnters: true},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			stp, store, subtreeStore, chain := newResetWALTestProcessor(t, false)
			stp.settings.BlockAssembly.MinedTxSetRetention = tc.retention

			firstTx, _ := createResetWALConflict(t, store, 29)
			lastTx, _ := createResetWALConflict(t, store, 30)
			minedInFirst := *firstTx.TxIDChainHash()
			unmined := chainhash.HashH([]byte("catch-up-late-not-mined"))

			chain.moveForward[0].Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, firstTx)}
			chain.moveForward[1].Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, lastTx)}

			now := time.Now()
			stp.queue.clock = fixedClock{t: now}
			stp.clock = clockFunc(func() time.Time { return now.Add(time.Millisecond) })

			fired := beforeForwardBlockOn(stp, chain.moveForward[0].Header, func() {
				stp.AddBatch(
					[]subtreepkg.Node{{Hash: minedInFirst, Fee: 1, SizeInBytes: 1}, {Hash: unmined, Fee: 1, SizeInBytes: 1}},
					[]*subtreepkg.TxInpoints{inpointsPtr(), inpointsPtr()},
				)

				// The last block takes an hour, far past the retention.
				now = now.Add(time.Hour)
			})

			require.NoError(t, stp.reorgBlocks(t.Context(), []*model.Block{}, chain.moveForward))
			require.True(t, *fired)
			require.Equal(t, chain.moveForward[1].Header.Hash(), stp.GetCurrentBlockHeader().Hash())
			require.Zero(t, stp.queue.length(), "the last block's drain takes the late batch")

			hashes := collectSubtreeHashes(stp)
			require.Equal(t, tc.reEnters, slices.Contains(hashes, minedInFirst))
			require.Contains(t, hashes, unmined)
		})
	}
}

// A catch-up that fails part-way rolls back the blocks it applied and forgets their kept sets, so a
// late hand-off of a transaction only those blocks mined is queued again.
func TestReorgBlocks_CatchUp_FailedRollbackForgetsItsSets(t *testing.T) {
	stp, store, subtreeStore, chain := newResetWALTestProcessor(t, false)
	stp.settings.BlockAssembly.MinedTxSetRetention = 10 * time.Second

	firstTx, _ := createResetWALConflict(t, store, 31)
	minedInFirst := *firstTx.TxIDChainHash()

	chain.moveForward[0].Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, firstTx)}
	// The second block's subtree is not in the store, so its step fails.
	missing := chainhash.HashH([]byte("catch-up-missing-subtree"))
	chain.moveForward[1].Subtrees = []*chainhash.Hash{&missing}

	keptAfterFirst := -1
	fired := beforeForwardBlockOn(stp, chain.moveForward[0].Header, func() {
		keptAfterFirst = stp.keptMinedSetCount()
	})

	require.Error(t, stp.reorgBlocks(t.Context(), []*model.Block{}, chain.moveForward))
	require.True(t, *fired)
	require.Equal(t, 1, keptAfterFirst, "the first block's step kept its set")
	require.Equal(t, chain.genesis.Hash(), stp.GetCurrentBlockHeader().Hash(), "the catch-up rolled back")
	require.Zero(t, stp.keptMinedSetCount(), "the rolled-back blocks' sets are forgotten")

	stp.AddBatch([]subtreepkg.Node{{Hash: minedInFirst, Fee: 1, SizeInBytes: 1}}, []*subtreepkg.TxInpoints{inpointsPtr()})
	require.Equal(t, int64(1), stp.queue.length(), "a hand-off of the rolled-back block's transaction is queued")
}

package subtreeprocessor

import (
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

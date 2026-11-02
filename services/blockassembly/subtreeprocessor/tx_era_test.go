package subtreeprocessor

import (
	"context"
	"math"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// txEraFixture names a parent validated under the previous rules, its in-era child and
// grandchild, an unrelated in-era tx and an untagged tx. Regtest: Genesis at 100,
// Chronicle at 200, so next height 150 shares rules with [100, 199].
type txEraFixture struct {
	parent, child, grandchild, inEra, untagged chainhash.Hash
}

func newTxEraFixture() txEraFixture {
	return txEraFixture{
		parent:     chainhash.HashH([]byte("tx-era-parent")),
		child:      chainhash.HashH([]byte("tx-era-child")),
		grandchild: chainhash.HashH([]byte("tx-era-grandchild")),
		inEra:      chainhash.HashH([]byte("tx-era-in-era")),
		untagged:   chainhash.HashH([]byte("tx-era-untagged")),
	}
}

// unminedSinceHeights tags reloaded transactions with their unmined_since, as block
// assembly does for those never mined.
func unminedSinceHeights(txs []*utxostore.UnminedTransaction) []uint32 {
	heights := make([]uint32, len(txs))
	for i, tx := range txs {
		heights[i] = uint32(tx.UnminedSince) //nolint:gosec // test heights
	}

	return heights
}

// filterUnmined runs FilterUnminedTxEra with unmined_since heights.
func filterUnmined(stp *SubtreeProcessor, txs []*utxostore.UnminedTransaction) []*utxostore.UnminedTransaction {
	return stp.FilterUnminedTxEra(txs, unminedSinceHeights(txs))
}

func inpointsWithParent(parent chainhash.Hash) *subtreepkg.TxInpoints {
	return &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{parent}}
}

func (f txEraFixture) requireOnlyAdmitted(t *testing.T, stp *SubtreeProcessor) {
	t.Helper()

	for _, h := range []chainhash.Hash{f.inEra, f.untagged} {
		_, ok := stp.currentTxMap.Get(h)
		require.True(t, ok, "an in-era or untagged tx must enter the template: %s", h)
	}

	for _, h := range []chainhash.Hash{f.parent, f.child, f.grandchild} {
		_, ok := stp.currentTxMap.Get(h)
		require.False(t, ok, "a tx validated under other rules, or a descendant of one, must not enter the template: %s", h)
	}
}

// TestTxEra_DequeueDropsOtherRulesAndDescendants pins issue 4940's queue path: a tx tagged
// with a height under other consensus rules than the next block's is dropped at dequeue,
// and so is every queued descendant of it, even when the descendant is in a later batch.
func TestTxEra_DequeueDropsOtherRulesAndDescendants(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stp := newTestSubtreeProcessorForDequeueStaleness(t, ctx)
	stp.SetTxEra(150)

	f := newTxEraFixture()

	require.True(t, stp.AddBatchIfRoom(
		[]subtreepkg.Node{{Hash: f.parent, Fee: 1, SizeInBytes: 1}, {Hash: f.inEra, Fee: 1, SizeInBytes: 1}, {Hash: f.untagged, Fee: 1, SizeInBytes: 1}},
		[]*subtreepkg.TxInpoints{{}, {}, {}},
		[]uint32{99, 150, 0},
	))
	require.True(t, stp.AddBatchIfRoom(
		[]subtreepkg.Node{{Hash: f.child, Fee: 1, SizeInBytes: 1}, {Hash: f.grandchild, Fee: 1, SizeInBytes: 1}},
		[]*subtreepkg.TxInpoints{inpointsWithParent(f.parent), inpointsWithParent(f.child)},
		[]uint32{150, 150},
	))

	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(ctx) })

	require.Eventually(t, func() bool {
		_, ok := stp.currentTxMap.Get(f.untagged)
		return stp.queue.length() == 0 && ok
	}, 2*time.Second, 5*time.Millisecond)

	f.requireOnlyAdmitted(t, stp)
}

// TestTxEra_ReloadDropsOtherRulesAndDescendants pins the reload path: an unmined tx is
// tagged with the height it was created at (unmined_since), so a reload after the rules
// change drops the txs validated under the previous rules and their descendants.
func TestTxEra_ReloadDropsOtherRulesAndDescendants(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stp := newTestSubtreeProcessorForDequeueStaleness(t, ctx)
	stp.SetTxEra(150)

	f := newTxEraFixture()

	unmined := func(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints, unminedSince int) *utxostore.UnminedTransaction {
		return &utxostore.UnminedTransaction{Node: &subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1}, TxInpoints: inpoints, UnminedSince: unminedSince}
	}

	require.NoError(t, stp.AddNodesDirectly(filterUnmined(stp, []*utxostore.UnminedTransaction{
		unmined(f.parent, &subtreepkg.TxInpoints{}, 99),
		unmined(f.inEra, &subtreepkg.TxInpoints{}, 150),
		unmined(f.untagged, &subtreepkg.TxInpoints{}, 0),
		unmined(f.child, inpointsWithParent(f.parent), 150),
		unmined(f.grandchild, inpointsWithParent(f.child), 150),
	}), true))

	f.requireOnlyAdmitted(t, stp)
}

// TestTxEra_FollowsTheNextHeight pins the range SetTxEra installs.
func TestTxEra_FollowsTheNextHeight(t *testing.T) {
	stp := newTestSubtreeProcessorForDequeueStaleness(t, context.Background())

	_, _, ok := stp.TxEra()
	require.False(t, ok, "no era before one is set")

	stp.SetTxEra(150)
	lo, hi, ok := stp.TxEra()
	require.True(t, ok)
	require.Equal(t, []uint32{100, 199}, []uint32{lo, hi})

	stp.SetTxEra(200)
	lo, hi, _ = stp.TxEra()
	require.Equal(t, []uint32{200, 575}, []uint32{lo, hi}, "Chronicle to CSV")
}

// TestTxEra_ReadmittedParentReleasesItsDescendants pins that a transaction refused once and
// admitted later (validated again under the next block's rules, e.g. from a block) is
// forgotten, so its descendants are admitted with it.
func TestTxEra_ReadmittedParentReleasesItsDescendants(t *testing.T) {
	stp := newTestSubtreeProcessorForDequeueStaleness(t, context.Background())
	stp.SetTxEra(150)

	f := newTxEraFixture()

	unmined := func(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints, unminedSince int) *utxostore.UnminedTransaction {
		return &utxostore.UnminedTransaction{Node: &subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1}, TxInpoints: inpoints, UnminedSince: unminedSince}
	}

	kept := filterUnmined(stp, []*utxostore.UnminedTransaction{unmined(f.parent, &subtreepkg.TxInpoints{}, 99)})
	require.Empty(t, kept)

	kept = filterUnmined(stp, []*utxostore.UnminedTransaction{
		unmined(f.parent, &subtreepkg.TxInpoints{}, 150),
		unmined(f.child, inpointsWithParent(f.parent), 150),
	})
	require.Len(t, kept, 2, "the parent, now in era, and its child are both admitted")
	require.Zero(t, stp.droppedCount.Load())
}

// TestTxEra_BlockMovementDrainDropsOtherRulesAndDescendants pins the drain a block move
// runs over the queue (forEachDrainChunk), which bypasses the dequeue loop.
func TestTxEra_BlockMovementDrainDropsOtherRulesAndDescendants(t *testing.T) {
	stp := newTestSubtreeProcessorForDequeueStaleness(t, context.Background())
	stp.SetTxEra(150)

	f := newTxEraFixture()

	require.True(t, stp.AddBatchIfRoom(
		[]subtreepkg.Node{{Hash: f.parent, Fee: 1, SizeInBytes: 1}, {Hash: f.inEra, Fee: 1, SizeInBytes: 1}, {Hash: f.untagged, Fee: 1, SizeInBytes: 1}},
		[]*subtreepkg.TxInpoints{{}, {}, {}},
		[]uint32{99, 150, 0},
	))
	require.True(t, stp.AddBatchIfRoom(
		[]subtreepkg.Node{{Hash: f.child, Fee: 1, SizeInBytes: 1}, {Hash: f.grandchild, Fee: 1, SizeInBytes: 1}},
		[]*subtreepkg.TxInpoints{inpointsWithParent(f.parent), inpointsWithParent(f.child)},
		[]uint32{150, 150},
	))

	// The queue's double-spend window holds back batches younger than it.
	stp.settings.BlockAssembly.DoubleSpendWindow = 0
	time.Sleep(2 * time.Millisecond)

	require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, true))

	f.requireOnlyAdmitted(t, stp)
}

// TestTxEra_MoveBackKeepsOutABlockUnderOtherRules pins the reorg path: a block moved back
// returns its transactions to the template, and they were validated at its height. A
// block below the next block's rules (a reorg that keeps the next height past an
// activation height) must not return them.
func TestTxEra_MoveBackKeepsOutABlockUnderOtherRules(t *testing.T) {
	for _, tt := range []struct {
		name        string
		blockHeight uint32
		returned    bool
	}{
		{"block under the next block's rules", 150, true},
		{"block below Genesis, next block past it", 99, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stp := newTestSubtreeProcessorForDequeueStaleness(t, context.Background())
			stp.SetTxEra(150)

			blockchainClient := &blockchain.Mock{}
			blockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			stp.blockchainClient = blockchainClient

			utxoStoreURL, err := url.Parse("sqlitememory:///test")
			require.NoError(t, err)
			utxoStore, err := sql.New(t.Context(), ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
			require.NoError(t, err)
			stp.utxoStore = utxoStore

			st := createSubtree(t, 4, true)
			subtreeBytes, err := st.Serialize()
			require.NoError(t, err)
			require.NoError(t, stp.subtreeStore.Set(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

			metaBytes, err := createSubtreeMeta(t, st).Serialize()
			require.NoError(t, err)
			require.NoError(t, stp.subtreeStore.Set(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeMeta, metaBytes))

			_, _ = utxoStore.Create(t.Context(), coinbaseTx, 0)
			stp.InitCurrentBlockHeader(blockHeader)

			_, _, err = stp.moveBackBlock(t.Context(), &model.Block{
				Header:     prevBlockHeader,
				Height:     tt.blockHeight,
				Subtrees:   []*chainhash.Hash{st.RootHash()},
				CoinbaseTx: coinbaseTx,
			}, true)
			require.NoError(t, err)

			for _, node := range st.Nodes[1:] {
				_, ok := stp.currentTxMap.Get(node.Hash)
				require.Equal(t, tt.returned, ok, "tx %s", node.Hash)
			}
		})
	}
}

// TestTxEra_MinedDroppedParentReleasesItsDescendants pins that a dropped transaction a block
// then mines is forgotten. Its descendants are validated against the confirmed parent under
// the next block's rules, and must not stay out of the template because of the drop.
func TestTxEra_MinedDroppedParentReleasesItsDescendants(t *testing.T) {
	f := newCatchUpQueueFixture(t, true)
	f.stp.SetTxEra(150)

	// Only the block may clear the record: drop the fixture's queued copy of txA, whose
	// re-admission would.
	for {
		if _, ok := f.stp.queue.dequeueBatch(math.MaxInt64); !ok {
			break
		}
	}

	unmined := func(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints, unminedSince int) *utxostore.UnminedTransaction {
		return &utxostore.UnminedTransaction{Node: &subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1}, TxInpoints: inpoints, UnminedSince: unminedSince}
	}

	// txA validated under other rules, then mined by another miner in block1.
	require.Empty(t, filterUnmined(f.stp, []*utxostore.UnminedTransaction{unmined(f.txA, &subtreepkg.TxInpoints{}, 99)}))

	require.NoError(t, f.stp.reorgBlocks(t.Context(), []*model.Block{}, []*model.Block{f.block1, f.block2}))

	child := chainhash.HashH([]byte("tx-era-child-of-mined"))
	require.Len(t, filterUnmined(f.stp, []*utxostore.UnminedTransaction{unmined(child, inpointsWithParent(f.txA), 150)}), 1,
		"a child of a dropped parent that has since been mined must be admitted")
}

// TestTxEra_RestartDroppedTxs pins the reload's handling of the dropped set: a reload
// re-derives it from the UTXO store, so records it does not find again (a parent mined
// meanwhile) are gone after a successful reload, and kept after a failed one.
func TestTxEra_RestartDroppedTxs(t *testing.T) {
	for _, reloaded := range []bool{true, false} {
		stp := newTestSubtreeProcessorForDequeueStaleness(t, context.Background())
		stp.SetTxEra(150)

		f := newTxEraFixture()

		require.Empty(t, filterUnmined(stp, []*utxostore.UnminedTransaction{
			{Node: &subtreepkg.Node{Hash: f.parent}, TxInpoints: &subtreepkg.TxInpoints{}, UnminedSince: 99},
		}))

		finish := stp.RestartDroppedTxs()
		finish(reloaded)

		kept := filterUnmined(stp, []*utxostore.UnminedTransaction{
			{Node: &subtreepkg.Node{Hash: f.child}, TxInpoints: inpointsWithParent(f.parent), UnminedSince: 150},
		})

		if reloaded {
			require.Len(t, kept, 1, "a reload that did not find the parent again forgets it")
		} else {
			require.Empty(t, kept, "a failed reload keeps the records it was replacing")
		}
	}
}

// TestReset_RemoveMapReadDuringReset pins that a reset empties the remove map in place: the
// block assembly state endpoint reads its length from another goroutine, and a reset
// replacing the field raced with it (a smoke run caught it once era changes made resets
// routine on regtest).
func TestReset_RemoveMapReadDuringReset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stp := newTestSubtreeProcessorForDequeueStaleness(t, ctx)
	stp.InitCurrentBlockHeader(blockHeader)
	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(ctx) })

	require.NoError(t, stp.removeMap.Put(chainhash.HashH([]byte("to-remove")), 1))

	done := make(chan struct{})
	go func() {
		defer close(done)

		for ctx.Err() == nil {
			_ = stp.GetRemoveMapLength()
		}
	}()

	for i := 0; i < 5; i++ {
		require.NoError(t, stp.Reset(stp.GetCurrentBlockHeader(), nil, nil, false, nil).Err)
	}

	cancel()
	<-done

	require.Zero(t, stp.GetRemoveMapLength(), "the reset empties the remove map")
}

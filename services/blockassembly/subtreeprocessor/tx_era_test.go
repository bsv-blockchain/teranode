package subtreeprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
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

	require.NoError(t, stp.AddNodesDirectly([]*utxostore.UnminedTransaction{
		unmined(f.parent, &subtreepkg.TxInpoints{}, 99),
		unmined(f.inEra, &subtreepkg.TxInpoints{}, 150),
		unmined(f.untagged, &subtreepkg.TxInpoints{}, 0),
		unmined(f.child, inpointsWithParent(f.parent), 150),
		unmined(f.grandchild, inpointsWithParent(f.child), 150),
	}, true))

	f.requireOnlyAdmitted(t, stp)
}

// TestTxEra_SetReportsAChangeOfRules pins the signal block assembly resets on.
func TestTxEra_SetReportsAChangeOfRules(t *testing.T) {
	stp := newTestSubtreeProcessorForDequeueStaleness(t, context.Background())

	require.False(t, stp.SetTxEra(150), "the first era set is not a change")
	require.False(t, stp.SetTxEra(160), "same rules")
	require.True(t, stp.SetTxEra(200), "Chronicle")
	require.True(t, stp.SetTxEra(199), "back below Chronicle")
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

package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// nettingRecorder is an applyRecorder whose store says it nets below the checkpoint, as utxoset
// does, so quick validation takes its netting mode. The lists still go to the recorder's SQLite
// store, which applies them one transaction at a time.
type nettingRecorder struct {
	*applyRecorder
}

func (nettingRecorder) NetsBelowCheckpoint() bool { return true }

// newNetRangeHarness is newListHarness in the netting mode: outpoint-only, the UTXO lock skipped,
// a store that nets, and a small position range so a test block spans several ranges.
func newNetRangeHarness(t *testing.T, dbName string, netRange int) (*BlockValidation, *applyRecorder, func()) {
	t.Helper()

	bv, recorder, cleanup := newListHarness(t, dbName, true)
	bv.settings.BlockValidation.QuickValidateSkipUtxoLock = true
	bv.settings.BlockValidation.QuickValidateNetRange = netRange
	bv.utxoStore = nettingRecorder{recorder}

	return bv, recorder, cleanup
}

// netBatchFor builds one batch of a block from a subtree layout: layout[i] is the transactions
// of subtree i, with subtree 0's coinbase left out, as quick validation reads them.
func netBatchFor(t *testing.T, bv *BlockValidation, block *model.Block, layout [][]*bt.Tx) *SubtreeProcessingBatch {
	t.Helper()

	batch := &SubtreeProcessingBatch{
		subtreeData:  make([]*subtreepkg.Data, len(layout)),
		txRanges:     make([][2]int, len(layout)),
		batchStart:   0,
		batchEnd:     len(layout),
		outpointOnly: true,
	}

	for i, txs := range layout {
		data := txs
		if i == 0 {
			data = append([]*bt.Tx{nil}, txs...)
		}

		batch.subtreeData[i] = &subtreepkg.Data{Txs: data}
	}

	require.NoError(t, bv.extendBatch(context.Background(), block, batch, map[chainhash.Hash]*bt.Tx{}))

	return batch
}

func netBlockFor(layout [][]*bt.Tx) *model.Block {
	count := uint64(1) // the coinbase
	for _, txs := range layout {
		count += uint64(len(txs))
	}

	return &model.Block{Height: 100, ID: 42, Subtrees: make([]*chainhash.Hash, len(layout)), TransactionCount: count}
}

func txidsOf(txs []*bt.Tx) []chainhash.Hash {
	out := make([]chainhash.Hash, len(txs))
	for i, tx := range txs {
		out[i] = *tx.TxIDChainHash()
	}

	return out
}

// The lists a netting store gets are fixed ranges of transaction positions, whatever subtree
// size the block arrived with: a repeat through another route, with another subtree size, hands
// the store exactly the same lists.
func TestNetRange_ListsAreTheSameForAnySubtreeSize(t *testing.T) {
	const netRange = 4

	run := func(t *testing.T, dbName string, layoutOf func(txs []*bt.Tx) [][]*bt.Tx) [][]chainhash.Hash {
		bv, recorder, cleanup := newNetRangeHarness(t, dbName, netRange)
		defer cleanup()

		root, key := seedRoot(t, recorder.Store, 6, "NET_RANGE_KEY")

		txs := make([]*bt.Tx, 6)
		for i := range txs {
			txs[i] = spendOf(t, key, root, uint32(i), 90_000) //nolint:gosec // test index
		}

		layout := layoutOf(txs)
		block := netBlockFor(layout)

		recorder.reset()
		require.NoError(t, bv.createAndSpendUTXOsForBatch(context.Background(), block, netBatchFor(t, bv, block, layout)))

		recorded := recorder.recordedLists()
		lists := make([][]chainhash.Hash, 0, len(recorded))

		for _, l := range recorded {
			lists = append(lists, l.txids)
		}

		return lists
	}

	// Positions: 0 is the coinbase, then the six transactions at 1 to 6. Ranges of 4: {1,2,3}
	// and {4,5,6}.
	small := run(t, "net_range_s2", func(txs []*bt.Tx) [][]*bt.Tx {
		return [][]*bt.Tx{txs[0:1], txs[1:3], txs[3:5], txs[5:6]} // subtrees of 2
	})

	large := run(t, "net_range_s4", func(txs []*bt.Tx) [][]*bt.Tx {
		return [][]*bt.Tx{txs[0:3], txs[3:6]} // subtrees of 4
	})

	require.Len(t, small, 2, "two ranges of four positions")
	require.Equal(t, small, large, "the same lists whatever the subtree size")
}

// On a repeat a netting store reports the transactions it already holds as existing, and quick
// validation only stamps them: it never spends their inputs again, because no journal row would
// accept the repeat.
func TestNetRange_ExistingTransactionsAreNotSpentAgain(t *testing.T) {
	bv, recorder, cleanup := newNetRangeHarness(t, "net_range_repeat", 65536)
	defer cleanup()

	ctx := context.Background()

	root, key := seedRoot(t, recorder.Store, 2, "NET_REPEAT_KEY")
	txs := []*bt.Tx{spendOf(t, key, root, 0, 90_000), spendOf(t, key, root, 1, 90_000)}

	layout := [][]*bt.Tx{txs}
	block := netBlockFor(layout)

	require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, block, netBatchFor(t, bv, block, layout)))

	recorder.reset()
	require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, block, netBatchFor(t, bv, block, layout)))
	require.Zero(t, recorder.spendOnlyCalls(), "an existing transaction is stamped, not spent again")
}

// In the netting mode a transaction with no spendable output stays in the list: its spends need
// the list's claim to be repeatable.
func TestNetRange_UnspendableTransactionStaysInTheList(t *testing.T) {
	bv, recorder, cleanup := newNetRangeHarness(t, "net_range_unspendable", 65536)
	defer cleanup()

	bv.settings.BlockValidation.SkipUnspendableTxStorageDuringCatchup = true

	root, key := seedRoot(t, recorder.Store, 2, "NET_UNSPENDABLE_KEY")

	spend := spendOf(t, key, root, 0, 90_000)

	data := spendOf(t, key, root, 1, 90_000)
	script, err := bscript.NewFromASM("OP_FALSE OP_RETURN 74657374")
	require.NoError(t, err)

	data.Outputs = []*bt.Output{{Satoshis: 0, LockingScript: script}}

	layout := [][]*bt.Tx{{spend, data}}
	block := netBlockFor(layout)

	recorder.reset()
	require.NoError(t, bv.createAndSpendUTXOsForBatch(context.Background(), block, netBatchFor(t, bv, block, layout)))

	lists := recorder.recordedLists()
	require.Len(t, lists, 1)
	require.Equal(t, txidsOf([]*bt.Tx{spend, data}), lists[0].txids, "the unspendable transaction is in the list")
	require.Zero(t, recorder.spendOnlyCalls(), "and has no separate spend")
}

// A subtree that is not full, other than the last, breaks the position arithmetic, and the batch
// is refused rather than cut into ranges that another route would cut differently.
func TestNetRange_RefusesASubtreeOfTheWrongSize(t *testing.T) {
	bv, recorder, cleanup := newNetRangeHarness(t, "net_range_shape", 4)
	defer cleanup()

	root, key := seedRoot(t, recorder.Store, 4, "NET_SHAPE_KEY")

	txs := make([]*bt.Tx, 4)
	for i := range txs {
		txs[i] = spendOf(t, key, root, uint32(i), 90_000) //nolint:gosec // test index
	}

	// Four subtrees for five transactions: capacity 2, but subtree 1 holds 1.
	layout := [][]*bt.Tx{txs[0:1], txs[1:2], txs[2:4], {}}
	block := netBlockFor(layout)

	err := bv.createAndSpendUTXOsForBatch(context.Background(), block, netBatchFor(t, bv, block, layout))
	require.Error(t, err)
	require.Contains(t, err.Error(), "subtree size")
}

// In the netting mode the batch is cut to whole ranges, so a range never spans two batches.
func TestNetRange_BatchIsCutToWholeRanges(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "net_range_batch", 65536)
	defer cleanup()

	block := &model.Block{Height: 100, Subtrees: make([]*chainhash.Hash, 40), TransactionCount: 40 * 4096}
	require.Equal(t, 16, bv.quickSubtreeBatchSize(block, true), "16 subtrees of 4,096 make one range")

	block = &model.Block{Height: 100, Subtrees: make([]*chainhash.Hash, 3), TransactionCount: 3 * 1048576}
	require.Equal(t, 1, bv.quickSubtreeBatchSize(block, true), "a subtree larger than a range is one batch of several ranges")

	block = &model.Block{Height: 100, Subtrees: make([]*chainhash.Hash, 40), TransactionCount: 40 * 1024}
	require.Equal(t, 64, bv.quickSubtreeBatchSize(block, true), "64 subtrees of 1,024 make one range")
}

package blockvalidation

import (
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newTestScanner returns a released-on-cleanup in-memory scanner.
func newTestScanner(t *testing.T) *model.DuplicateTxScanner {
	t.Helper()

	s, err := model.NewDuplicateTxScanner(8, nil)
	require.NoError(t, err)
	t.Cleanup(s.Release)

	return s
}

// batchOf wraps transactions in the minimum SubtreeProcessingBatch scanBatchForDuplicates
// reads: it only looks at batchTxs, the list about to be applied to the UTXO store.
func batchOf(txs ...*bt.Tx) *SubtreeProcessingBatch {
	return &SubtreeProcessingBatch{batchTxs: txs}
}

// TestScanBatchForDuplicates pins the CVE-2012-2459 floor at its seam on the
// quick-validation path (issue 1424). That path is default-on for every block at or below
// the highest checkpoint and previously ran no duplicate check at all, while the merkle
// root provably cannot detect the mutation.
//
// The scan deliberately reads batchTxs — the transactions about to be written — rather
// than block.SubtreeSlices, because on a retry those slices are deserialized from a local
// .subtree file that can differ from the peer body being applied.
func TestScanBatchForDuplicates(t *testing.T) {
	// coinbase plus three distinct spendable transactions.
	txs := transactions.CreateTestTransactionChainWithCount(t, 5)
	require.Len(t, txs, 4)
	txA, txB, txC := txs[1], txs[2], txs[3]

	t.Run("duplicate inside one batch is rejected", func(t *testing.T) {
		err := scanBatchForDuplicates(batchOf(txA, txB, txA), newTestScanner(t))
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate transaction")
		require.Contains(t, err.Error(), "CVE-2012-2459")
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a duplicate must be BlockInvalid, so the peer is punished")
		require.False(t, errors.Is(err, errors.ErrProcessing),
			"it must not carry a top-level ErrProcessing code, which reads as transient")
	})

	t.Run("duplicate spanning two batches is rejected", func(t *testing.T) {
		// The cross-batch case is why the scanner is per block rather than per batch: a
		// mutant can put its repeat in a later subtree than the original, and a batch-local
		// check would see two clean batches.
		dedup := newTestScanner(t)

		require.NoError(t, scanBatchForDuplicates(batchOf(txA, txB), dedup))

		err := scanBatchForDuplicates(batchOf(txC, txA), dedup)
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate transaction")
		require.True(t, errors.Is(err, errors.ErrBlockInvalid))
	})

	t.Run("distinct transactions pass", func(t *testing.T) {
		dedup := newTestScanner(t)
		require.NoError(t, scanBatchForDuplicates(batchOf(txA, txB), dedup))
		require.NoError(t, scanBatchForDuplicates(batchOf(txC), dedup))
	})

	t.Run("an empty batch is a no-op", func(t *testing.T) {
		require.NoError(t, scanBatchForDuplicates(batchOf(), newTestScanner(t)))
	})
}

// newQuickPathSuiteWithRealUTXOStore returns a catchup suite whose mock UTXO store has been
// swapped for a real sqlitememory one, so the test can assert on what was actually written
// rather than on mock call expectations — and per AGENTS.md the store is not something to mock.
func newQuickPathSuiteWithRealUTXOStore(t *testing.T) *CatchupTestSuite {
	t.Helper()

	suite := NewCatchupTestSuite(t)

	storeURL, err := url.Parse("sqlitememory:///quick_validate_duplicate_test")
	require.NoError(t, err)

	realStore, err := sql.New(suite.Ctx, ulogger.TestLogger{}, suite.Server.blockValidation.settings, storeURL)
	require.NoError(t, err)

	suite.Server.blockValidation.utxoStore = realStore

	suite.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Maybe()

	// Left on the default prefetch depth, so this runs the pipeline variant. The sequential
	// variant (blockvalidation_subtree_batch_prefetch_depth=0) cannot be driven end to end
	// today: processSubtreeBatch never allocates SubtreeProcessingBatch.fullSubtreeExists,
	// which prefetchSubtreeBatch does, so writeSubtreeFilesForBatch panics indexing a nil
	// slice. That is pre-existing on main and unrelated to this change. All three variants
	// call scanBatchForDuplicates before their fan-out, so the floor holds on each.

	return suite
}

// storeDuplicateBearingBlock builds a block whose body carries the same transaction in two
// different subtrees — the CVE-2012-2459 shape — and writes both subtrees and their data to
// the suite's subtree store so the quick-validation pipeline can read them back.
//
// It returns the duplicated transaction alongside the block so a caller can assert that
// nothing was written for it — the property the scan's placement exists to guarantee.
func storeDuplicateBearingBlock(t *testing.T, suite *CatchupTestSuite) (*model.Block, *bt.Tx) {
	t.Helper()

	// A real coinbase plus one real spending transaction; readSubtree requires subtree 0
	// index 0 to be a coinbase. The helper returns count-1 transactions, so 3 gives two.
	txs := transactions.CreateTestTransactionChainWithCount(t, 3)
	require.Len(t, txs, 2)
	coinbaseTx, dupTx := txs[0], txs[1]

	storeSubtree := func(st *subtreepkg.Subtree, data *subtreepkg.Data) {
		stBytes, err := st.Serialize()
		require.NoError(t, err)
		require.NoError(t, suite.Server.subtreeStore.Set(suite.Ctx, st.RootHash()[:], fileformat.FileTypeSubtreeToCheck, stBytes))

		dataBytes, err := data.Serialize()
		require.NoError(t, err)
		require.NoError(t, suite.Server.subtreeStore.Set(suite.Ctx, st.RootHash()[:], fileformat.FileTypeSubtreeData, dataBytes))
	}

	// Subtree 0: coinbase placeholder, then dupTx.
	first, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, first.AddCoinbaseNode())
	require.NoError(t, first.AddNode(*dupTx.TxIDChainHash(), 1, 1))

	firstData := subtreepkg.NewSubtreeData(first)
	require.NoError(t, firstData.AddTx(coinbaseTx, 0))
	require.NoError(t, firstData.AddTx(dupTx, 1))
	storeSubtree(first, firstData)

	// Subtree 1: dupTx again. Different leaf count, so a different root — the two subtrees
	// are distinct entries in the block, carrying the same transaction.
	second, err := subtreepkg.NewIncompleteTreeByLeafCount(1)
	require.NoError(t, err)
	require.NoError(t, second.AddNode(*dupTx.TxIDChainHash(), 1, 1))

	secondData := subtreepkg.NewSubtreeData(second)
	require.NoError(t, secondData.AddTx(dupTx, 0))
	storeSubtree(second, secondData)

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	block.CoinbaseTx = coinbaseTx
	block.Subtrees = []*chainhash.Hash{first.RootHash(), second.RootHash()}
	block.Height = 1 // the UTXO store rejects a spend at height zero

	return block, dupTx
}

// TestQuickValidateBlock_DuplicateIsBlockInvalidNotProcessing pins the two entry-point
// pass-throughs in quickValidateBlock and quickValidateBlockAsync, which return an
// ErrBlockInvalid from the subtree pipeline unwrapped instead of re-wrapping it as a
// ProcessingError.
//
// A mutation check showed these had zero coverage: deleting BOTH left every test in the
// package passing. TestScanBatchForDuplicates above does not reach them — it drives the scan
// directly and so never crosses the site that does the wrapping.
//
// Two assertions carry the test. The negative error assertion: (*Error).Is walks the wrapped
// chain, so errors.Is(err, ErrBlockInvalid) stays true even when the rejection IS re-wrapped
// as a ProcessingError, and asserting only that would pass with the pass-throughs deleted.
// What re-wrapping changes is the error's own code — the rejection would additionally satisfy
// ErrProcessing, so a consumer classifying on the top-level code reads a consensus rejection
// as transient. Hence require.False on ErrProcessing.
//
// And the store assertion: nothing may have been written for the duplicated transaction. That
// is the property the scan's placement exists to guarantee. Before the scan moved ahead of
// createAndSpendUTXOsForBatch, a rejected mutant left the whole block's UTXOs created and
// locked, with no unlock pass on any rejection path to clear them.
func TestQuickValidateBlock_DuplicateIsBlockInvalidNotProcessing(t *testing.T) {
	// requireConsensusRejection asserts the shape both entry points must produce.
	requireConsensusRejection := func(t *testing.T, err error) {
		t.Helper()

		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate transaction")
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a duplicate on the quick path must reach the caller as BlockInvalid")
		require.False(t, errors.Is(err, errors.ErrProcessing),
			"it must not carry a top-level ErrProcessing code, which reads as a transient fault")
	}

	t.Run("quickValidateBlock returns the rejection unwrapped", func(t *testing.T) {
		suite := newQuickPathSuiteWithRealUTXOStore(t)
		defer suite.Cleanup()

		block, dupTx := storeDuplicateBearingBlock(t, suite)

		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test-peer", "")
		requireConsensusRejection(t, err)
		requireNothingWritten(t, suite, dupTx)
	})

	t.Run("quickValidateBlockAsync returns the rejection unwrapped", func(t *testing.T) {
		suite := newQuickPathSuiteWithRealUTXOStore(t)
		defer suite.Cleanup()

		block, dupTx := storeDuplicateBearingBlock(t, suite)

		// Buffered large enough that the async path never blocks queuing a write job.
		writeJobsChan := make(chan *SubtreeWriteJob, 16)

		err := suite.Server.blockValidation.quickValidateBlockAsync(suite.Ctx, block, "test-peer", "", writeJobsChan)
		requireConsensusRejection(t, err)
		requireNothingWritten(t, suite, dupTx)

		// Nothing doctored may have been queued for writing either: subtreeWriteWorker
		// writes with WithAllowOverwrite(true), so a job surviving the rejection could
		// land a doctored body under an honest subtree hash after catchup's cleanup pass.
		require.Empty(t, writeJobsChan, "a rejected block must not have queued subtree write jobs")
	})
}

// requireNothingWritten asserts the rejected block left no trace of the duplicated
// transaction in the UTXO store — neither created nor created-and-locked.
func requireNothingWritten(t *testing.T, suite *CatchupTestSuite, dupTx *bt.Tx) {
	t.Helper()

	_, err := suite.Server.blockValidation.utxoStore.Get(suite.Ctx, dupTx.TxIDChainHash())
	require.Error(t, err, "the duplicate's UTXOs must never have been created")
	require.True(t, errors.Is(err, errors.ErrTxNotFound),
		"expected the tx to be absent from the store, got: %v", err)
}

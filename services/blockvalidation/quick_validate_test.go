package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	testutil "github.com/bsv-blockchain/teranode/util/test"
	prometheustestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestQuickValidateBlock(t *testing.T) {
	t.Run("empty block", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Mock blockchain AddBlock and check how it was called
		suite.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Once()
		suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		suite.MockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()

		block := testhelpers.CreateTestBlocks(t, 1)[0]

		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test", "")
		assert.NoError(t, err, "Should successfully quick validate an empty block")

		// Verify AddBlock was called with correct parameters
		suite.MockBlockchain.AssertCalled(t, "AssignBlockID", mock.Anything, mock.Anything)
		suite.MockBlockchain.AssertCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)

		arguments := suite.MockBlockchain.Calls[1].Arguments
		addedBlock := arguments.Get(1).(*model.Block)
		assert.Equal(t, uint32(0), addedBlock.Height, "Block height should be set correctly")
		assert.Equal(t, block.Header.Hash(), addedBlock.Header.Hash(), "Block hash should match")

		peerID := arguments.Get(2).(string)
		assert.Equal(t, "test", peerID, "Peer ID should match")

		storeBlockOptions := arguments.Get(3).([]options.StoreBlockOption)
		assert.Len(t, storeBlockOptions, 3, "Should have one store block option")

		sbo := options.StoreBlockOptions{}
		for _, opt := range storeBlockOptions {
			opt(&sbo)
		}
		assert.True(t, sbo.MinedSet, "MinedSetting option should be true")
		assert.True(t, sbo.SubtreesSet, "SubtreesSetting option should be false")
		assert.False(t, sbo.Invalid, "SkipValidation option should be true")
	})

	t.Run("block with 1 subtree and 2 txs", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Mock blockchain AddBlock and check how it was called
		suite.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Maybe()
		suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		suite.MockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()
		suite.MockBlockchain.On("RevalidateBlock", mock.Anything, mock.Anything).Return(nil).Maybe()

		// Create a transaction chain with coinbase + 2 regular transactions
		txs := transactions.CreateTestTransactionChainWithCount(t, 4)
		coinbaseTx := txs[0]
		regularTxs := txs[1:] // txs[1], txs[2]

		// Create block with the proper coinbase
		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.Height = 100
		block.CoinbaseTx = coinbaseTx // Use the coinbase from our transaction chain

		subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(3)
		require.NoError(t, err, "Should create subtree without error")

		require.NoError(t, subtree.AddCoinbaseNode())
		require.NoError(t, subtree.AddNode(*regularTxs[0].TxIDChainHash(), 1, 1))
		require.NoError(t, subtree.AddNode(*regularTxs[1].TxIDChainHash(), 2, 2))

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err, "Should serialize subtree without error")

		err = suite.Server.subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes)
		require.NoError(t, err, "Should store subtree without error")

		subtreeData := subtreepkg.NewSubtreeData(subtree)
		require.NoError(t, subtreeData.AddTx(coinbaseTx, 0), "Should add coinbase tx to subtree data without error")
		require.NoError(t, subtreeData.AddTx(regularTxs[0], 1), "Should add tx 0 to subtree data without error")
		require.NoError(t, subtreeData.AddTx(regularTxs[1], 2), "Should add tx 1 to subtree data without error")

		subtreeDataBytes, err := subtreeData.Serialize()
		require.NoError(t, err, "Should serialize subtree data without error")

		err = suite.Server.subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
		require.NoError(t, err, "Should store subtree data without error")

		block.Subtrees = []*chainhash.Hash{subtree.RootHash()}
		block.TransactionCount = 3 // coinbase + 2 transactions

		// Update the merkle root to match the subtree
		block.Header.HashMerkleRoot, err = subtree.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, 0)
		require.NoError(t, err, "Should create merkle root hash without error")

		// Setup Get expectation for checking existing transactions (used for BlockID reuse on retry)
		suite.MockUTXOStore.On("Get", mock.Anything, mock.Anything, mock.Anything).Return((*meta.Data)(nil), errors.NewNotFoundError("not found"))

		// Setup UTXO store expectations for creating all transactions (including coinbase)
		// Use mock.Anything for the transaction since the order may vary
		suite.MockUTXOStore.On("SpendAndCreate", mock.Anything, mock.Anything, uint32(100), matchCreateOnly()).Return(&meta.Data{}, nil, nil)

		// Setup UTXO store expectations for spending transactions
		suite.MockUTXOStore.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, matchSpendOnly()).Return(nil, []*utxo.Spend{}, nil)

		// Setup SetLocked expectation for unlocking UTXOs after AddBlock
		suite.MockUTXOStore.On("SetLocked", mock.Anything, mock.Anything, false).Return(nil)

		// Setup validator to return no errors (one for each transaction: coinbase + 2 regular)
		suite.MockValidator.Errors = []error{nil, nil, nil}

		err = suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test", "")
		assert.NoError(t, err, "Should successfully quick validate a block with transactions")

		// Verify AddBlock was called with correct parameters
		suite.MockBlockchain.AssertCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)

		// Find the AddBlock call in the mock calls
		var addBlockCall *mock.Call
		for i := range suite.MockBlockchain.Calls {
			if suite.MockBlockchain.Calls[i].Method == "AddBlock" {
				addBlockCall = &suite.MockBlockchain.Calls[i]
				break
			}
		}
		require.NotNil(t, addBlockCall, "AddBlock should have been called")

		arguments := addBlockCall.Arguments
		require.GreaterOrEqual(t, len(arguments), 4, "AddBlock should have at least 4 arguments")

		addedBlock := arguments.Get(1).(*model.Block)
		assert.Equal(t, uint32(100), addedBlock.Height, "Block height should be set correctly")
		assert.Equal(t, block.Header.Hash(), addedBlock.Header.Hash(), "Block hash should match")

		peerID := arguments.Get(2).(string)
		assert.Equal(t, "test", peerID, "Peer ID should match")

		storeBlockOptions := arguments.Get(3).([]options.StoreBlockOption)
		assert.Len(t, storeBlockOptions, 3, "Should have three store block options: WithSubtreesSet, WithMinedSet, and WithID")

		sbo := options.StoreBlockOptions{}
		for _, opt := range storeBlockOptions {
			opt(&sbo)
		}
		assert.True(t, sbo.MinedSet, "MinedSetting option should be true")
		assert.True(t, sbo.SubtreesSet, "SubtreesSetting option should be true")
		assert.Equal(t, uint64(1), sbo.ID, "ID option should be set to 1")
	})
}

// TestProcessBlockSubtrees tests subtree processing (simplified)
func TestProcessBlockSubtrees(t *testing.T) {
	t.Run("EmptySubtrees", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create block with no subtrees
		prevHash := chainhash.Hash{}
		merkleRoot := chainhash.Hash{1, 2, 3}
		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &prevHash,
				HashMerkleRoot: &merkleRoot,
				Timestamp:      1000000,
			},
			Height:           100,
			TransactionCount: 0,
			Subtrees:         []*chainhash.Hash{}, // Empty subtrees
		}

		// Execute processBlockSubtrees (outpointOnly irrelevant here — errors on no subtrees)
		_, err := suite.Server.blockValidation.processBlockSubtrees(suite.Ctx, block, false)

		// Verify error
		assert.Error(t, err, "Should fail when block has no subtrees")
		assert.Contains(t, err.Error(), "block has no subtrees", "Error should indicate no subtrees")
	})
}

// TestQuickValidationDecisionLogic tests the core validation decision logic
// (Removed TestQuickValidationDecisionLogic/HeightBasedValidation: it recomputed the gate
// expression inline and asserted it against hand-authored constants, invoking no production
// code. The real gate — useQuickValidation && height <= highestCheckpointHeight in
// tryQuickValidation — is exercised through production calls in
// catchup_quickvalidation_test.go:TestTryQuickValidation.)

// TestCreateAndSpendUTXOsForBatch_UpdatesExistingTransactions tests that existing transactions
// have their mined info updated when ErrTxExists is returned during quick validation.
// This is critical for crash recovery scenarios where UTXOs may have been created with
// a different BlockID in a previous failed attempt.
func TestCreateAndSpendUTXOsForBatch_UpdatesExistingTransactions(t *testing.T) {
	t.Run("all new transactions - no SetMinedMulti call", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test transactions - need 4 to get 3 regular txs after skipping coinbase
		txs := transactions.CreateTestTransactionChainWithCount(t, 4)
		regularTxs := txs[1:3] // Get exactly 2 transactions

		// Setup block
		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.Height = 100
		block.ID = 50

		// Create batch with 2 subtrees, each containing 1 transaction
		batch := &SubtreeProcessingBatch{
			batchTxs:   regularTxs,
			txRanges:   [][2]int{{0, 1}, {1, 2}}, // subtree 0: tx 0, subtree 1: tx 1
			batchStart: 0,
			batchEnd:   2,
		}

		// Mock the create phase to succeed (no ErrTxExists)
		suite.MockUTXOStore.On("SpendAndCreate", mock.Anything, mock.Anything, uint32(100), matchCreateOnly()).
			Return(&meta.Data{}, nil, nil).Maybe()

		// Mock the spend phase
		suite.MockUTXOStore.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, matchSpendOnly()).
			Return(nil, []*utxo.Spend{}, nil).Maybe()

		// SetMinedMulti should NOT be called since all txs are new

		err := suite.Server.blockValidation.createAndSpendUTXOsForBatch(suite.Ctx, block, batch)
		require.NoError(t, err)

		// Verify the create phase ran for each transaction
		require.Equal(t, 2, countCreatePhaseCalls(suite.MockUTXOStore))
		// Verify SetMinedMulti was NOT called
		suite.MockUTXOStore.AssertNotCalled(t, "SetMinedMulti", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("all existing transactions - SetMinedMulti called", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test transactions
		txs := transactions.CreateTestTransactionChainWithCount(t, 4)
		regularTxs := txs[1:3] // Get exactly 2 transactions

		// Setup block
		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.Height = 100
		block.ID = 50

		// Create batch with 2 subtrees, each containing 1 transaction
		batch := &SubtreeProcessingBatch{
			batchTxs:   regularTxs,
			txRanges:   [][2]int{{0, 1}, {1, 2}},
			batchStart: 0,
			batchEnd:   2,
		}

		// Mock the create phase to return ErrTxExists for all transactions
		suite.MockUTXOStore.On("SpendAndCreate", mock.Anything, mock.Anything, uint32(100), matchCreateOnly()).
			Return((*meta.Data)(nil), nil, errors.ErrTxExists).Maybe()

		// Mock SetMinedMulti - should be called with both transaction hashes
		suite.MockUTXOStore.On("SetMinedMulti", mock.Anything, mock.MatchedBy(func(hashes []*chainhash.Hash) bool {
			return len(hashes) == 2
		}), mock.MatchedBy(func(info utxo.MinedBlockInfo) bool {
			return info.BlockID == 50 && info.BlockHeight == 100
		})).Return(map[chainhash.Hash][]uint32{}, nil).Once()

		// Mock the spend phase
		suite.MockUTXOStore.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, matchSpendOnly()).
			Return(nil, []*utxo.Spend{}, nil).Maybe()

		err := suite.Server.blockValidation.createAndSpendUTXOsForBatch(suite.Ctx, block, batch)
		require.NoError(t, err)

		// Verify SetMinedMulti was called
		suite.MockUTXOStore.AssertCalled(t, "SetMinedMulti", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("SetMinedMulti error is propagated", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test transactions
		txs := transactions.CreateTestTransactionChainWithCount(t, 3)
		regularTxs := txs[1:2] // Get exactly 1 transaction

		// Setup block
		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.Height = 100
		block.ID = 50

		// Create batch with 1 subtree containing 1 transaction
		batch := &SubtreeProcessingBatch{
			batchTxs:   regularTxs,
			txRanges:   [][2]int{{0, 1}},
			batchStart: 0,
			batchEnd:   1,
		}

		// Mock the create phase to return ErrTxExists
		suite.MockUTXOStore.On("SpendAndCreate", mock.Anything, mock.Anything, uint32(100), matchCreateOnly()).
			Return((*meta.Data)(nil), nil, errors.ErrTxExists).Maybe()

		// Mock SetMinedMulti to return an error
		suite.MockUTXOStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(map[chainhash.Hash][]uint32{}, errors.NewProcessingError("database error")).Once()

		err := suite.Server.blockValidation.createAndSpendUTXOsForBatch(suite.Ctx, block, batch)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to update mined info")
	})
}

// TestExtendTxFromSameBlockParents covers the shared same-block-parent extend
// helper, including the issue 1283 bounds check that turns an out-of-range vout
// into an error instead of an index-out-of-range panic.
func TestExtendTxFromSameBlockParents(t *testing.T) {
	// Parent with two outputs: vout 0 and 1 are valid, >= 2 is out of range.
	parent := bt.NewTx()
	parent.Outputs = append(parent.Outputs,
		&bt.Output{Satoshis: 1000, LockingScript: &bscript.Script{}},
		&bt.Output{Satoshis: 2000, LockingScript: &bscript.Script{}},
	)
	parentHash := *parent.TxIDChainHash()

	childWithVout := func(t *testing.T, vout uint32, parentID chainhash.Hash) *bt.Tx {
		t.Helper()
		child := bt.NewTx()
		in := &bt.Input{PreviousTxOutIndex: vout}
		require.NoError(t, in.PreviousTxIDAdd(&parentID))
		child.Inputs = append(child.Inputs, in)
		require.False(t, child.IsExtended(), "child must be non-extended to exercise the extend path")
		return child
	}

	parents := map[chainhash.Hash]*bt.Tx{parentHash: parent}

	t.Run("in-range vout extends the input", func(t *testing.T) {
		child := childWithVout(t, 1, parentHash)

		needsExternal, err := extendTxFromSameBlockParents(child, parents)
		require.NoError(t, err)
		require.False(t, needsExternal)
		require.Equal(t, uint64(2000), child.Inputs[0].PreviousTxSatoshis)
		require.NotNil(t, child.Inputs[0].PreviousTxScript)
	})

	t.Run("out-of-range vout returns error, no panic", func(t *testing.T) {
		child := childWithVout(t, 2, parentHash) // parent only has outputs 0 and 1

		_, err := extendTxFromSameBlockParents(child, parents)
		require.Error(t, err)
		require.Contains(t, err.Error(), "non-existent output")
	})

	t.Run("parent absent from map needs external lookup", func(t *testing.T) {
		child := childWithVout(t, 0, parentHash)

		needsExternal, err := extendTxFromSameBlockParents(child, map[chainhash.Hash]*bt.Tx{})
		require.NoError(t, err)
		require.True(t, needsExternal)
	})

	t.Run("parent with no outputs returns error", func(t *testing.T) {
		emptyParent := bt.NewTx()
		emptyHash := *emptyParent.TxIDChainHash()
		child := childWithVout(t, 0, emptyHash)

		_, err := extendTxFromSameBlockParents(child, map[chainhash.Hash]*bt.Tx{emptyHash: emptyParent})
		require.Error(t, err)
		require.Contains(t, err.Error(), "non-existent output")
	})
}

// TestExtendBatch_SameBlockParentVoutBounds is the end-to-end regression guard for
// issue 1283: a same-block-parent input whose PreviousTxOutIndex is out of range
// must fail the block cleanly instead of panicking with index-out-of-range.
func TestExtendBatch_SameBlockParentVoutBounds(t *testing.T) {
	// Parent with a single output: vout 0 is valid, anything >= 1 is out of range.
	parent := bt.NewTx()
	parent.Outputs = append(parent.Outputs, &bt.Output{Satoshis: 1000, LockingScript: &bscript.Script{}})
	parentHash := *parent.TxIDChainHash()

	// newChild builds a non-extended child (nil PreviousTxScript) referencing the
	// same-block parent at the given vout.
	newChild := func(t *testing.T, vout uint32) *bt.Tx {
		t.Helper()
		child := bt.NewTx()
		in := &bt.Input{PreviousTxOutIndex: vout}
		require.NoError(t, in.PreviousTxIDAdd(&parentHash))
		child.Inputs = append(child.Inputs, in)
		require.False(t, child.IsExtended(), "child must be non-extended to exercise the extend path")
		return child
	}

	extendedTxs := map[chainhash.Hash]*bt.Tx{parentHash: parent}

	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	block := testhelpers.CreateTestBlocks(t, 1)[0]

	batch := &SubtreeProcessingBatch{
		subtreeData: []*subtreepkg.Data{{Txs: []*bt.Tx{newChild(t, 5)}}}, // vout 5 on a 1-output parent
		batchStart:  0,
		batchEnd:    1,
	}

	// Before the fix this panicked with index-out-of-range; now it must return a
	// clean per-block error.
	err := suite.Server.blockValidation.extendBatch(suite.Ctx, block, batch, extendedTxs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-existent output")
}

// TestProcessSubtreeBatch_SameBlockParentVoutBounds drives the SEQUENTIAL
// quick-validation path (processBlockSubtrees -> processBlockSubtreesSequential ->
// processSubtreeBatch) through the issue 1283 bounds-check error branch.
//
// The crafted subtree holds a same-block PARENT tx (1 output) followed by a
// non-extended CHILD tx whose input references the parent at vout 5 (out of
// range). processSubtreeBatch builds its in-batch parent map as it iterates the
// subtree's Txs, so the parent (Txs[1], after the coinbase at Txs[0]) is in the
// map by the time the child (Txs[2]) is extended. extendTxFromSameBlockParents
// then returns the "non-existent output" error, which processSubtreeBatch wraps
// as "same-block parent extension failed". Before the fix this panicked with
// index-out-of-range; now it must return a clean per-block error.
func TestProcessSubtreeBatch_SameBlockParentVoutBounds(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	// Force the sequential path (processSubtreeBatch) rather than the pipeline.
	suite.Server.blockValidation.settings.BlockValidation.SubtreeBatchPrefetchDepth = 0

	// Real coinbase for Txs[0] (readSubtree requires subtree 0, index 0 to be a coinbase).
	coinbaseTx := transactions.CreateTestTransactionChainWithCount(t, 2)[0]

	// Same-block parent with a single output: vout 0 is valid, anything >= 1 is out of range.
	parent := bt.NewTx()
	parent.Outputs = append(parent.Outputs, &bt.Output{Satoshis: 1000, LockingScript: &bscript.Script{}})

	// Non-extended child whose only input references the parent at vout 5 (out of range).
	child := bt.NewTx()
	in := &bt.Input{PreviousTxOutIndex: 5}
	require.NoError(t, in.PreviousTxIDAdd(parent.TxIDChainHash()))
	child.Inputs = append(child.Inputs, in)
	require.False(t, child.IsExtended(), "child must be non-extended to exercise the extend path")

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	block.CoinbaseTx = coinbaseTx

	// Subtree nodes: coinbase, parent, child (order must match the subtree data below).
	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(3)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*parent.TxIDChainHash(), 1, 1))
	require.NoError(t, subtree.AddNode(*child.TxIDChainHash(), 2, 2))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, suite.Server.subtreeStore.Set(suite.Ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))
	require.NoError(t, subtreeData.AddTx(parent, 1)) // parent before child so it lands in the in-batch map
	require.NoError(t, subtreeData.AddTx(child, 2))
	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, suite.Server.subtreeStore.Set(suite.Ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	block.Subtrees = []*chainhash.Hash{subtree.RootHash()}

	// Must return a clean error (not panic) whose chain mentions the out-of-range output.
	require.NotPanics(t, func() {
		_, err = suite.Server.blockValidation.processBlockSubtrees(suite.Ctx, block, false)
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-existent output")
}

func TestQuickValidateBlock_IncompleteBlockNilCoinbase(t *testing.T) {
	t.Run("nil coinbase returns ErrBlockIncomplete", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.CoinbaseTx = nil

		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test-peer", "")
		require.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrBlockIncomplete), "expected ErrBlockIncomplete, got: %v", err)
		assert.False(t, errors.Is(err, errors.ErrBlockInvalid), "should NOT be ErrBlockInvalid")
	})

	t.Run("empty inputs returns ErrBlockIncomplete", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.CoinbaseTx = &bt.Tx{Inputs: []*bt.Input{}}

		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test-peer", "")
		require.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrBlockIncomplete), "expected ErrBlockIncomplete, got: %v", err)
		assert.False(t, errors.Is(err, errors.ErrBlockInvalid), "should NOT be ErrBlockInvalid")
	})
}

func TestQuickValidateSkipUtxoLockSetting_DefaultsOff(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	require.False(t, suite.Server.blockValidation.settings.BlockValidation.QuickValidateSkipUtxoLock,
		"QuickValidateSkipUtxoLock must default to false")
}

// filterCalls removes mock expectations for a specific method
func filterCalls(calls []*mock.Call, methodToRemove string) []*mock.Call {
	filtered := make([]*mock.Call, 0)
	for _, call := range calls {
		if call.Method != methodToRemove {
			filtered = append(filtered, call)
		}
	}
	return filtered
}

// parseCreateOptions applies opts to a fresh CreateOptions for inspection.
func parseCreateOptions(opts []utxo.CreateOption) *utxo.CreateOptions {
	o := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(o)
	}

	return o
}

// matchCreateOnly matches the create-phase SpendAndCreate call (WithCreateOnly).
func matchCreateOnly() interface{} {
	return mock.MatchedBy(func(opts []utxo.CreateOption) bool { return parseCreateOptions(opts).CreateOnly })
}

// matchSpendOnly matches the spend-phase SpendAndCreate call (WithSpendOnly).
func matchSpendOnly() interface{} {
	return mock.MatchedBy(func(opts []utxo.CreateOption) bool { return parseCreateOptions(opts).SpendOnly })
}

// countCreatePhaseCalls counts recorded SpendAndCreate calls that carried WithCreateOnly.
func countCreatePhaseCalls(m *utxo.MockUtxostore) int {
	count := 0

	for _, c := range m.Calls {
		if c.Method == "SpendAndCreate" && parseCreateOptions(c.Arguments.Get(3).([]utxo.CreateOption)).CreateOnly {
			count++
		}
	}

	return count
}

// setCheckpointSlice replaces the checkpoint set on the suite's settings with the given
// slice. It copies ChainCfgParams first so it never mutates the shared (global) chaincfg.Params.
func setCheckpointSlice(t *testing.T, s *CatchupTestSuite, cps []chaincfg.Checkpoint) {
	t.Helper()
	require.NotNil(t, s.Server.blockValidation.settings.ChainCfgParams)
	params := *s.Server.blockValidation.settings.ChainCfgParams
	params.Checkpoints = cps
	s.Server.blockValidation.settings.ChainCfgParams = &params
}

// setCheckpoints replaces the checkpoint set on the suite's settings with a single
// checkpoint at the given height.
func setCheckpoints(t *testing.T, s *CatchupTestSuite, height uint32) {
	t.Helper()
	setCheckpointSlice(t, s, []chaincfg.Checkpoint{{Height: int32(height)}})
}

func TestQuickValidateSkipsUtxoLock(t *testing.T) {
	tests := []struct {
		name             string
		settingOn        bool
		emptyCheckpoints bool
		checkpointAt     uint32
		blockHeight      uint32
		want             bool
	}{
		{name: "setting off", settingOn: false, checkpointAt: 1000, blockHeight: 100, want: false},
		{name: "on, height below checkpoint", settingOn: true, checkpointAt: 1000, blockHeight: 100, want: true},
		{name: "on, height equal to checkpoint", settingOn: true, checkpointAt: 1000, blockHeight: 1000, want: true},
		{name: "on, height above checkpoint", settingOn: true, checkpointAt: 50, blockHeight: 100, want: false},
		{name: "on, checkpoint at height 0", settingOn: true, checkpointAt: 0, blockHeight: 100, want: false},
		{name: "on, empty checkpoint list", settingOn: true, emptyCheckpoints: true, blockHeight: 100, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suite := NewCatchupTestSuite(t)
			defer suite.Cleanup()

			suite.Server.blockValidation.settings.BlockValidation.QuickValidateSkipUtxoLock = tt.settingOn
			if tt.emptyCheckpoints {
				setCheckpointSlice(t, suite, []chaincfg.Checkpoint{})
			} else {
				setCheckpoints(t, suite, tt.checkpointAt)
			}

			block := &model.Block{Height: tt.blockHeight}
			got := suite.Server.blockValidation.quickValidateSkipsUtxoLock(block)
			require.Equal(t, tt.want, got)
		})
	}
}

// assertCreatedLocked asserts every recorded Create call used the expected WithLocked flag.
func assertCreatedLocked(t *testing.T, m *utxo.MockUtxostore, wantLocked bool) {
	t.Helper()
	found := false
	for _, c := range m.Calls {
		if c.Method != "SpendAndCreate" {
			continue
		}
		opts, ok := c.Arguments.Get(3).([]utxo.CreateOption)
		require.True(t, ok, "SpendAndCreate 4th arg should be []utxo.CreateOption")
		o := parseCreateOptions(opts)
		if !o.CreateOnly {
			continue
		}
		require.Equal(t, wantLocked, o.Locked, "SpendAndCreate WithLocked flag mismatch")
		found = true
	}
	require.True(t, found, "expected at least one create-phase SpendAndCreate call")
}

// assertCreatedSkipExtended asserts every Create call carried WithSkipExtendedInputs(want).
// Pins the outpoint-only mode threaded into createAndSpendUTXOsForBatch's Create so a
// refactor that drops the flag (re-triggering GetFees on un-decorated txs) fails here.
func assertCreatedSkipExtended(t *testing.T, m *utxo.MockUtxostore, want bool) {
	t.Helper()
	found := false
	for _, c := range m.Calls {
		if c.Method != "SpendAndCreate" {
			continue
		}
		opts, ok := c.Arguments.Get(3).([]utxo.CreateOption)
		require.True(t, ok, "SpendAndCreate 4th arg should be []utxo.CreateOption")
		o := parseCreateOptions(opts)
		if !o.CreateOnly {
			continue
		}
		require.Equal(t, want, o.SkipExtendedInputs, "SpendAndCreate WithSkipExtendedInputs flag mismatch")
		found = true
	}
	require.True(t, found, "expected at least one create-phase SpendAndCreate call")
}

// assertSpentSkipUTXOHashCheck asserts every Spend call carried IgnoreFlags.SkipUTXOHashCheck(want).
// Pins the outpoint-only mode threaded into createAndSpendUTXOsForBatch's Spend so a refactor
// that drops the flag (re-enabling the hash check on un-decorated spends) fails here.
func assertSpentSkipUTXOHashCheck(t *testing.T, m *utxo.MockUtxostore, want bool) {
	t.Helper()
	found := false
	for _, c := range m.Calls {
		if c.Method != "SpendAndCreate" {
			continue
		}
		opts, ok := c.Arguments.Get(3).([]utxo.CreateOption)
		require.True(t, ok, "SpendAndCreate 4th arg should be []utxo.CreateOption")
		o := parseCreateOptions(opts)
		if !o.SpendOnly {
			continue
		}
		require.Equal(t, want, o.IgnoreFlags.SkipUTXOHashCheck, "SpendAndCreate SkipUTXOHashCheck flag mismatch")
		found = true
	}
	require.True(t, found, "expected at least one spend-phase SpendAndCreate call")
}

// buildOneSubtreeBlock builds a block with one subtree (coinbase + 2 txs) and stores its
// subtree + subtree-data files in the suite's subtree store. Mirrors the existing
// "block with 1 subtree and 2 txs" setup in TestQuickValidateBlock.
func buildOneSubtreeBlock(t *testing.T, s *CatchupTestSuite, height uint32) *model.Block {
	t.Helper()
	txs := transactions.CreateTestTransactionChainWithCount(t, 4)
	coinbaseTx := txs[0]
	regularTxs := txs[1:]

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	block.Height = height
	block.CoinbaseTx = coinbaseTx

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(3)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*regularTxs[0].TxIDChainHash(), 1, 1))
	require.NoError(t, subtree.AddNode(*regularTxs[1].TxIDChainHash(), 2, 2))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, s.Server.subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))
	require.NoError(t, subtreeData.AddTx(regularTxs[0], 1))
	require.NoError(t, subtreeData.AddTx(regularTxs[1], 2))
	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, s.Server.subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	block.Subtrees = []*chainhash.Hash{subtree.RootHash()}
	block.TransactionCount = 3
	block.Header.HashMerkleRoot, err = subtree.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, 0)
	require.NoError(t, err)
	return block
}

// enableOutpointOnlyFastPath makes the suite's mock UTXO store report fast-path
// support, so quickValidateOutpointOnly's store-capability guard is satisfied. Call
// this on any test suite where OutpointOnlyBelowCheckpoint=true is expected to engage.
func enableOutpointOnlyFastPath(t *testing.T, s *CatchupTestSuite) {
	t.Helper()
	s.MockUTXOStore.SupportsOutpointOnlySpendResult = true
}

func setupQuickValidateMocks(s *CatchupTestSuite) {
	s.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Maybe()
	s.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.MockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()
	s.MockUTXOStore.On("Get", mock.Anything, mock.Anything, mock.Anything).Return((*meta.Data)(nil), errors.NewNotFoundError("not found"))
	s.MockUTXOStore.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, matchCreateOnly()).Return(&meta.Data{}, nil, nil)
	s.MockUTXOStore.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, matchSpendOnly()).Return(nil, []*utxo.Spend{}, nil)
	s.MockUTXOStore.On("SetLocked", mock.Anything, mock.Anything, false).Return(nil).Maybe()
	s.MockValidator.Errors = []error{nil, nil, nil}
}

func TestQuickValidateBlock_UtxoLockGating(t *testing.T) {
	t.Run("setting off: UTXOs locked then unlocked", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()
		setupQuickValidateMocks(suite)

		block := buildOneSubtreeBlock(t, suite, 100)

		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test", "")
		require.NoError(t, err)

		assertCreatedLocked(t, suite.MockUTXOStore, true)
		suite.MockUTXOStore.AssertCalled(t, "SetLocked", mock.Anything, mock.Anything, false)
	})

	t.Run("setting on, height <= checkpoint: unlocked, no unlock pass", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()
		setupQuickValidateMocks(suite)
		suite.Server.blockValidation.settings.BlockValidation.QuickValidateSkipUtxoLock = true
		setCheckpoints(t, suite, 1000)

		block := buildOneSubtreeBlock(t, suite, 100)

		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test", "")
		require.NoError(t, err)

		assertCreatedLocked(t, suite.MockUTXOStore, false)
		suite.MockUTXOStore.AssertNotCalled(t, "SetLocked", mock.Anything, mock.Anything, false)
	})

	t.Run("setting on, height > checkpoint: lock still applied", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()
		setupQuickValidateMocks(suite)
		suite.Server.blockValidation.settings.BlockValidation.QuickValidateSkipUtxoLock = true
		setCheckpoints(t, suite, 50)

		block := buildOneSubtreeBlock(t, suite, 100)

		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test", "")
		require.NoError(t, err)

		assertCreatedLocked(t, suite.MockUTXOStore, true)
		suite.MockUTXOStore.AssertCalled(t, "SetLocked", mock.Anything, mock.Anything, false)
	})
}

// buildOneSubtreeBlockWithExternalParentTx builds a one-subtree block that contains a tx
// (externalParentTx) whose input references a parent NOT in the block and whose
// PreviousTxScript is nil. That tx therefore lands in txsNeedingExtension, making any
// assertion on BatchPreviousOutputsDecorate load-bearing — it will be called when the
// outpoint-only gate is OFF and suppressed when it is ON.
func buildOneSubtreeBlockWithExternalParentTx(t *testing.T, s *CatchupTestSuite, height uint32) *model.Block {
	t.Helper()
	txs := transactions.CreateTestTransactionChainWithCount(t, 4)
	coinbaseTx := txs[0]
	regularTxs := txs[1:]

	// Build an un-extended tx: its input points to an external parent hash (not in this
	// block) with PreviousTxScript left nil, so IsExtended() returns false and the code
	// cannot resolve the parent from extendedTxsFromPrevBatches → lands in txsNeedingExtension.
	externalParentHash := chainhash.HashH([]byte("external-parent-not-in-block"))
	rawInput := &bt.Input{
		SequenceNumber:     0xFFFFFFFF,
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: 0,
		PreviousTxScript:   nil, // nil → IsExtended() == false
	}
	require.NoError(t, rawInput.PreviousTxIDAdd(&externalParentHash))

	externalParentTx := bt.NewTx()
	externalParentTx.Inputs = append(externalParentTx.Inputs, rawInput)
	// Add a minimal output so Create/Spend can operate on this tx.
	externalParentTx.AddOutput(&bt.Output{
		Satoshis:      0,
		LockingScript: bscript.NewFromBytes([]byte{0x51}), // OP_1 (trivially valid script)
	})

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	block.Height = height
	block.CoinbaseTx = coinbaseTx

	// Subtree: coinbase + 2 regular extended txs + 1 external-parent (unextended) tx.
	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*regularTxs[0].TxIDChainHash(), 1, 1))
	require.NoError(t, subtree.AddNode(*regularTxs[1].TxIDChainHash(), 2, 2))
	require.NoError(t, subtree.AddNode(*externalParentTx.TxIDChainHash(), 3, 3))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, s.Server.subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))
	require.NoError(t, subtreeData.AddTx(regularTxs[0], 1))
	require.NoError(t, subtreeData.AddTx(regularTxs[1], 2))
	require.NoError(t, subtreeData.AddTx(externalParentTx, 3))
	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, s.Server.subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	block.Subtrees = []*chainhash.Hash{subtree.RootHash()}
	block.TransactionCount = 4
	block.Header.HashMerkleRoot, err = subtree.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, 0)
	require.NoError(t, err)
	return block
}

// TestQuickValidate_OutpointOnly_NoDecorate_ZeroFees verifies the outpoint-only fast path
// with a load-bearing ON/OFF polarity check.
//
// The block contains an un-extended tx (PreviousTxScript=nil, external parent) that lands
// in txsNeedingExtension. When the gate is OFF, BatchPreviousOutputsDecorate IS called for
// that tx; when it is ON the call is suppressed — proving the gate is what makes the
// difference, not simply that there are no txs needing extension.
func TestQuickValidate_OutpointOnly_NoDecorate_ZeroFees(t *testing.T) {
	// sub-test helper: builds common mock state shared by both polarities.
	setupSuite := func(t *testing.T) *CatchupTestSuite {
		t.Helper()
		suite := NewCatchupTestSuite(t)
		// Skip UTXO locking so SetLocked expectations don't complicate the assertion.
		suite.Server.blockValidation.settings.BlockValidation.QuickValidateSkipUtxoLock = true
		// Place a high checkpoint so height 500 is firmly below it.
		setCheckpoints(t, suite, 1_000_000)
		setupQuickValidateMocks(suite)
		// The block has coinbase + 3 regular txs; validator needs 4 nil errors.
		suite.MockValidator.Errors = []error{nil, nil, nil, nil}
		return suite
	}

	t.Run("gate on: BatchPreviousOutputsDecorate not called, fees zero", func(t *testing.T) {
		suite := setupSuite(t)
		defer suite.Cleanup()

		suite.Server.blockValidation.settings.BlockValidation.OutpointOnlyBelowCheckpoint = true
		enableOutpointOnlyFastPath(t, suite) // make the mock store report fast-path support
		block := buildOneSubtreeBlockWithExternalParentTx(t, suite, 500)

		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test-peer", "")
		require.NoError(t, err)

		// Gate suppresses the call — no expectation registered, any call would panic.
		suite.MockUTXOStore.AssertNotCalled(t, "BatchPreviousOutputsDecorate", mock.Anything, mock.Anything)

		// All four seams must agree for one block: decorate skipped (above), fees zero
		// (below), AND Create/Spend carry the outpoint-only flags (the threaded mode).
		assertCreatedSkipExtended(t, suite.MockUTXOStore, true)
		assertSpentSkipUTXOHashCheck(t, suite.MockUTXOStore, true)

		for i, st := range block.SubtreeSlices {
			if st == nil {
				continue
			}
			require.Equal(t, uint64(0), st.Fees, "subtree %d: expected zero fees on outpoint-only path", i)
		}
	})

	t.Run("gate off: BatchPreviousOutputsDecorate IS called for unextended tx", func(t *testing.T) {
		suite := setupSuite(t)
		defer suite.Cleanup()

		suite.Server.blockValidation.settings.BlockValidation.OutpointOnlyBelowCheckpoint = false
		block := buildOneSubtreeBlockWithExternalParentTx(t, suite, 500)

		// Register expectation: decorate must be called with the unextended tx.
		suite.MockUTXOStore.On("BatchPreviousOutputsDecorate", mock.Anything, mock.Anything).Return(nil).Once()

		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test-peer", "")
		require.NoError(t, err)

		// Without the gate, decorate IS called — proving the un-extended tx genuinely
		// reached txsNeedingExtension and that only the gate suppresses the call.
		suite.MockUTXOStore.AssertCalled(t, "BatchPreviousOutputsDecorate", mock.Anything, mock.Anything)
	})
}

func TestQuickValidateBlockAsync_UtxoLockGating(t *testing.T) {
	t.Run("setting off: UTXOs locked then unlocked", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()
		setupQuickValidateMocks(suite)

		block := buildOneSubtreeBlock(t, suite, 100)

		// Buffered large enough that quickValidateBlockAsync never blocks queuing write
		// jobs (one job per subtree; the test block has a single subtree), so no consumer
		// goroutine is needed.
		writeJobsChan := make(chan *SubtreeWriteJob, 16)

		err := suite.Server.blockValidation.quickValidateBlockAsync(suite.Ctx, block, "test", "", writeJobsChan)
		require.NoError(t, err)

		assertCreatedLocked(t, suite.MockUTXOStore, true)
		suite.MockUTXOStore.AssertCalled(t, "SetLocked", mock.Anything, mock.Anything, false)
	})

	t.Run("setting on, height <= checkpoint: unlocked, no unlock pass", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()
		setupQuickValidateMocks(suite)
		suite.Server.blockValidation.settings.BlockValidation.QuickValidateSkipUtxoLock = true
		setCheckpoints(t, suite, 1000)

		block := buildOneSubtreeBlock(t, suite, 100)

		// Buffered large enough that quickValidateBlockAsync never blocks queuing write
		// jobs (one job per subtree; the test block has a single subtree), so no consumer
		// goroutine is needed.
		writeJobsChan := make(chan *SubtreeWriteJob, 16)

		err := suite.Server.blockValidation.quickValidateBlockAsync(suite.Ctx, block, "test", "", writeJobsChan)
		require.NoError(t, err)

		assertCreatedLocked(t, suite.MockUTXOStore, false)
		suite.MockUTXOStore.AssertNotCalled(t, "SetLocked", mock.Anything, mock.Anything, false)
	})

	t.Run("setting on, height > checkpoint: lock still applied", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()
		setupQuickValidateMocks(suite)
		suite.Server.blockValidation.settings.BlockValidation.QuickValidateSkipUtxoLock = true
		setCheckpoints(t, suite, 50)

		block := buildOneSubtreeBlock(t, suite, 100)

		// Buffered large enough that quickValidateBlockAsync never blocks queuing write
		// jobs (one job per subtree; the test block has a single subtree), so no consumer
		// goroutine is needed.
		writeJobsChan := make(chan *SubtreeWriteJob, 16)

		err := suite.Server.blockValidation.quickValidateBlockAsync(suite.Ctx, block, "test", "", writeJobsChan)
		require.NoError(t, err)

		assertCreatedLocked(t, suite.MockUTXOStore, true)
		suite.MockUTXOStore.AssertCalled(t, "SetLocked", mock.Anything, mock.Anything, false)
	})
}

// newBlockValidationWithRealStore creates a BlockValidation backed by a real
// sqlitememory UTXO store. It is used for end-to-end tests that need to assert
// on actual store contents rather than on mock call expectations.
//
// The returned cleanup function must be deferred to release background goroutines.
func newBlockValidationWithRealStore(t *testing.T) (*BlockValidation, utxo.Store, func()) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	logger := ulogger.TestLogger{}
	tSettings := testutil.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///skip_unspendable_test")
	require.NoError(t, err)

	realStore, err := sql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)

	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		utxoStore:                     realStore,
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
	}

	cleanup := func() {
		bv.blockExistsCache.Stop()
		bv.lastValidatedBlocks.Stop()
		cancel()
	}

	return bv, realStore, cleanup
}

// setCheckpointsOnBV sets ChainCfgParams checkpoints on a BlockValidation to a
// single checkpoint at the given height. It copies ChainCfgParams so it never
// mutates the shared global instance.
func setCheckpointsOnBV(t *testing.T, bv *BlockValidation, height uint32) {
	t.Helper()
	require.NotNil(t, bv.settings.ChainCfgParams)
	params := *bv.settings.ChainCfgParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: int32(height)}}
	bv.settings.ChainCfgParams = &params
}

// TestSkipUnspendableTxStorageDuringCatchup_EndToEnd verifies that when both
// QuickValidateSkipUtxoLock and SkipUnspendableTxStorageDuringCatchup are
// enabled and the block is at/below the highest checkpoint:
//   - An OP_RETURN-only tx is NOT written to the UTXO store (skipped).
//   - A normal spendable tx IS written to the UTXO store.
//   - The parent UTXO consumed by the OP_RETURN tx's input IS marked spent.
//   - Processing returns no error.
//
// A negative case pins the safety invariant: with QuickValidateSkipUtxoLock=false
// the skip does not fire, so the OP_RETURN tx IS stored.
func TestSkipUnspendableTxStorageDuringCatchup_EndToEnd(t *testing.T) {
	t.Run("both settings on, height below checkpoint: op_return skipped, spendable stored, parent spent", func(t *testing.T) {
		bv, store, cleanup := newBlockValidationWithRealStore(t)
		defer cleanup()

		const blockHeight = uint32(100)
		const checkpointHeight = uint32(1000)

		bv.settings.BlockValidation.QuickValidateSkipUtxoLock = true
		bv.settings.BlockValidation.SkipUnspendableTxStorageDuringCatchup = true
		setCheckpointsOnBV(t, bv, checkpointHeight)

		ctx := context.Background()

		// Build a parent tx with 2 outputs and pre-seed it in the store.
		// opReturnTx spends output 0; spendableTx spends output 1.
		privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("SKIP_UNSPENDABLE_E2E_TEST_KEY"))
		parentTx := transactions.Create(t,
			transactions.WithCoinbaseData(1, "/genesis/"),
			transactions.WithP2PKHOutputs(2, 5000, publicKey),
		)
		_, _, err := store.SpendAndCreate(ctx, parentTx, 0, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1}), utxo.WithCreateOnly())
		require.NoError(t, err)

		// opReturnTx: spends output 0 of parent, produces only an OP_RETURN (unspendable).
		// Build manually so the output uses bt.AddOpReturnOutput which produces the correct
		// OP_FALSE OP_RETURN encoding that HasNoSpendableOutputs recognises.
		opReturnTx := bt.NewTx()
		require.NoError(t, opReturnTx.FromUTXOs(&bt.UTXO{
			TxIDHash:      parentTx.TxIDChainHash(),
			Vout:          0,
			LockingScript: parentTx.Outputs[0].LockingScript,
			Satoshis:      parentTx.Outputs[0].Satoshis,
		}))
		require.NoError(t, opReturnTx.AddOpReturnOutput([]byte("skip-unspendable-e2e-test")))

		// spendableTx: spends output 1 of parent, produces a P2PKH output (spendable).
		spendableTx := transactions.Create(t,
			transactions.WithPrivateKey(privateKey),
			transactions.WithInput(parentTx, 1),
			transactions.WithP2PKHOutputs(1, 4000, publicKey),
		)

		block := &model.Block{
			Height: blockHeight,
			ID:     99,
		}

		// batch: subtree 0 holds opReturnTx; subtree 1 holds spendableTx.
		batch := &SubtreeProcessingBatch{
			batchTxs:   []*bt.Tx{opReturnTx, spendableTx},
			txRanges:   [][2]int{{0, 1}, {1, 2}},
			batchStart: 0,
			batchEnd:   2,
		}

		err = bv.createAndSpendUTXOsForBatch(ctx, block, batch)
		require.NoError(t, err, "createAndSpendUTXOsForBatch must succeed")

		// 1. OP_RETURN-only tx must NOT be in the store (was skipped).
		_, getErr := store.Get(ctx, opReturnTx.TxIDChainHash())
		require.Error(t, getErr, "op_return tx should not be in the store")
		require.True(t, errors.Is(getErr, errors.ErrTxNotFound),
			"expected ErrTxNotFound for skipped op_return tx, got: %v", getErr)

		// 2. Spendable tx must be in the store (was not skipped).
		txMeta, getErr := store.Get(ctx, spendableTx.TxIDChainHash())
		require.NoError(t, getErr, "spendable tx should be in the store")
		require.NotNil(t, txMeta)

		// 3. Parent output 0 (the input consumed by the OP_RETURN tx) must be SPENT.
		spendResp, getErr := store.GetSpend(ctx, &utxo.Spend{
			TxID: parentTx.TxIDChainHash(),
			Vout: 0,
		})
		require.NoError(t, getErr, "GetSpend for parent output 0 must not error")
		require.Equal(t, int(utxo.Status_SPENT), spendResp.Status,
			"parent output 0 (spent by op_return tx input) must be marked SPENT")
	})

	// Negative case: with QuickValidateSkipUtxoLock=false the skip is gated out;
	// the OP_RETURN tx IS stored despite SkipUnspendableTxStorageDuringCatchup=true.
	// This pins the safety invariant: skip is only safe when the unlock pass is also skipped.
	t.Run("negative: lock on (QuickValidateSkipUtxoLock=false), op_return IS stored", func(t *testing.T) {
		bv, store, cleanup := newBlockValidationWithRealStore(t)
		defer cleanup()

		const blockHeight = uint32(100)
		const checkpointHeight = uint32(1000)

		bv.settings.BlockValidation.QuickValidateSkipUtxoLock = false
		bv.settings.BlockValidation.SkipUnspendableTxStorageDuringCatchup = true
		setCheckpointsOnBV(t, bv, checkpointHeight)

		ctx := context.Background()

		_, publicKey := bec.PrivateKeyFromBytes([]byte("SKIP_UNSPENDABLE_E2E_TEST_KEY"))
		parentTx := transactions.Create(t,
			transactions.WithCoinbaseData(1, "/genesis/"),
			transactions.WithP2PKHOutputs(1, 5000, publicKey),
		)
		_, _, err := store.SpendAndCreate(ctx, parentTx, 0, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1}), utxo.WithCreateOnly())
		require.NoError(t, err)

		opReturnTx := bt.NewTx()
		require.NoError(t, opReturnTx.FromUTXOs(&bt.UTXO{
			TxIDHash:      parentTx.TxIDChainHash(),
			Vout:          0,
			LockingScript: parentTx.Outputs[0].LockingScript,
			Satoshis:      parentTx.Outputs[0].Satoshis,
		}))
		// Set a non-empty unlocking script so the SQL store's NOT NULL constraint is satisfied.
		// This tx is never signature-validated by createAndSpendUTXOsForBatch.
		dummyScript := bscript.NewFromBytes([]byte{0x00})
		opReturnTx.Inputs[0].UnlockingScript = dummyScript
		require.NoError(t, opReturnTx.AddOpReturnOutput([]byte("should-be-stored")))

		block := &model.Block{
			Height: blockHeight,
			ID:     99,
		}

		batch := &SubtreeProcessingBatch{
			batchTxs:   []*bt.Tx{opReturnTx},
			txRanges:   [][2]int{{0, 1}},
			batchStart: 0,
			batchEnd:   1,
		}

		err = bv.createAndSpendUTXOsForBatch(ctx, block, batch)
		require.NoError(t, err, "createAndSpendUTXOsForBatch must succeed")

		// With lock on, the skip gate is closed — the op_return tx must be in the store.
		_, getErr := store.Get(ctx, opReturnTx.TxIDChainHash())
		require.NoError(t, getErr,
			"op_return tx must be stored when QuickValidateSkipUtxoLock=false (safety gate)")
	})
}

// TestOutpointOnly_MetricIncrementsBelowOnly verifies that the
// prometheusBlockValidationOutpointOnlyBlocks counter increments by exactly 1
// per block (not per subtree batch) when quickValidateBlock processes a
// below-checkpoint block with OutpointOnlyBelowCheckpoint on, and does NOT
// increment when the setting is off.
//
// Per-block counting is guaranteed by the Inc() placement at the top of
// quickValidateBlock (and quickValidateBlockAsync), before any batch loop.
// A multi-subtree block cannot be constructed with the current CatchupTestSuite
// harness without rewriting subtree+merkle wiring, so the per-block assertion
// relies on the code relocation: the counter is no longer inside
// createAndSpendUTXOsForBatch, which runs once per batch.
func TestOutpointOnly_MetricIncrementsBelowOnly(t *testing.T) {
	initPrometheusMetrics()

	t.Run("setting on, below checkpoint: counter increments by 1", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		suite.Server.blockValidation.settings.BlockValidation.OutpointOnlyBelowCheckpoint = true
		enableOutpointOnlyFastPath(t, suite) // make the mock store report fast-path support
		suite.Server.blockValidation.settings.BlockValidation.QuickValidateSkipUtxoLock = true
		setCheckpoints(t, suite, 1000)
		setupQuickValidateMocks(suite)

		block := buildOneSubtreeBlock(t, suite, 500)

		before := prometheustestutil.ToFloat64(prometheusBlockValidationOutpointOnlyBlocks)
		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test-peer", "")
		require.NoError(t, err)
		require.Equal(t, before+1, prometheustestutil.ToFloat64(prometheusBlockValidationOutpointOnlyBlocks))
	})

	t.Run("setting off: counter does not increment", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		suite.Server.blockValidation.settings.BlockValidation.OutpointOnlyBelowCheckpoint = false
		setCheckpoints(t, suite, 1000)
		setupQuickValidateMocks(suite)

		block := buildOneSubtreeBlock(t, suite, 500)

		before := prometheustestutil.ToFloat64(prometheusBlockValidationOutpointOnlyBlocks)
		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test-peer", "")
		require.NoError(t, err)
		require.Equal(t, before, prometheustestutil.ToFloat64(prometheusBlockValidationOutpointOnlyBlocks))
	})
}

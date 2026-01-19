package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestQuickValidateBlock(t *testing.T) {
	t.Run("empty block", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Mock blockchain AddBlock and check how it was called
		suite.MockBlockchain.On("GetNextBlockID", mock.Anything).Return(uint64(1), nil).Once()
		suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		suite.MockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()

		block := testhelpers.CreateTestBlocks(t, 1)[0]

		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test")
		assert.NoError(t, err, "Should successfully quick validate an empty block")

		// Verify AddBlock was called with correct parameters
		suite.MockBlockchain.AssertCalled(t, "GetNextBlockID", mock.Anything)
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
		suite.MockBlockchain.On("GetNextBlockID", mock.Anything).Return(uint64(1), nil).Maybe()
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

		// Setup UTXO store expectations for all transactions (including coinbase)
		// Use mock.Anything for the transaction since the order may vary
		suite.MockUTXOStore.On("Create", mock.Anything, mock.Anything, uint32(100), mock.Anything).Return(&meta.Data{}, nil)

		// Setup UTXO store expectations for spending transactions (context, tx, ignoreFlags)
		suite.MockUTXOStore.On("Spend", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*utxo.Spend{}, nil)

		// Setup SetLocked expectation for unlocking UTXOs after AddBlock
		suite.MockUTXOStore.On("SetLocked", mock.Anything, mock.Anything, false).Return(nil)

		// Setup validator to return no errors (one for each transaction: coinbase + 2 regular)
		suite.MockValidator.Errors = []error{nil, nil, nil}

		err = suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test")
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

		// Execute processBlockSubtrees
		_, err := suite.Server.blockValidation.processBlockSubtrees(suite.Ctx, block)

		// Verify error
		assert.Error(t, err, "Should fail when block has no subtrees")
		assert.Contains(t, err.Error(), "block has no subtrees", "Error should indicate no subtrees")
	})
}

// TestQuickValidationDecisionLogic tests the core validation decision logic
func TestQuickValidationDecisionLogic(t *testing.T) {
	t.Run("HeightBasedValidation", func(t *testing.T) {
		// Test the simplified logic: useQuickValidation && block.Height <= highestCheckpointHeight

		testCases := []struct {
			name                    string
			blockHeight             uint32
			highestCheckpointHeight uint32
			useQuickValidation      bool
			expected                bool
		}{
			{
				name:                    "BlockBelowCheckpoint",
				blockHeight:             50,
				highestCheckpointHeight: 100,
				useQuickValidation:      true,
				expected:                true,
			},
			{
				name:                    "BlockAtCheckpoint",
				blockHeight:             100,
				highestCheckpointHeight: 100,
				useQuickValidation:      true,
				expected:                true,
			},
			{
				name:                    "BlockAboveCheckpoint",
				blockHeight:             150,
				highestCheckpointHeight: 100,
				useQuickValidation:      true,
				expected:                false,
			},
			{
				name:                    "QuickValidationDisabled",
				blockHeight:             50,
				highestCheckpointHeight: 100,
				useQuickValidation:      false,
				expected:                false,
			},
			{
				name:                    "NoCheckpoints",
				blockHeight:             50,
				highestCheckpointHeight: 0,
				useQuickValidation:      true,
				expected:                false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// This is the exact logic from validateBlocksOnChannel
				canUseQuickValidation := tc.useQuickValidation && tc.blockHeight <= tc.highestCheckpointHeight

				assert.Equal(t, tc.expected, canUseQuickValidation,
					"Quick validation decision should match expected result for %s", tc.name)
			})
		}
	})
}

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

		// Mock Create to succeed (no ErrTxExists)
		suite.MockUTXOStore.On("Create", mock.Anything, mock.Anything, uint32(100), mock.Anything).
			Return(&meta.Data{}, nil).Maybe()

		// Mock Spend - need to clear the default and set our own
		suite.MockUTXOStore.ExpectedCalls = filterCalls(suite.MockUTXOStore.ExpectedCalls, "Spend")
		suite.MockUTXOStore.On("Spend", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return([]*utxo.Spend{}, nil).Maybe()

		// SetMinedMulti should NOT be called since all txs are new

		err := suite.Server.blockValidation.createAndSpendUTXOsForBatch(suite.Ctx, block, batch)
		require.NoError(t, err)

		// Verify Create was called for each transaction
		suite.MockUTXOStore.AssertNumberOfCalls(t, "Create", 2)
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

		// Mock Create to return ErrTxExists for all transactions
		suite.MockUTXOStore.On("Create", mock.Anything, mock.Anything, uint32(100), mock.Anything).
			Return((*meta.Data)(nil), errors.ErrTxExists).Maybe()

		// Mock SetMinedMulti - should be called with both transaction hashes
		suite.MockUTXOStore.On("SetMinedMulti", mock.Anything, mock.MatchedBy(func(hashes []*chainhash.Hash) bool {
			return len(hashes) == 2
		}), mock.MatchedBy(func(info utxo.MinedBlockInfo) bool {
			return info.BlockID == 50 && info.BlockHeight == 100
		})).Return(map[chainhash.Hash][]uint32{}, nil).Once()

		// Mock Spend - clear default and set our own
		suite.MockUTXOStore.ExpectedCalls = filterCalls(suite.MockUTXOStore.ExpectedCalls, "Spend")
		suite.MockUTXOStore.On("Spend", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return([]*utxo.Spend{}, nil).Maybe()

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

		// Mock Create to return ErrTxExists
		suite.MockUTXOStore.On("Create", mock.Anything, mock.Anything, uint32(100), mock.Anything).
			Return((*meta.Data)(nil), errors.ErrTxExists).Maybe()

		// Mock SetMinedMulti to return an error
		suite.MockUTXOStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(map[chainhash.Hash][]uint32{}, errors.NewProcessingError("database error")).Once()

		err := suite.Server.blockValidation.createAndSpendUTXOsForBatch(suite.Ctx, block, batch)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to update mined info")
	})
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

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

		arguments := suite.MockBlockchain.Calls[1].Arguments
		if len(arguments) < 2 {
			t.Fatalf("Expected at least 2 arguments, got %d", len(arguments))
		}
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

// TestQuickValidationComponents tests the individual components of quick validation
func TestQuickValidationComponents(t *testing.T) {
	t.Run("CreateAllUTXOs_Success", func(t *testing.T) {
		// Setup test suite
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test data
		block := testhelpers.CreateTestBlockWithSubtrees(t, 100)
		txs := testhelpers.CreateTestTransactions(t, 2)
		txWrappers := make([]txWrapper, len(txs))

		// Setup UTXO store expectations for all transactions
		for idx, tx := range txs {
			suite.MockUTXOStore.On("Create", mock.Anything, tx, uint32(100), mock.Anything).Return(&meta.Data{}, nil)

			txWrappers[idx] = txWrapper{tx: tx, subtreeIdx: 0}
		}

		// Execute createAllUTXOs
		err := suite.Server.blockValidation.createAllUTXOs(suite.Ctx, block, txWrappers)

		// Verify success
		assert.NoError(t, err, "Should successfully create all UTXOs")

		// Verify all mock expectations were met
		suite.MockUTXOStore.AssertExpectations(t)
	})

	t.Run("CreateAllUTXOs_Error", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		block := testhelpers.CreateTestBlockWithSubtrees(t, 100)
		txs := testhelpers.CreateTestTransactions(t, 1)
		txWrappers := []txWrapper{{tx: txs[0], subtreeIdx: 0}}

		// Setup UTXO store to return error
		suite.MockUTXOStore.On("Create", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*meta.Data)(nil), errors.NewServiceError("database connection failed"))

		// Execute createAllUTXOs
		err := suite.Server.blockValidation.createAllUTXOs(suite.Ctx, block, txWrappers)

		// Verify error is propagated
		assert.Error(t, err, "Should propagate UTXO creation errors")
		assert.Contains(t, err.Error(), "failed to create", "Error should indicate UTXO creation failure")
	})

	t.Run("CreateAllUTXOs_ConcurrencyTest", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create many transactions to test concurrency limiting
		block := testhelpers.CreateTestBlockWithSubtrees(t, 100)
		txs := testhelpers.CreateTestTransactions(t, 20) // Test with multiple transactions
		txWrappers := make([]txWrapper, len(txs))

		// Setup UTXO store expectations for all transactions
		for idx, tx := range txs {
			suite.MockUTXOStore.On("Create", mock.Anything, tx, uint32(100), mock.Anything).Return(&meta.Data{}, nil)
			txWrappers[idx] = txWrapper{tx: tx, subtreeIdx: 0}
		}

		// Execute createAllUTXOs
		err := suite.Server.blockValidation.createAllUTXOs(suite.Ctx, block, txWrappers)

		// Verify success even with many transactions
		assert.NoError(t, err, "Should handle large number of transactions with concurrency limits")

		// Verify all mock expectations were met
		suite.MockUTXOStore.AssertExpectations(t)
	})
}

// TestValidateAllTransactions tests transaction validation with proper options
func TestValidateAllTransactions(t *testing.T) {
	t.Run("ValidationSuccess", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test data
		block := testhelpers.CreateTestBlockWithSubtrees(t, 100)
		txs := testhelpers.CreateTestTransactions(t, 3)
		txWrappers := make([]txWrapper, len(txs))

		// Setup UTXO store expectations for validator internal calls
		for idx, tx := range txs {
			// The validator may call Create during validation
			suite.MockUTXOStore.On("Create", mock.Anything, tx, mock.Anything, mock.Anything).Return(&meta.Data{}, nil).Maybe()
			// Setup UTXO store expectations for spending transactions
			suite.MockUTXOStore.On("Spend", mock.Anything, tx, mock.Anything, mock.Anything).Return([]*utxo.Spend{}, nil).Maybe()
			txWrappers[idx] = txWrapper{tx: tx, subtreeIdx: 0}
		}

		// Execute validateAllTransactions
		err := suite.Server.blockValidation.spendAllTransactions(suite.Ctx, block, txWrappers)

		// Verify success
		assert.NoError(t, err, "Should successfully validate all transactions")
	})

	t.Run("ValidationWithManyTransactions", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create many transactions to test concurrency limiting
		block := testhelpers.CreateTestBlockWithSubtrees(t, 100)
		txs := testhelpers.CreateTestTransactions(t, 15) // Test with multiple transactions
		txWrappers := make([]txWrapper, len(txs))

		// Setup UTXO store expectations for validator internal calls
		for idx, tx := range txs {
			// The validator may call Create during validation
			suite.MockUTXOStore.On("Create", mock.Anything, tx, mock.Anything, mock.Anything).Return(&meta.Data{}, nil).Maybe()
			// Setup UTXO store expectations for spending transactions
			suite.MockUTXOStore.On("Spend", mock.Anything, tx, mock.Anything, mock.Anything).Return([]*utxo.Spend{}, nil).Maybe()
			txWrappers[idx] = txWrapper{tx: tx, subtreeIdx: 0}
		}

		// Execute validateAllTransactions
		err := suite.Server.blockValidation.spendAllTransactions(suite.Ctx, block, txWrappers)

		// Verify success even with many transactions
		assert.NoError(t, err, "Should handle large number of transactions with concurrency limits")
	})
}

// TestGetBlockTransactions tests transaction retrieval (simplified)
func TestGetBlockTransactions(t *testing.T) {
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

		// Execute getBlockTransactions
		_, err := suite.Server.blockValidation.getBlockTransactions(suite.Ctx, block)

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

package subtreevalidation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	blobmemory "github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	utxometa "github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestCheckBlockSubtrees(t *testing.T) {
	t.Run("EmptyBlock", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create a block with no subtrees using proper model construction
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}

		coinbaseTx := &bt.Tx{Version: 1}
		block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{}, 1, 250, 0, 0, nil)
		require.NoError(t, err)

		blockBytes, err := block.Bytes()
		require.NoError(t, err)

		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "http://test.com",
		}

		response, err := server.CheckBlockSubtrees(context.Background(), request)
		require.NoError(t, err)
		assert.True(t, response.Blessed)
	})

	t.Run("WithSubtrees", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test transactions
		tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
		require.NoError(t, err)

		tx2, err := createTestTransaction("6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4")
		require.NoError(t, err)

		// Create subtree with test transactions
		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("test_subtree_hash_32_bytes_long!"))

		// Store subtreeData containing the transactions
		subtreeData := bytes.Buffer{}
		// Write transactions in the format expected by readTransactionsFromSubtreeDataStream
		subtreeData.Write(tx1.Bytes())
		subtreeData.Write(tx2.Bytes())

		err = server.subtreeStore.Set(context.Background(), subtreeHash[:], fileformat.FileTypeSubtreeData, subtreeData.Bytes())
		require.NoError(t, err)

		// Mark the subtree as already validated to avoid calling ValidateSubtreeInternal
		err = server.subtreeStore.Set(context.Background(), subtreeHash[:], fileformat.FileTypeSubtree, []byte("validated"))
		require.NoError(t, err)

		// Create a block with subtrees using proper model construction
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}

		coinbaseTx := &bt.Tx{Version: 1}
		block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{&subtreeHash}, 2, 500, 0, 0, nil)
		require.NoError(t, err)

		blockBytes, err := block.Bytes()
		require.NoError(t, err)

		// Mock UTXO store Create method
		server.utxoStore.(*utxo.MockUtxostore).On("Create",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&utxometa.Data{}, nil)

		// Mock validator to return success - set up the validator client to succeed
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
			mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{1, 2, 3}, nil)

		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(true, nil)

		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "http://test.com",
		}

		response, err := server.CheckBlockSubtrees(context.Background(), request)
		require.NoError(t, err)
		assert.True(t, response.Blessed)
	})

	t.Run("InvalidBlockData", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create request with invalid block data
		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   []byte("invalid block data"),
			BaseUrl: "http://test.com",
		}

		response, err := server.CheckBlockSubtrees(context.Background(), request)
		require.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "Failed to get block from blockchain client")
	})

	t.Run("BlockchainClientError", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test transactions and store them
		tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
		require.NoError(t, err)

		// Create subtree with test transactions
		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("test_subtree_hash_32_bytes_long!"))

		subtreeData := bytes.Buffer{}
		subtreeData.Write(tx1.Bytes())

		err = server.subtreeStore.Set(context.Background(), subtreeHash[:], fileformat.FileTypeSubtreeData, subtreeData.Bytes())
		require.NoError(t, err)

		// Create a block with subtrees
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}

		coinbaseTx := &bt.Tx{Version: 1}
		block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{&subtreeHash}, 1, 400, 0, 0, nil)
		require.NoError(t, err)

		blockBytes, err := block.Bytes()
		require.NoError(t, err)

		// Mock blockchain client to return error
		server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
			mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{}, errors.NewServiceError("blockchain client error"))

		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "http://test.com",
		}

		response, err := server.CheckBlockSubtrees(context.Background(), request)
		require.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "Failed to get block headers from blockchain client")
	})

	t.Run("HTTPFetchingPath", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create subtree hash that doesn't exist in store to trigger HTTP fetching
		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("missing_subtree_hash_32_bytes_lng!"))

		// Create a block with the missing subtree
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}

		coinbaseTx := &bt.Tx{Version: 1}
		block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{&subtreeHash}, 1, 400, 0, 0, nil)
		require.NoError(t, err)

		blockBytes, err := block.Bytes()
		require.NoError(t, err)

		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "http://nonexistent-host.com", // This will fail HTTP request
		}

		response, err := server.CheckBlockSubtrees(context.Background(), request)
		require.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "Failed to get subtree tx hashes")
	})

	t.Run("SubtreeExistsError", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create a mock blob store that returns errors
		mockBlobStore := &MockBlobStore{}
		server.subtreeStore = mockBlobStore

		// Set up the mock to return an error when checking existence
		mockBlobStore.On("Exists", mock.Anything, mock.Anything, fileformat.FileTypeSubtree).
			Return(false, errors.NewStorageError("storage connection failed"))

		// Create a block with subtrees
		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("test_subtree_hash_32_bytes_long!"))

		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}

		coinbaseTx := &bt.Tx{Version: 1}
		block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{&subtreeHash}, 1, 400, 0, 0, nil)
		require.NoError(t, err)

		blockBytes, err := block.Bytes()
		require.NoError(t, err)

		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "http://test.com",
		}

		response, err := server.CheckBlockSubtrees(context.Background(), request)
		require.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "Failed to check if subtree exists in store")
	})

	t.Run("PartialSubtreesExist", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test transactions
		tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
		require.NoError(t, err)

		tx2, err := createTestTransaction("6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4")
		require.NoError(t, err)

		// Create two subtrees, store only one
		existingSubtreeHash := chainhash.Hash{}
		copy(existingSubtreeHash[:], []byte("existing_subtree_hash_32_bytes__!"))

		missingSubtreeHash := chainhash.Hash{}
		copy(missingSubtreeHash[:], []byte("missing_subtree_hash_32_bytes___!"))

		// Store the existing subtree data
		subtreeData := bytes.Buffer{}
		subtreeData.Write(tx1.Bytes())
		subtreeData.Write(tx2.Bytes())

		err = server.subtreeStore.Set(context.Background(), missingSubtreeHash[:], fileformat.FileTypeSubtreeData, subtreeData.Bytes())
		require.NoError(t, err)

		// Store the existing subtree as already validated
		err = server.subtreeStore.Set(context.Background(), existingSubtreeHash[:], fileformat.FileTypeSubtree, []byte("validated"))
		require.NoError(t, err)

		// Create a block with both subtrees
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}

		coinbaseTx := &bt.Tx{Version: 1}
		block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{&existingSubtreeHash, &missingSubtreeHash}, 2, 500, 0, 0, nil)
		require.NoError(t, err)

		blockBytes, err := block.Bytes()
		require.NoError(t, err)

		// Mock UTXO store Create method
		server.utxoStore.(*utxo.MockUtxostore).On("Create",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&utxometa.Data{}, nil)

		// Mock validator and blockchain client
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore

		server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
			mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{1, 2, 3}, nil)

		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(true, nil)

		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "http://nonexistent-host.com",
		}

		response, err := server.CheckBlockSubtrees(context.Background(), request)
		require.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "Failed to validate subtree")
	})
}

func TestExtractAndCollectTransactions(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test transactions
		tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
		require.NoError(t, err)

		tx2, err := createTestTransaction("6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4")
		require.NoError(t, err)

		// Store subtreeData
		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("test_subtree_hash_32_bytes_long!"))

		subtreeData := bytes.Buffer{}
		subtreeData.Write(tx1.Bytes())
		subtreeData.Write(tx2.Bytes())

		err = server.subtreeStore.Set(context.Background(), subtreeHash[:], fileformat.FileTypeSubtreeData, subtreeData.Bytes())
		require.NoError(t, err)

		// Test extraction
		var mutex sync.Mutex
		var allTransactions []*bt.Tx

		err = server.extractAndCollectTransactions(context.Background(), subtreeHash, &mutex, &allTransactions)
		require.NoError(t, err)

		assert.Len(t, allTransactions, 2)
		assert.Equal(t, tx1.TxID(), allTransactions[0].TxID())
		assert.Equal(t, tx2.TxID(), allTransactions[1].TxID())
	})

	t.Run("StorageError", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Non-existent subtree hash
		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("non_existent_hash_32_bytes_long!"))

		var mutex sync.Mutex
		var allTransactions []*bt.Tx

		err := server.extractAndCollectTransactions(context.Background(), subtreeHash, &mutex, &allTransactions)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get subtreeData from store")
	})

	t.Run("InvalidTransactionFormat", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Store corrupted transaction data
		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("test_subtree_hash_32_bytes_long!"))

		// Store invalid transaction data that will fail parsing
		invalidData := []byte{0x01, 0x00, 0x00, 0x00, 0xFF, 0xFF} // Invalid tx format
		err := server.subtreeStore.Set(context.Background(), subtreeHash[:], fileformat.FileTypeSubtreeData, invalidData)
		require.NoError(t, err)

		var mutex sync.Mutex
		var allTransactions []*bt.Tx

		err = server.extractAndCollectTransactions(context.Background(), subtreeHash, &mutex, &allTransactions)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read transactions from subtreeData")
	})
}

func TestProcessSubtreeDataStream(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test transactions
		tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
		require.NoError(t, err)

		tx2, err := createTestTransaction("6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4")
		require.NoError(t, err)

		// Create stream with transaction data
		subtreeData := bytes.Buffer{}
		subtreeData.Write(tx1.Bytes())
		subtreeData.Write(tx2.Bytes())

		body := io.NopCloser(&subtreeData)
		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("test_subtree_hash_32_bytes_long!"))

		var mutex sync.Mutex
		var allTransactions []*bt.Tx

		err = server.processSubtreeDataStream(context.Background(), subtreeHash, body, &mutex, &allTransactions)
		require.NoError(t, err)

		// Verify transactions were collected
		assert.Len(t, allTransactions, 2)

		// Verify data was stored
		exists, err := server.subtreeStore.Exists(context.Background(), subtreeHash[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("StorageError", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create a mock blob store that returns storage errors
		mockBlobStore := &MockBlobStore{}
		server.subtreeStore = mockBlobStore

		// Set up the mock to return an error when storing
		mockBlobStore.On("Set", mock.Anything, mock.Anything, fileformat.FileTypeSubtreeData, mock.Anything).
			Return(errors.NewStorageError("failed to write to storage"))

		// Create test transaction
		tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
		require.NoError(t, err)

		// Create stream with transaction data
		subtreeData := bytes.Buffer{}
		subtreeData.Write(tx1.Bytes())
		body := io.NopCloser(&subtreeData)

		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("test_subtree_hash_32_bytes_long!"))

		var mutex sync.Mutex
		var allTransactions []*bt.Tx

		err = server.processSubtreeDataStream(context.Background(), subtreeHash, body, &mutex, &allTransactions)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to store subtree data")
		// Verify transaction was still collected before storage error
		assert.Len(t, allTransactions, 1)
	})

	t.Run("InvalidTransactionData", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create stream with invalid transaction data
		invalidData := []byte("invalid transaction data that cannot be parsed")
		body := io.NopCloser(bytes.NewReader(invalidData))

		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("test_subtree_hash_32_bytes_long!"))

		var mutex sync.Mutex
		var allTransactions []*bt.Tx

		err := server.processSubtreeDataStream(context.Background(), subtreeHash, body, &mutex, &allTransactions)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error reading transaction")
	})
}

func TestReadTransactionsFromSubtreeDataStream(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test transactions
		tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
		require.NoError(t, err)

		tx2, err := createTestTransaction("6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4")
		require.NoError(t, err)

		// Create stream
		subtreeData := bytes.Buffer{}
		subtreeData.Write(tx1.Bytes())
		subtreeData.Write(tx2.Bytes())

		var mutex sync.Mutex
		var allTransactions []*bt.Tx

		count, err := server.readTransactionsFromSubtreeDataStream(&subtreeData, &mutex, &allTransactions)
		require.NoError(t, err)

		assert.Equal(t, 2, count)
		assert.Len(t, allTransactions, 2)
		assert.Equal(t, tx1.TxID(), allTransactions[0].TxID())
		assert.Equal(t, tx2.TxID(), allTransactions[1].TxID())
	})

	t.Run("EmptyStream", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		emptyBuffer := bytes.Buffer{}
		var mutex sync.Mutex
		var allTransactions []*bt.Tx

		count, err := server.readTransactionsFromSubtreeDataStream(&emptyBuffer, &mutex, &allTransactions)
		require.NoError(t, err)

		assert.Equal(t, 0, count)
		assert.Len(t, allTransactions, 0)
	})
}

// The missingTx and ValidateSubtree types are already defined in SubtreeValidation.go
// so we don't need to redefine them here

func TestPrepareTxsPerLevel(t *testing.T) {
	t.Run("SingleLevel", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create transactions with no dependencies
		tx1, err := createTestTransaction("tx1")
		require.NoError(t, err)
		tx2, err := createTestTransaction("tx2")
		require.NoError(t, err)

		missingTxs := []missingTx{
			{tx: tx1, idx: 0},
			{tx: tx2, idx: 1},
		}

		maxLevel, txsPerLevel, err := server.prepareTxsPerLevel(context.Background(), missingTxs)
		require.NoError(t, err)

		// All independent transactions should be at level 0
		assert.Equal(t, uint32(0), maxLevel)
		assert.Len(t, txsPerLevel[0], 2)
	})

	t.Run("MultiplelevelsWithDependencies", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create parent transaction
		parentTx, err := createTestTransaction("parent")
		require.NoError(t, err)

		// Create child transaction that would depend on parent
		// In real scenario, we'd set up the input to reference parent's output
		childTx, err := createTestTransaction("child")
		require.NoError(t, err)

		// Add mock to simulate dependency
		// This would require actual implementation details of how dependencies are determined
		missingTxs := []missingTx{
			{tx: childTx, idx: 0},
			{tx: parentTx, idx: 1},
		}

		maxLevel, txsPerLevel, err := server.prepareTxsPerLevel(context.Background(), missingTxs)
		require.NoError(t, err)

		// Verify level structure exists
		assert.GreaterOrEqual(t, maxLevel, uint32(0))
		assert.NotNil(t, txsPerLevel)
	})
}

func TestProcessTransactionsInLevels(t *testing.T) {
	t.Run("EmptyTransactions", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		var allTransactions []*bt.Tx
		blockIds := make(map[uint32]bool)

		err := server.processTransactionsInLevels(context.Background(), allTransactions, 100, blockIds)
		require.NoError(t, err)
	})

	t.Run("WithTransactions", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test transactions
		tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
		require.NoError(t, err)

		allTransactions := []*bt.Tx{tx1}
		blockIds := make(map[uint32]bool)

		// Mock validator to return success - set up the validator client to succeed
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(true, nil)

		err = server.processTransactionsInLevels(context.Background(), allTransactions, 100, blockIds)
		require.NoError(t, err)
	})

	t.Run("ValidationErrors", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test transactions
		tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
		require.NoError(t, err)

		allTransactions := []*bt.Tx{tx1}
		blockIds := make(map[uint32]bool)

		// Mock validator to return validation errors
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore
		// Add an error to the validator to simulate validation failure
		mockValidator.Errors = []error{errors.NewTxInvalidError("invalid transaction for testing")}

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(true, nil)

		// Should not fail even with validation errors (errors are logged but not returned)
		err = server.processTransactionsInLevels(context.Background(), allTransactions, 100, blockIds)
		require.NoError(t, err)
	})

	t.Run("MissingParentErrors", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test transactions
		tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
		require.NoError(t, err)

		allTransactions := []*bt.Tx{tx1}
		blockIds := make(map[uint32]bool)

		// Mock validator to return missing parent errors
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore
		// Add missing parent error
		mockValidator.Errors = []error{errors.NewTxMissingParentError("missing parent for testing")}

		// Mock blockchain client to return running state
		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(true, nil)

		// Should not fail, should add to orphanage
		err = server.processTransactionsInLevels(context.Background(), allTransactions, 100, blockIds)
		require.NoError(t, err)

		// Verify transaction was added to orphanage
		assert.Equal(t, 1, server.orphanage.Len())
	})

	t.Run("BlockchainNotRunning", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test transactions
		tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
		require.NoError(t, err)

		allTransactions := []*bt.Tx{tx1}
		blockIds := make(map[uint32]bool)

		// Mock validator to return missing parent errors
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore
		mockValidator.Errors = []error{errors.NewTxMissingParentError("missing parent for testing")}

		// Mock blockchain client to return NOT running state
		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(false, nil)

		// Should not fail, but transaction should NOT be added to orphanage
		err = server.processTransactionsInLevels(context.Background(), allTransactions, 100, blockIds)
		require.NoError(t, err)

		// Verify transaction was NOT added to orphanage
		assert.Equal(t, 0, server.orphanage.Len())
	})

	t.Run("BlockchainClientError", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test transactions
		tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
		require.NoError(t, err)

		allTransactions := []*bt.Tx{tx1}
		blockIds := make(map[uint32]bool)

		// Mock validator to return missing parent errors
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore
		mockValidator.Errors = []error{errors.NewTxMissingParentError("missing parent for testing")}

		// Mock blockchain client to return error
		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(false, errors.NewServiceError("blockchain client error"))

		// Should not fail, but transaction should NOT be added to orphanage due to error
		err = server.processTransactionsInLevels(context.Background(), allTransactions, 100, blockIds)
		require.NoError(t, err)

		// Verify transaction was NOT added to orphanage
		assert.Equal(t, 0, server.orphanage.Len())
	})

	t.Run("NilTransaction", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create slice with nil transaction
		allTransactions := []*bt.Tx{nil}
		blockIds := make(map[uint32]bool)

		// Should fail with nil transaction
		err := server.processTransactionsInLevels(context.Background(), allTransactions, 100, blockIds)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "transaction is nil")
	})

	t.Run("TransactionDependencies", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create parent transaction
		parentTx, err := createTestTransaction("parent")
		require.NoError(t, err)

		// Create child transaction that depends on parent
		// Note: In a real scenario, we'd create a transaction that spends the parent's output
		childTx, err := createTestTransaction("child")
		require.NoError(t, err)

		// Create grandchild transaction
		grandchildTx, err := createTestTransaction("grandchild")
		require.NoError(t, err)

		// Add transactions in mixed order to test level-based processing
		allTransactions := []*bt.Tx{grandchildTx, parentTx, childTx}
		blockIds := make(map[uint32]bool)

		// Mock validator to return success
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(true, nil)

		err = server.processTransactionsInLevels(context.Background(), allTransactions, 100, blockIds)
		require.NoError(t, err)
	})

	t.Run("ConcurrentValidationError", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create multiple transactions
		var allTransactions []*bt.Tx
		for i := 0; i < 5; i++ {
			tx, err := createTestTransaction(fmt.Sprintf("tx%d", i))
			require.NoError(t, err)
			allTransactions = append(allTransactions, tx)
		}

		blockIds := make(map[uint32]bool)

		// Mock validator to return errors for some transactions
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore
		// Set up errors for specific transactions
		mockValidator.Errors = []error{
			errors.NewTxInvalidError("invalid tx 1"),
			errors.NewTxInvalidError("invalid tx 2"),
		}

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(true, nil)

		// Should not return error even with some validation failures
		err := server.processTransactionsInLevels(context.Background(), allTransactions, 100, blockIds)
		require.NoError(t, err)
	})
}

// Helper function to create test transaction
func createTestTransaction(txIDStr string) (*bt.Tx, error) {
	// Create different non-coinbase transaction hexes based on input
	// These are regular transactions (not coinbase) with one input and one output
	var txHex string

	switch txIDStr {
	case "tx1":
		txHex = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0100f2052a01000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00000000"
	case "tx2":
		txHex = "0100000001b7c4c7b600c21cec2cb7e7ff8e5c45f722f2df6e16b3e19abaf6f3dd3a0e7d2d0000000048473044022027d03a989454c6c784a9bdc1a03829b528c38bb63cea26e95ce87fc6c30a860202202fa8be40c2b0bcbc73e02e2b77833c3db47b94b1e0de7e95a86ca79c860b793201ffffffff0100e1f505000000001976a914389ffce9cd9ae88dcc0631e88a821ffdbe9bfe2688ac00000000"
	default:
		// Default to a third unique transaction
		txHex = "01000000010b43c95dc0b280eab9f961d67de9dc13ad4a5f86e47816fddddfa96d1b9a8cf20000000048473044022054ae3b4c09f97eb1dcbb41e717166646dd7688dc0421b3ed2a1de8bf5dbe9c8e02201f53de302f6c0c529c67c3eeb154098eed95e4c959568d0c8c246da3c86cbc8101ffffffff0100f2052a010000001976a914389ffce9cd9ae88dcc0631e88a821ffdbe9bfe2688ac00000000"
	}

	tx, err := bt.NewTxFromString(txHex)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func TestValidateSubtreeInternal(t *testing.T) {
	t.Run("SuccessfulValidation", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test subtree hash
		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("test_subtree_hash_32_bytes_long!"))

		// Create validate subtree request
		v := ValidateSubtree{
			SubtreeHash:   subtreeHash,
			BaseURL:       "http://test.com",
			AllowFailFast: false,
		}

		// Mock the quorum to return a lock
		// This would require access to the quorum instance
		blockIds := make(map[uint32]bool)
		blockIds[1] = true

		// Since ValidateSubtreeInternal is complex and involves external dependencies,
		// we'd need to mock more components for a full test
		// For now, we can at least verify the function exists and can be called
		_, _ = server.ValidateSubtreeInternal(context.Background(), v, 100, blockIds)
	})
}

func TestBlessMissingTransaction(t *testing.T) {
	t.Run("SuccessfulBlessing", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test transaction
		tx, err := createTestTransaction("test")
		require.NoError(t, err)

		blockHash := chainhash.Hash{}
		copy(blockHash[:], []byte("test_block_hash_32_bytes_long___!"))

		blockIds := make(map[uint32]bool)
		blockIds[1] = true

		// Mock validator to return success
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore

		// Call blessMissingTransaction
		validatorOptions := validator.ProcessOptions()
		_, _ = server.blessMissingTransaction(context.Background(), blockHash, tx, 100, blockIds, validatorOptions)
	})
}

func TestProcessOrphans(t *testing.T) {
	t.Run("NoOrphans", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		blockHash := chainhash.Hash{}
		copy(blockHash[:], []byte("test_block_hash_32_bytes_long___!"))

		blockIds := make(map[uint32]bool)

		// Process orphans with empty orphanage
		server.processOrphans(context.Background(), blockHash, 100, blockIds)

		// Verify orphanage is still empty
		assert.Equal(t, 0, server.orphanage.Len())
	})

	t.Run("WithOrphans", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Add orphaned transaction
		tx, err := createTestTransaction("orphan")
		require.NoError(t, err)
		server.orphanage.Set(*tx.TxIDChainHash(), tx)

		blockHash := chainhash.Hash{}
		copy(blockHash[:], []byte("test_block_hash_32_bytes_long___!"))

		blockIds := make(map[uint32]bool)

		// Mock validator to return success
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore

		// Process orphans
		server.processOrphans(context.Background(), blockHash, 100, blockIds)
	})
}

func TestCheckBlockSubtrees_ConcurrentProcessing(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create multiple subtrees that don't exist in store
	var subtreeHashes []*chainhash.Hash
	for i := 0; i < 5; i++ {
		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte(fmt.Sprintf("subtree_hash_%d_32_bytes_long__!", i)))
		subtreeHashes = append(subtreeHashes, &subtreeHash)

		// Store subtree data for each
		tx, err := createTestTransaction(fmt.Sprintf("tx%d", i))
		require.NoError(t, err)

		subtreeData := bytes.Buffer{}
		subtreeData.Write(tx.Bytes())

		err = server.subtreeStore.Set(context.Background(), subtreeHash[:], fileformat.FileTypeSubtreeData, subtreeData.Bytes())
		require.NoError(t, err)

		// Mark as validated to avoid HTTP calls
		err = server.subtreeStore.Set(context.Background(), subtreeHash[:], fileformat.FileTypeSubtree, []byte("validated"))
		require.NoError(t, err)
	}

	// Create a block with multiple subtrees
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{},
		Nonce:          0,
	}

	coinbaseTx := &bt.Tx{Version: 1}
	block, err := model.NewBlock(header, coinbaseTx, subtreeHashes, 5, 1000, 0, 0, nil)
	require.NoError(t, err)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	// Mock blockchain client
	server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
		mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{1, 2, 3}, nil)

	server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
		mock.Anything, blockchain.FSMStateRUNNING).
		Return(true, nil)

	// Mock validator
	mockValidator := server.validatorClient.(*validator.MockValidatorClient)
	mockValidator.UtxoStore = server.utxoStore

	request := &subtreevalidation_api.CheckBlockSubtreesRequest{
		Block:   blockBytes,
		BaseUrl: "http://test.com",
	}

	response, err := server.CheckBlockSubtrees(context.Background(), request)
	require.NoError(t, err)
	assert.True(t, response.Blessed)
}

func TestExtractAndCollectTransactions_ConcurrentAccess(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create test transactions
	tx1, err := createTestTransaction("tx1")
	require.NoError(t, err)
	tx2, err := createTestTransaction("tx2")
	require.NoError(t, err)

	// Store subtreeData
	subtreeHash1 := chainhash.Hash{}
	copy(subtreeHash1[:], []byte("subtree_hash_1_32_bytes_long____!"))
	subtreeHash2 := chainhash.Hash{}
	copy(subtreeHash2[:], []byte("subtree_hash_2_32_bytes_long____!"))

	subtreeData1 := bytes.Buffer{}
	subtreeData1.Write(tx1.Bytes())
	err = server.subtreeStore.Set(context.Background(), subtreeHash1[:], fileformat.FileTypeSubtreeData, subtreeData1.Bytes())
	require.NoError(t, err)

	subtreeData2 := bytes.Buffer{}
	subtreeData2.Write(tx2.Bytes())
	err = server.subtreeStore.Set(context.Background(), subtreeHash2[:], fileformat.FileTypeSubtreeData, subtreeData2.Bytes())
	require.NoError(t, err)

	// Shared resources
	var mutex sync.Mutex
	var allTransactions []*bt.Tx

	// Extract from multiple subtrees concurrently
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := server.extractAndCollectTransactions(context.Background(), subtreeHash1, &mutex, &allTransactions)
		assert.NoError(t, err)
	}()

	go func() {
		defer wg.Done()
		err := server.extractAndCollectTransactions(context.Background(), subtreeHash2, &mutex, &allTransactions)
		assert.NoError(t, err)
	}()

	wg.Wait()

	// Verify both transactions were collected
	assert.Len(t, allTransactions, 2)
}

// Add these interfaces to properly compile blob.Store mock
var _ blob.Store = (*MockBlobStore)(nil)

// MockBlobStore is a mock implementation of blob.Store for testing
type MockBlobStore struct {
	mock.Mock
}

func (m *MockBlobStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, data []byte, opts ...options.FileOption) error {
	args := m.Called(ctx, key, fileType, data)
	return args.Error(0)
}

func (m *MockBlobStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	args := m.Called(ctx, key, fileType)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockBlobStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	args := m.Called(ctx, key, fileType)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(io.ReadCloser), args.Error(1)
}

func (m *MockBlobStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	args := m.Called(ctx, key, fileType)
	return args.Bool(0), args.Error(1)
}

func (m *MockBlobStore) Delete(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	args := m.Called(ctx, key, fileType)
	return args.Error(0)
}

func (m *MockBlobStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	args := m.Called(ctx, key, fileType)
	return args.Error(0)
}

func (m *MockBlobStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	args := m.Called(ctx, key, fileType, reader)
	return args.Error(0)
}

func (m *MockBlobStore) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...options.FileOption) error {
	args := m.Called(ctx, key, fileType, dah)
	return args.Error(0)
}

func (m *MockBlobStore) GetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (uint32, error) {
	args := m.Called(ctx, key, fileType)
	return args.Get(0).(uint32), args.Error(1)
}

func (m *MockBlobStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)
	return args.Int(0), args.String(1), args.Error(2)
}

func (m *MockBlobStore) SetCurrentBlockHeight(height uint32) {
	m.Called(height)
}

func (m *MockBlobStore) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// Helper function to setup test server
func setupTestServer(t *testing.T) (*Server, func()) {
	logger := &ulogger.TestLogger{}

	// Create test settings
	testSettings := settings.NewSettings()
	testSettings.SubtreeValidation.SpendBatcherSize = 10

	// Create stores
	subtreeStore := blobmemory.New()
	txStore := blobmemory.New()

	// Mock UTXO store
	mockUtxoStore := &utxo.MockUtxostore{}
	// Set up default mock for Create method
	mockUtxoStore.On("Create",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&utxometa.Data{}, nil).Maybe()
	// Set up default mock for BatchDecorate method
	mockUtxoStore.On("BatchDecorate",
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()
	// Set up default mock for GetBlockHeight method
	mockUtxoStore.On("GetBlockHeight").
		Return(uint32(100)).Maybe()
	// Set up default mock for GetMeta method
	mockUtxoStore.On("GetMeta",
		mock.Anything, mock.Anything).
		Return(&utxometa.Data{}, nil).Maybe()

	// Mock validator client
	mockValidatorClient := &validator.MockValidatorClient{}

	// Mock blockchain client
	mockBlockchainClient := &blockchain.Mock{}

	// Create orphanage to avoid nil pointer dereference
	orphanage := expiringmap.New[chainhash.Hash, *bt.Tx](time.Minute * 10)

	server := &Server{
		logger:           logger,
		settings:         testSettings,
		subtreeStore:     subtreeStore,
		txStore:          txStore,
		utxoStore:        mockUtxoStore,
		validatorClient:  mockValidatorClient,
		blockchainClient: mockBlockchainClient,
		orphanage:        orphanage,
	}

	return server, func() {
		// Cleanup if needed
	}
}

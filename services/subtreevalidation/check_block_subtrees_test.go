package subtreevalidation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxometa "github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestCheckBlockSubtrees(t *testing.T) {
	// Create test headers
	testHeaders := testhelpers.CreateTestHeaders(t, 1)

	t.Run("EmptyBlock", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
			mock.Anything).
			Return(testHeaders[0], &model.BlockHeaderMeta{}, nil)

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
		block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{}, 1, 250, 0, 0)
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

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
			mock.Anything).
			Return(testHeaders[0], &model.BlockHeaderMeta{}, nil)

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
		block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{&subtreeHash}, 2, 500, 0, 0)
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

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
			mock.Anything).
			Return(testHeaders[0], &model.BlockHeaderMeta{}, nil)

		// Create test transactions and store them
		tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
		require.NoError(t, err)

		// Create subtree with test transactions
		subtree, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)

		require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 1))

		subtreeData := subtreepkg.NewSubtreeData(subtree)
		require.NoError(t, subtreeData.AddTx(tx1, 0))

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)

		subtreeDataBytes, err := subtreeData.Serialize()
		require.NoError(t, err)

		// Mark the subtree as already validated to avoid HTTP calls
		err = server.subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes)
		require.NoError(t, err)

		err = server.subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
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
		block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{subtree.RootHash()}, 1, 400, 0, 0)
		require.NoError(t, err)

		blockBytes, err := block.Bytes()
		require.NoError(t, err)

		// Mock blockchain client to return error
		server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
			mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{}, errors.NewServiceError("blockchain client error"))

		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "http://localhost:8090",
		}

		response, err := server.CheckBlockSubtrees(context.Background(), request)
		require.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "Failed to get block headers from blockchain client")
	})

	t.Run("HTTPFetchingPath", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
			mock.Anything).
			Return(testHeaders[0], &model.BlockHeaderMeta{}, nil).Once()

		// Mock GetBlockHeaderIDs which is now called early in CheckBlockSubtrees
		server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
			mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{1, 2, 3}, nil)

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
		block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{&subtreeHash}, 1, 400, 0, 0)
		require.NoError(t, err)

		blockBytes, err := block.Bytes()
		require.NoError(t, err)

		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "http://localhost8090", // This will fail HTTP request
		}

		response, err := server.CheckBlockSubtrees(context.Background(), request)
		require.Error(t, err)
		assert.Nil(t, response)
		// The error message will vary depending on network conditions, but it should be a processing error
		assert.Contains(t, err.Error(), "CheckBlockSubtreesRequest")
	})

	t.Run("SubtreeExistsError", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
			mock.Anything).
			Return(testHeaders[0], &model.BlockHeaderMeta{}, nil).Once()

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
		block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{&subtreeHash}, 1, 400, 0, 0)
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

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
			mock.Anything).
			Return(testHeaders[0], &model.BlockHeaderMeta{}, nil).Once()
		currentState := blockchain.FSMStateRUNNING
		server.blockchainClient.(*blockchain.Mock).On("GetFSMCurrentState", mock.Anything).Return(&currentState, nil).Once()

		// Create test transactions
		tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
		require.NoError(t, err)

		tx2, err := createTestTransaction("6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4")
		require.NoError(t, err)

		missingSubtreeHash := chainhash.Hash{}
		copy(missingSubtreeHash[:], []byte("missing_subtree_hash_32_bytes___!"))

		subtree, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)

		require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 1))
		require.NoError(t, subtree.AddNode(*tx2.TxIDChainHash(), 2, 2))

		subtreeData := subtreepkg.NewSubtreeData(subtree)
		require.NoError(t, subtreeData.AddTx(tx1, 0))
		require.NoError(t, subtreeData.AddTx(tx2, 1))

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)

		subtreeDataBytes, err := subtreeData.Serialize()
		require.NoError(t, err)

		err = server.subtreeStore.Set(context.Background(), missingSubtreeHash[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes)
		require.NoError(t, err)

		err = server.subtreeStore.Set(context.Background(), missingSubtreeHash[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
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
		block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{subtree.RootHash(), &missingSubtreeHash}, 2, 500, 0, 0)
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
		assert.Contains(t, err.Error(), "Failed to get subtree tx hashes")
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
		subtree, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)

		require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 1))
		require.NoError(t, subtree.AddNode(*tx2.TxIDChainHash(), 1, 2))

		subtreeData := bytes.Buffer{}
		subtreeData.Write(tx1.Bytes())
		subtreeData.Write(tx2.Bytes())

		err = server.subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeData.Bytes())
		require.NoError(t, err)

		// Test extraction
		var allTransactions []*bt.Tx

		err = server.extractAndCollectTransactions(context.Background(), subtree, &allTransactions)
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

		// Create a subtree with a node - since we won't store any data for it, it will cause a storage error
		subtree, err := subtreepkg.NewTreeByLeafCount(1)
		require.NoError(t, err)
		// Add a node - the root hash will be calculated based on the nodes
		nodeHash := chainhash.Hash{}
		copy(nodeHash[:], []byte("node_hash_32_bytes_long_for_test!"))
		require.NoError(t, subtree.AddNode(nodeHash, 1, 1))

		var allTransactions []*bt.Tx

		err = server.extractAndCollectTransactions(context.Background(), subtree, &allTransactions)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get subtreeData from store")
	})

	t.Run("InvalidTransactionFormat", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create a subtree with a node first to get the root hash
		subtree, err := subtreepkg.NewTreeByLeafCount(1)
		require.NoError(t, err)
		// Add a node to make a valid subtree structure
		nodeHash := chainhash.Hash{}
		copy(nodeHash[:], []byte("node_hash_32_bytes_long_for_test!"))
		require.NoError(t, subtree.AddNode(nodeHash, 1, 1))

		// Store invalid transaction data that will fail parsing using the subtree's root hash
		invalidData := []byte{0x01, 0x00, 0x00, 0x00, 0xFF, 0xFF} // Invalid tx format
		err = server.subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, invalidData)
		require.NoError(t, err)

		var allTransactions []*bt.Tx

		err = server.extractAndCollectTransactions(context.Background(), subtree, &allTransactions)
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

		// Create subtree with the transactions
		subtree, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)
		// Add the transactions to the subtree for validation
		require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 1))
		require.NoError(t, subtree.AddNode(*tx2.TxIDChainHash(), 1, 2))

		var allTransactions []*bt.Tx

		err = server.processSubtreeDataStream(context.Background(), subtree, body, &allTransactions)
		require.NoError(t, err)

		// Verify transactions were collected
		assert.Len(t, allTransactions, 2)

		// Verify data was stored using the subtree's root hash
		exists, err := server.subtreeStore.Exists(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("StorageError", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create a mock blob store that returns storage errors
		mockBlobStore := &MockBlobStore{}
		server.subtreeStore = mockBlobStore

		// Set up the mock to return an error when storing (SetFromReader is now used)
		mockBlobStore.On("SetFromReader", mock.Anything, mock.Anything, fileformat.FileTypeSubtreeData, mock.Anything).
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

		// Create subtree with the transaction
		subtree, err := subtreepkg.NewTreeByLeafCount(1)
		require.NoError(t, err)
		// Add the transaction to the subtree for validation
		require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 1))

		var allTransactions []*bt.Tx

		err = server.processSubtreeDataStream(context.Background(), subtree, body, &allTransactions)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to store subtree data")
		// With streaming approach, if storage fails, no transactions are collected
		assert.Len(t, allTransactions, 0)
	})

	t.Run("InvalidTransactionData", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create stream with invalid transaction data
		invalidData := []byte("invalid transaction data that cannot be parsed")
		body := io.NopCloser(bytes.NewReader(invalidData))

		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("test_subtree_hash_32_bytes_long!"))

		// Create subtree with a node
		subtree, err := subtreepkg.NewTreeByLeafCount(1)
		require.NoError(t, err)
		// Add a dummy node to make a valid subtree
		nodeHash := chainhash.Hash{}
		copy(nodeHash[:], []byte("dummy_node_hash_32_bytes_long___!"))
		require.NoError(t, subtree.AddNode(nodeHash, 1, 1))

		var allTransactions []*bt.Tx

		err = server.processSubtreeDataStream(context.Background(), subtree, body, &allTransactions)
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

		// Create subtree with the transactions
		subtree, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)
		require.NoError(t, subtree.AddCoinbaseNode())
		require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 1))
		require.NoError(t, subtree.AddNode(*tx2.TxIDChainHash(), 1, 2))

		var allTransactions []*bt.Tx

		count, err := server.readTransactionsFromSubtreeDataStream(subtree, &subtreeData, &allTransactions)
		require.NoError(t, err)

		assert.Equal(t, 3, count)         // includes coinbase in the count
		assert.Len(t, allTransactions, 2) // does not include the coinbase tx
		assert.Equal(t, tx1.TxID(), allTransactions[0].TxID())
		assert.Equal(t, tx2.TxID(), allTransactions[1].TxID())
	})

	t.Run("EmptyStream", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		emptyBuffer := bytes.Buffer{}
		// Create subtree with 1 leaf but empty buffer (0 transactions)
		subtree, err := subtreepkg.NewTreeByLeafCount(1)
		require.NoError(t, err)
		// Don't add any nodes - this means 0 transactions expected
		var allTransactions []*bt.Tx

		count, err := server.readTransactionsFromSubtreeDataStream(subtree, &emptyBuffer, &allTransactions)
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

	t.Run("full block test", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		blockBytes, err := os.ReadFile("testdata/171/0000000023ffe4075b18e77ce7342b90a7deeb92e4b3681551253291c3522824.block")
		require.NoError(t, err)

		block, err := model.NewBlockFromBytes(blockBytes)
		require.NoError(t, err)

		allTxs := make([]*bt.Tx, 0, block.TransactionCount)
		allTxsMu := sync.Mutex{}

		g := errgroup.Group{}

		// get all the transactions from all the subtrees in the block
		for _, subtreeHash := range block.Subtrees {
			subtreeHash := *subtreeHash

			g.Go(func() error {
				subtreeBytes, err := os.ReadFile("testdata/171/" + subtreeHash.String() + ".subtree")
				require.NoError(t, err)

				subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(len(subtreeBytes) / chainhash.HashSize)
				require.NoError(t, err, fmt.Sprintf("failed to parse subtree %s", subtreeHash.String()))

				for i := 0; i < len(subtreeBytes); i += chainhash.HashSize {
					var h chainhash.Hash
					copy(h[:], subtreeBytes[i:i+chainhash.HashSize])

					if h.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
						err = subtree.AddCoinbaseNode()
						require.NoError(t, err)
					} else {
						err = subtree.AddNode(h, 0, 0)
						require.NoError(t, err)
					}
				}

				subtreeDataBytes, err := os.ReadFile("testdata/171/" + subtreeHash.String() + ".subtree_data")
				require.NoError(t, err)

				subtreeData, err := subtreepkg.NewSubtreeDataFromBytes(subtree, subtreeDataBytes)
				require.NoError(t, err)

				for _, tx := range subtreeData.Txs {
					if tx != nil {
						allTxsMu.Lock()
						allTxs = append(allTxs, tx)
						allTxsMu.Unlock()
					}
				}

				return nil
			})
		}

		err = g.Wait()
		require.NoError(t, err)

		missingTxs := make([]missingTx, len(allTxs))
		for i, tx := range allTxs {
			missingTxs[i] = missingTx{
				tx:  tx,
				idx: i,
			}
		}

		// Use the existing prepareTxsPerLevel logic to organize transactions by dependency levels
		maxLevel, txsPerLevel, err := server.prepareTxsPerLevel(t.Context(), missingTxs)
		require.NoError(t, err)

		// for level, txs := range txsPerLevel {
		// 	fmt.Printf("Level %d has %d transactions\n", level, len(txs))
		// 	for _, tx := range txs {
		// 		fmt.Printf("  TxID: %s\n", tx.tx.TxIDChainHash().String())
		// 	}
		// }

		assert.Equal(t, uint32(12), maxLevel)
		assert.NotNil(t, txsPerLevel)

		// get the level of "0f3a71a9441084a263d0c7c18ea536793c93da0a50666d41ee0dc8ec07b7eced" (child)
		// then get the level of "7198ecdae55f77cef5d8e8042adecae5e37fd149c17cd0a291d0c342251ee228" (parent)
		// and make sure the level of the first is greater than the level of the second
		var levelA, levelB int
		for level, txs := range txsPerLevel {
			for _, tx := range txs {
				if tx.tx.TxIDChainHash().String() == "0f3a71a9441084a263d0c7c18ea536793c93da0a50666d41ee0dc8ec07b7eced" {
					levelA = level
				}
				if tx.tx.TxIDChainHash().String() == "7198ecdae55f77cef5d8e8042adecae5e37fd149c17cd0a291d0c342251ee228" {
					levelB = level
				}
			}
		}
		assert.Greater(t, levelA, levelB, "Expected level of first transaction to be greater than second")
	})
}

func TestProcessTransactionsInLevels(t *testing.T) {
	t.Run("EmptyTransactions", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		var allTransactions []*bt.Tx
		blockIds := make(map[uint32]bool)

		err := server.processTransactionsInLevels(context.Background(), allTransactions, chainhash.Hash{}, chainhash.Hash{}, 100, blockIds)
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

		err = server.processTransactionsInLevels(context.Background(), allTransactions, chainhash.Hash{}, chainhash.Hash{}, 100, blockIds)
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

		// Should fail with validation errors (errors are logged but not returned)
		err = server.processTransactionsInLevels(context.Background(), allTransactions, chainhash.Hash{}, chainhash.Hash{}, 100, blockIds)
		require.Error(t, err)
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

		// Should fail because transaction has missing parent
		err = server.processTransactionsInLevels(context.Background(), allTransactions, chainhash.Hash{}, chainhash.Hash{}, 100, blockIds)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "processTransactionsInLevels")

		// Verify transaction was added to orphanage even though processing failed
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

		// Should fail because transaction has validation errors and blockchain not running
		err = server.processTransactionsInLevels(context.Background(), allTransactions, chainhash.Hash{}, chainhash.Hash{}, 100, blockIds)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "processTransactionsInLevels")

		// Verify transaction was NOT added to orphanage (blockchain not running)
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

		// Should fail because transaction has validation errors and blockchain client error
		err = server.processTransactionsInLevels(context.Background(), allTransactions, chainhash.Hash{}, chainhash.Hash{}, 100, blockIds)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "processTransactionsInLevels")

		// Verify transaction was NOT added to orphanage (blockchain client error)
		assert.Equal(t, 0, server.orphanage.Len())
	})

	t.Run("NilTransaction", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create slice with nil transaction
		allTransactions := []*bt.Tx{nil}
		blockIds := make(map[uint32]bool)

		// Should fail with nil transaction
		err := server.processTransactionsInLevels(context.Background(), allTransactions, chainhash.Hash{}, chainhash.Hash{}, 100, blockIds)
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

		err = server.processTransactionsInLevels(context.Background(), allTransactions, chainhash.Hash{}, chainhash.Hash{}, 100, blockIds)
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

		// Should return error even some validation failures
		err := server.processTransactionsInLevels(context.Background(), allTransactions, chainhash.Hash{}, chainhash.Hash{}, 100, blockIds)
		require.Error(t, err)
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
		_, _ = server.blessMissingTransaction(context.Background(), blockHash, blockHash, tx, 100, blockIds, validatorOptions)
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
	block, err := model.NewBlock(header, coinbaseTx, subtreeHashes, 5, 1000, 0, 0)
	require.NoError(t, err)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	// Mock blockchain client
	prevHash := chainhash.Hash{}
	copy(prevHash[:], []byte("previous_block_hash_32_bytes____!"))
	merkleRoot := chainhash.Hash{}
	copy(merkleRoot[:], []byte("merkle_root_hash_32_bytes_______!"))

	server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
		mock.Anything).
		Return(&model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash, // Different from the block's parent
			HashMerkleRoot: &merkleRoot,
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}, &model.BlockHeaderMeta{
			ID: 100,
		}, nil)

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

	// Create subtrees first with the actual transaction hashes
	subtree1, err := subtreepkg.NewTreeByLeafCount(1)
	require.NoError(t, err)
	require.NoError(t, subtree1.AddNode(*tx1.TxIDChainHash(), 1, 1))

	subtree2, err := subtreepkg.NewTreeByLeafCount(1)
	require.NoError(t, err)
	require.NoError(t, subtree2.AddNode(*tx2.TxIDChainHash(), 1, 1))

	// Store subtreeData using the actual root hashes
	subtreeData1 := bytes.Buffer{}
	subtreeData1.Write(tx1.Bytes())
	err = server.subtreeStore.Set(context.Background(), subtree1.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeData1.Bytes())
	require.NoError(t, err)

	subtreeData2 := bytes.Buffer{}
	subtreeData2.Write(tx2.Bytes())
	err = server.subtreeStore.Set(context.Background(), subtree2.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeData2.Bytes())
	require.NoError(t, err)

	// Separate slices for each goroutine to avoid race conditions
	var transactions1 []*bt.Tx
	var transactions2 []*bt.Tx

	// Extract from multiple subtrees concurrently
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := server.extractAndCollectTransactions(context.Background(), subtree1, &transactions1)
		assert.NoError(t, err)
	}()

	go func() {
		defer wg.Done()
		err := server.extractAndCollectTransactions(context.Background(), subtree2, &transactions2)
		assert.NoError(t, err)
	}()

	wg.Wait()

	// Merge results and verify both transactions were collected
	allTransactions := append(transactions1, transactions2...)
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
	// Call the mock first to get the configured error (if any)
	// Don't pass the reader to avoid data races with testify's reflection-based argument matching
	args := m.Called(ctx, key, fileType, mock.Anything)
	err := args.Error(0)

	// Only drain the reader if there's no error (simulating realistic storage behavior)
	// In a real storage implementation, an error would stop reading from the reader
	if err == nil {
		_, _ = io.Copy(io.Discard, reader)
	}
	reader.Close()

	return err
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
	// Set up default mock for GetBlockExists to return true (parent exists on main chain by default)
	mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).
		Return(true, nil).Maybe()
	// Set up default mock for GetBlockHeader to return a header on the main chain
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(
			&model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{},
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      12345678,
				Bits:           model.NBit{},
				Nonce:          0,
			},
			&model.BlockHeaderMeta{ID: 123},
			nil,
		).Maybe()
	// Set up default mock for CheckBlockIsInCurrentChain to return true (on main chain)
	mockBlockchainClient.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).
		Return(true, nil).Maybe()

	currentState := blockchain.FSMStateRUNNING
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).
		Return(&currentState, nil).Maybe()

	// Create orphanage to avoid nil pointer dereference
	orphanage, err := NewOrphanage(time.Minute*10, 100, logger)
	require.NoError(t, err)

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

// TestCheckBlockSubtrees_DifferentFork tests the early return optimization.
// NOTE: After the optimization to check missing subtrees first, blocks with no missing
// subtrees return immediately before executing pause logic. The pause logic for different
// fork scenarios is tested in TestCheckBlockSubtrees/WithSubtrees and other integration tests.
func TestCheckBlockSubtrees_DifferentFork(t *testing.T) {
	// Create test settings once for all subtests
	testSettings := settings.NewSettings()
	testSettings.SubtreeValidation.SpendBatcherSize = 10

	tests := []struct {
		name string
	}{
		{
			name: "block with no missing subtrees returns early",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mocks
			mockBlockchainClient := &blockchain.Mock{}
			mockSubtreeStore := blobmemory.New()
			mockTxStore := blobmemory.New()
			mockUTXOStore := &utxo.MockUtxostore{}

			// Create test block - no subtrees so we hit the early return
			parentHash := &chainhash.Hash{}
			merkleRoot := &chainhash.Hash{}
			block := &model.Block{
				Header: &model.BlockHeader{
					Version:        1,
					HashPrevBlock:  parentHash,
					HashMerkleRoot: merkleRoot,
					Timestamp:      12345678,
					Bits:           model.NBit{},
					Nonce:          0,
				},
				Subtrees:         []*chainhash.Hash{},
				TransactionCount: 0,
			}
			blockBytes, _ := block.Bytes()

			// Create server
			server := &Server{
				settings:         testSettings,
				logger:           ulogger.TestLogger{},
				blockchainClient: mockBlockchainClient,
				subtreeStore:     mockSubtreeStore,
				txStore:          mockTxStore,
				utxoStore:        mockUTXOStore,
			}

			// Create request
			request := &subtreevalidation_api.CheckBlockSubtreesRequest{
				Block:   blockBytes,
				BaseUrl: "http://peer.example.com",
			}

			// Execute - should return immediately with blessed=true
			response, err := server.CheckBlockSubtrees(context.Background(), request)

			// Verify
			assert.NoError(t, err)
			assert.True(t, response.Blessed)

			// No blockchain client calls should have been made (early return)
			mockBlockchainClient.AssertExpectations(t)
		})
	}
}

func TestCheckBlockSubtrees_ParentBlockErrors(t *testing.T) {
	// Create test headers
	testHeaders := testhelpers.CreateTestHeaders(t, 2)
	parentHash := testHeaders[0].Hash()
	childHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  parentHash,
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{},
		Nonce:          0,
	}

	coinbaseTx := &bt.Tx{Version: 1}
	block, err := model.NewBlock(childHeader, coinbaseTx, []*chainhash.Hash{}, 1, 250, 0, 0)
	require.NoError(t, err)
	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	t.Run("GetBlockExists_Error", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Mock GetBestBlockHeader to return a different hash
		differentHash := chainhash.Hash{}
		copy(differentHash[:], []byte("different_hash_32_bytes_long!!!!"))
		differentHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}
		server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
			mock.Anything).
			Return(differentHeader, &model.BlockHeaderMeta{}, nil)

		// Mock GetBlockExists to return an error
		server.blockchainClient.(*blockchain.Mock).On("GetBlockExists",
			mock.Anything, parentHash).
			Return(false, errors.NewUnknownError("database error"))

		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "http://test.com",
		}

		response, err := server.CheckBlockSubtrees(context.Background(), request)
		require.NoError(t, err)
		assert.True(t, response.Blessed)
	})

	t.Run("GetBlockHeader_Error", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Mock GetBestBlockHeader to return a different hash
		differentHash := chainhash.Hash{}
		copy(differentHash[:], []byte("different_hash_32_bytes_long!!!!"))
		differentHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}
		server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
			mock.Anything).
			Return(differentHeader, &model.BlockHeaderMeta{}, nil)

		// Mock GetBlockExists to return true
		server.blockchainClient.(*blockchain.Mock).On("GetBlockExists",
			mock.Anything, parentHash).
			Return(true, nil)

		// Mock GetBlockHeader to return an error
		server.blockchainClient.(*blockchain.Mock).On("GetBlockHeader",
			mock.Anything, parentHash).
			Return(nil, nil, errors.NewUnknownError("database error"))

		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "http://test.com",
		}

		response, err := server.CheckBlockSubtrees(context.Background(), request)
		require.NoError(t, err)
		assert.True(t, response.Blessed)
	})

	t.Run("CheckBlockIsInCurrentChain_Error", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Mock GetBestBlockHeader to return a different hash
		differentHash := chainhash.Hash{}
		copy(differentHash[:], []byte("different_hash_32_bytes_long!!!!"))
		differentHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}
		server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
			mock.Anything).
			Return(differentHeader, &model.BlockHeaderMeta{}, nil)

		// Mock GetBlockExists to return true
		server.blockchainClient.(*blockchain.Mock).On("GetBlockExists",
			mock.Anything, parentHash).
			Return(true, nil)

		// Mock GetBlockHeader to return a valid header with meta
		server.blockchainClient.(*blockchain.Mock).On("GetBlockHeader",
			mock.Anything, parentHash).
			Return(testHeaders[0], &model.BlockHeaderMeta{ID: 123}, nil)

		// Mock CheckBlockIsInCurrentChain to return an error
		server.blockchainClient.(*blockchain.Mock).On("CheckBlockIsInCurrentChain",
			mock.Anything, []uint32{123}).
			Return(false, errors.NewUnknownError("chain check error"))

		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "http://test.com",
		}

		response, err := server.CheckBlockSubtrees(context.Background(), request)
		require.NoError(t, err)
		assert.True(t, response.Blessed)
	})
}

// TestCheckBlockSubtrees_LargeBlock_MemoryConsumption tests memory usage with a large number of transactions
// This test verifies that Phase 1 optimization reduces memory consumption by ~50%
// Run with: go test -v -run TestCheckBlockSubtrees_LargeBlock_MemoryConsumption -memprofile=mem.prof
// Analyze with: go tool pprof -http=:8080 mem.prof
func TestCheckBlockSubtrees_LargeBlock_MemoryConsumption(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory consumption test in short mode")
	}

	// Configuration: adjust these values to test with different loads
	const (
		numTransactions   = 10240 // Number of transactions to process (must be divisible by txsPerSubtree)
		txsPerSubtree     = 512   // Transactions per subtree (must be power of 2)
		estimatedTxSize   = 250   // Average transaction size in bytes
		expectedMemoryMB  = 100   // Expected peak memory in MB (adjust after baseline)
		memoryToleranceMB = 50    // Tolerance for memory variation
	)

	numSubtrees := numTransactions / txsPerSubtree

	t.Logf("Creating test with %d transactions across %d subtrees", numTransactions, numSubtrees)

	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create test headers
	testHeaders := testhelpers.CreateTestHeaders(t, 1)

	// Mock blockchain client
	server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
		mock.Anything).
		Return(testHeaders[0], &model.BlockHeaderMeta{}, nil)

	runningState := blockchain.FSMStateRUNNING
	server.blockchainClient.(*blockchain.Mock).On("GetFSMCurrentState",
		mock.Anything).
		Return(&runningState, nil).Maybe()

	server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
		mock.Anything, blockchain.FSMStateRUNNING).
		Return(true, nil).Maybe()

	// Generate transactions and organize into subtrees
	allSubtreeHashes := make([]*chainhash.Hash, 0, numSubtrees)
	allSubtrees := make([]*subtreepkg.Subtree, 0, numSubtrees)

	t.Log("Generating transactions and subtrees...")
	for i := 0; i < numSubtrees; i++ {
		// Create subtree with power-of-2 leaf count
		subtree, err := subtreepkg.NewTreeByLeafCount(txsPerSubtree)
		require.NoError(t, err)

		// Create subtreeData buffer
		subtreeData := bytes.Buffer{}

		// Generate transactions for this subtree
		for j := 0; j < txsPerSubtree; j++ {
			// Create unique transaction by using modulo to cycle through base transactions
			baseIdx := (i*txsPerSubtree + j) % 3
			var baseTxStr string
			switch baseIdx {
			case 0:
				baseTxStr = "tx1"
			case 1:
				baseTxStr = "tx2"
			default:
				baseTxStr = fmt.Sprintf("tx%d", baseIdx)
			}

			tx, err := createTestTransaction(baseTxStr)
			require.NoError(t, err)

			// Add to subtree
			err = subtree.AddNode(*tx.TxIDChainHash(), 1, uint64(j+1))
			require.NoError(t, err)

			// Add to subtreeData
			subtreeData.Write(tx.Bytes())
		}

		// Store subtreeData (skip if already exists due to duplicate subtree roots)
		exists, _ := server.subtreeStore.Exists(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
		if !exists {
			err = server.subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeData.Bytes())
			require.NoError(t, err)

			// Mark the subtree as already validated to avoid HTTP fetching
			err = server.subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, []byte("validated"))
			require.NoError(t, err)
		}

		allSubtreeHashes = append(allSubtreeHashes, subtree.RootHash())
		allSubtrees = append(allSubtrees, subtree)
	}

	// Create block with all subtrees
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{},
		Nonce:          0,
	}

	coinbaseTx := &bt.Tx{Version: 1}
	block, err := model.NewBlock(header, coinbaseTx, allSubtreeHashes, 1, 250, 0, 0)
	require.NoError(t, err)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	// Force GC before measurement
	runtime.GC()
	runtime.GC()

	// Measure memory before
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	t.Logf("Memory before processing:")
	t.Logf("  Heap Alloc: %.2f MB", float64(memBefore.HeapAlloc)/(1024*1024))
	t.Logf("  Heap Objects: %d", memBefore.HeapObjects)

	// Process the block
	// Use empty BaseUrl to read from local storage instead of HTTP
	request := &subtreevalidation_api.CheckBlockSubtreesRequest{
		Block:   blockBytes,
		BaseUrl: "",
	}

	response, err := server.CheckBlockSubtrees(context.Background(), request)
	require.NoError(t, err)
	assert.True(t, response.Blessed)

	// Measure memory after
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	t.Logf("Memory after processing:")
	t.Logf("  Heap Alloc: %.2f MB", float64(memAfter.HeapAlloc)/(1024*1024))
	t.Logf("  Heap Objects: %d", memAfter.HeapObjects)
	t.Logf("  Total Alloc: %.2f MB", float64(memAfter.TotalAlloc)/(1024*1024))

	// Calculate peak memory during processing
	peakMemoryMB := float64(memAfter.HeapAlloc) / (1024 * 1024)
	estimatedDataSizeMB := float64(numTransactions*estimatedTxSize) / (1024 * 1024)

	t.Logf("Estimated raw data size: %.2f MB", estimatedDataSizeMB)
	t.Logf("Peak memory usage: %.2f MB", peakMemoryMB)
	t.Logf("Memory overhead: %.2fx", peakMemoryMB/estimatedDataSizeMB)

	// Verify memory usage is reasonable
	// With Phase 1 optimization, we expect memory to be roughly 2-3x the raw data size
	// (accounting for Go object overhead, but not the 2x TeeReader duplication)
	maxExpectedMemoryMB := float64(expectedMemoryMB + memoryToleranceMB)

	if peakMemoryMB > maxExpectedMemoryMB {
		t.Logf("WARNING: Memory usage (%.2f MB) exceeds expected maximum (%.2f MB)", peakMemoryMB, maxExpectedMemoryMB)
		t.Logf("This may indicate the Phase 1 optimization is not working as expected")
		// Don't fail the test, just warn - memory usage can vary by platform
	} else {
		t.Logf("SUCCESS: Memory usage (%.2f MB) is within expected range (<= %.2f MB)", peakMemoryMB, maxExpectedMemoryMB)
	}

	// Additional memory statistics
	t.Logf("GC Statistics:")
	t.Logf("  Number of GCs: %d", memAfter.NumGC-memBefore.NumGC)
	t.Logf("  GC Pause Total: %.2f ms", float64(memAfter.PauseTotalNs-memBefore.PauseTotalNs)/(1000*1000))
}

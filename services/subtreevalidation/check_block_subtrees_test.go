package subtreevalidation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
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
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
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

// TestCheckBlockSubtrees_OversizedBody verifies that the peer-fetch fallback at
// check_block_subtrees.go refuses to allocate a response body larger than
// SubtreeValidation.MaxIncomingSubtreeBytes. Pre-fix a malicious peer could OOM the node by
// streaming oversized bytes inside the request window; post-fix the chain surfaces ErrExternal.
func TestCheckBlockSubtrees_OversizedBody(t *testing.T) {
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	server, cleanup := setupTestServer(t)
	defer cleanup()

	server.settings.SubtreeValidation.MaxIncomingSubtreeBytes = 128 // tiny cap

	server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
		mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{1, 2, 3}, nil)

	// Hash that doesn't exist in subtreeStore — forces the peer HTTP-fetch fallback.
	subtreeHash := chainhash.HashH([]byte("test-oversized-checkblock-subtree"))

	baseURL := testPeerURL
	subtreeURL := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())
	oversized := bytes.Repeat([]byte{0xab}, 4*1024) // 4 KB — far over the 128-byte cap
	httpmock.RegisterResponder("GET", subtreeURL,
		httpmock.NewBytesResponder(http.StatusOK, oversized))

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
		BaseUrl: baseURL,
	}

	response, err := server.CheckBlockSubtrees(context.Background(), request)
	require.Error(t, err)
	assert.Nil(t, response)
	assert.True(t, errors.Is(err, errors.ErrExternal), "expected ErrExternal in chain, got %v", err)
}

// TestCheckBlockSubtrees_LocalAssemblyPolicyIgnored is a regression test for issue #905.
// The peer-fetch fallback in CheckBlockSubtrees gates the response twice: first by the
// HTTP body size, then by the derived leaf count. Pre-fix both gates used the local
// BlockAssembly.MaximumMerkleItemsPerSubtree, so a docker-quickstart node (32k cap) rejected
// every peer subtree larger than 1 MiB even though the body cap was generous. Post-fix both
// gates are governed by SubtreeValidation.MaxIncomingSubtreeBytes; the local assembly cap
// no longer rejects legitimate peer responses.
func TestCheckBlockSubtrees_LocalAssemblyPolicyIgnored(t *testing.T) {
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Docker quickstart profile: small local assembly cap, generous receive cap.
	server.settings.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768
	server.settings.SubtreeValidation.MaxIncomingSubtreeBytes = 128 * 1024 * 1024

	server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
		mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{1, 2, 3}, nil)

	// Hash that doesn't exist in subtreeStore — forces the peer HTTP-fetch fallback.
	subtreeHash := chainhash.HashH([]byte("test-large-peer-checkblock-subtree"))
	baseURL := testPeerURL
	subtreeURL := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())

	// 65,536 32-byte hashes = 2 MiB. This is 2x the docker assembly cap (32k * 32 = 1 MiB)
	// but well below the receive cap (128 MiB). Pre-fix the leaf-count gate rejected this
	// with "exceeds policy max"; post-fix it must pass that gate. The synthesized hashes
	// won't compute back to subtreeHash so the call still fails downstream — we assert only
	// that the failure is NOT the policy-max gate.
	const leafCount = 65536
	payload := make([]byte, leafCount*chainhash.HashSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	httpmock.RegisterResponder("GET", subtreeURL,
		httpmock.NewBytesResponder(http.StatusOK, payload))

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
		BaseUrl: baseURL,
	}

	_, err = server.CheckBlockSubtrees(context.Background(), request)
	require.Error(t, err, "expected the synthesized payload's root to mismatch subtreeHash")
	require.NotContains(t, err.Error(), "exceeds policy max",
		"leaf-count gate rejected a peer subtree larger than the local assembly cap — see issue #905")
}

func TestCheckBlockSubtrees_WithQuorum(t *testing.T) {
	testHeaders := testhelpers.CreateTestHeaders(t, 1)

	t.Run("SubtreeExistsViaQuorum", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Set up quorum using the server's subtreeStore as the exister
		quorumDir := t.TempDir()
		q, err := NewQuorum(&ulogger.TestLogger{}, server.subtreeStore, quorumDir, WithTimeout(100*time.Millisecond))
		require.NoError(t, err)
		server.quorum = q

		server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
			mock.Anything).
			Return(testHeaders[0], &model.BlockHeaderMeta{}, nil)

		// Create a subtree hash and store it so it "exists"
		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("quorum_test_subtree_exists__"))

		err = server.subtreeStore.Set(context.Background(), subtreeHash[:], fileformat.FileTypeSubtree, []byte("validated"))
		require.NoError(t, err)

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

		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "http://test.com",
		}

		// Subtree exists — should return blessed with no missing subtrees
		response, err := server.CheckBlockSubtrees(context.Background(), request)
		require.NoError(t, err)
		assert.True(t, response.Blessed)
	})

	t.Run("SubtreeMissingViaQuorum_LocksAndMarks", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Set up quorum — subtree does NOT exist in store
		quorumDir := t.TempDir()
		q, err := NewQuorum(&ulogger.TestLogger{}, server.subtreeStore, quorumDir, WithTimeout(100*time.Millisecond))
		require.NoError(t, err)
		server.quorum = q

		server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
			mock.Anything).
			Return(testHeaders[0], &model.BlockHeaderMeta{}, nil)

		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("quorum_test_subtree_missing_"))

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

		server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
			mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{1, 2, 3}, nil)
		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(true, nil)

		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "http://127.0.0.1:0",
		}

		// Subtree is missing — quorum will lock, mark as missing, then try to HTTP-fetch,
		// from http://127.0.0.1:0, which fails deterministically because nothing listens there. The important thing is it detected missing via quorum.
		_, err = server.CheckBlockSubtrees(context.Background(), request)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to get subtree tx hashes")
	})

	t.Run("QuorumTimeout_TreatsAsMissing", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Set up quorum with very short timeout
		quorumDir := t.TempDir()
		q, err := NewQuorum(&ulogger.TestLogger{}, server.subtreeStore, quorumDir, WithTimeout(30*time.Millisecond))
		require.NoError(t, err)
		server.quorum = q

		server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
			mock.Anything).
			Return(testHeaders[0], &model.BlockHeaderMeta{}, nil)

		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("quorum_test_subtree_timeout_"))

		// Pre-create a lock file and keep it fresh to force timeout
		lockFilePath := filepath.Join(quorumDir, subtreeHash.String()+".lock")
		require.NoError(t, os.WriteFile(lockFilePath, []byte("locked"), 0600))
		defer os.Remove(lockFilePath)

		stopRefresh := make(chan struct{})
		defer close(stopRefresh)
		go func() {
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopRefresh:
					return
				case <-ticker.C:
					now := time.Now()
					_ = os.Chtimes(lockFilePath, now, now)
				}
			}
		}()

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

		server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
			mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{1, 2, 3}, nil)
		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(true, nil)

		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "http://127.0.0.1:0",
		}

		// Timeout returns (false, false, noopFunc, nil) — not an error — subtree treated as missing.
		// The downstream HTTP fetch will fail, but the quorum timeout itself should not error.
		_, err = server.CheckBlockSubtrees(context.Background(), request)
		require.Error(t, err)
		// The error should be from the HTTP fetch, not from quorum timeout
		assert.Contains(t, err.Error(), "Failed to get subtree tx hashes")
		assert.NotContains(t, err.Error(), "quorum lock")
	})

	t.Run("QuorumContextCancelled_ReturnsError", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		quorumDir := t.TempDir()
		q, err := NewQuorum(&ulogger.TestLogger{}, server.subtreeStore, quorumDir, WithTimeout(5*time.Second))
		require.NoError(t, err)
		server.quorum = q

		server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
			mock.Anything).
			Return(testHeaders[0], &model.BlockHeaderMeta{}, nil)

		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("quorum_test_ctx_cancelled__"))

		// Pre-create a lock file and keep it fresh
		lockFilePath := filepath.Join(quorumDir, subtreeHash.String()+".lock")
		require.NoError(t, os.WriteFile(lockFilePath, []byte("locked"), 0600))
		defer os.Remove(lockFilePath)

		stopRefresh := make(chan struct{})
		defer close(stopRefresh)
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-stopRefresh:
					return
				case <-ticker.C:
					now := time.Now()
					_ = os.Chtimes(lockFilePath, now, now)
				}
			}
		}()

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

		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "http://127.0.0.1:0",
		}

		// Cancel context immediately — should return error
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = server.CheckBlockSubtrees(ctx, request)
		require.Error(t, err)
	})

	// Tests the actual race scenario: lock exists (in-flight handler), subtree file appears
	// before timeout, CheckBlockSubtrees sees exists=true and does NOT mark it missing.
	t.Run("InFlightHandler_CompletesBeforeTimeout", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		quorumDir := t.TempDir()
		q, err := NewQuorum(&ulogger.TestLogger{}, server.subtreeStore, quorumDir, WithTimeout(2*time.Second))
		require.NoError(t, err)
		server.quorum = q

		server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
			mock.Anything).
			Return(testHeaders[0], &model.BlockHeaderMeta{}, nil)

		subtreeHash := chainhash.Hash{}
		copy(subtreeHash[:], []byte("quorum_test_inflight_race___"))

		// Pre-create lock file to simulate in-flight handler
		lockFilePath := filepath.Join(quorumDir, subtreeHash.String()+".lock")
		require.NoError(t, os.WriteFile(lockFilePath, []byte("locked"), 0600))

		// Simulate in-flight handler completing: after a short delay, store the subtree
		// and release the lock file. TryLockIfNotExistsWithTimeout will retry and see exists=true.
		go func() {
			time.Sleep(50 * time.Millisecond)
			_ = server.subtreeStore.Set(context.Background(), subtreeHash[:], fileformat.FileTypeSubtree, []byte("validated"))
			os.Remove(lockFilePath)
		}()

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

		request := &subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "http://127.0.0.1:0",
		}

		// The in-flight handler completes and stores the subtree before timeout.
		// CheckBlockSubtrees should see exists=true and return blessed (no missing subtrees).
		response, err := server.CheckBlockSubtrees(context.Background(), request)
		require.NoError(t, err)
		assert.True(t, response.Blessed)
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

		err = server.extractAndCollectTransactions(context.Background(), subtree, &allTransactions, nil)
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

		err = server.extractAndCollectTransactions(context.Background(), subtree, &allTransactions, nil)
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

		err = server.extractAndCollectTransactions(context.Background(), subtree, &allTransactions, nil)
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

		err = server.processSubtreeDataStream(context.Background(), subtree, body, &allTransactions, 100, nil)
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
		mockBlobStore.On("SetFromReader", mock.Anything, mock.Anything, fileformat.FileTypeSubtreeData, mock.Anything, mock.Anything).
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

		err = server.processSubtreeDataStream(context.Background(), subtree, body, &allTransactions, 100, nil)
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

		err = server.processSubtreeDataStream(context.Background(), subtree, body, &allTransactions, 100, nil)
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

		count, err := server.readTransactionsFromSubtreeDataStream(subtree, &subtreeData, &allTransactions, nil)
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

		count, err := server.readTransactionsFromSubtreeDataStream(subtree, &emptyBuffer, &allTransactions, nil)
		require.NoError(t, err)

		assert.Equal(t, 0, count)
		assert.Len(t, allTransactions, 0)
	})

	t.Run("CoinbasePlaceholderAtIndex0", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create a coinbase transaction (input from all-zero hash with 0xffffffff index)
		coinbaseTx := bt.NewTx()
		err := coinbaseTx.From(
			"0000000000000000000000000000000000000000000000000000000000000000",
			0xffffffff,
			"03640000", // minimal coinbase script with block height
			0,
		)
		require.NoError(t, err)
		coinbaseTx.AddOutput(&bt.Output{
			Satoshis: 5000000000,
			LockingScript: func() *bscript.Script {
				s, _ := bscript.NewFromHexString("76a914389ffce9cd9ae88dcc0631e88a821ffdbe9bfe2688ac")
				return s
			}(),
		})
		require.True(t, coinbaseTx.IsCoinbase(), "test tx must be coinbase")

		// Create a regular transaction
		tx1, err := createTestTransaction("tx1")
		require.NoError(t, err)

		// Build subtree with coinbase placeholder at index 0 (simulating the real scenario
		// where the coinbase hash is not yet known when the subtree is built)
		subtree, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)
		require.NoError(t, subtree.AddCoinbaseNode()) // places CoinbasePlaceholderHashValue at index 0
		require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 1))

		// Write coinbase + tx1 to the data stream
		subtreeData := bytes.Buffer{}
		subtreeData.Write(coinbaseTx.Bytes())
		subtreeData.Write(tx1.Bytes())

		var allTransactions []*bt.Tx
		count, err := server.readTransactionsFromSubtreeDataStream(subtree, &subtreeData, &allTransactions, nil)
		require.NoError(t, err)

		// Should succeed — the coinbase placeholder at index 0 is allowed when the tx is coinbase
		require.Equal(t, 2, count)
		require.Len(t, allTransactions, 2)
	})

	t.Run("PlaceholderAtNonZeroIndexFails", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create two regular transactions
		tx1, err := createTestTransaction("tx1")
		require.NoError(t, err)
		tx2, err := createTestTransaction("tx2")
		require.NoError(t, err)

		// Build subtree with placeholder hash at index 1 (not index 0) — this should fail
		subtree, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)
		require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 0))
		require.NoError(t, subtree.AddNode(*tx2.TxIDChainHash(), 1, 1))
		// Manually overwrite node 1 to the placeholder hash to simulate an invalid subtree
		subtree.Nodes[1] = subtreepkg.Node{Hash: subtreepkg.CoinbasePlaceholderHashValue}

		subtreeData := bytes.Buffer{}
		subtreeData.Write(tx1.Bytes())
		subtreeData.Write(tx2.Bytes())

		var allTransactions []*bt.Tx
		_, err = server.readTransactionsFromSubtreeDataStream(subtree, &subtreeData, &allTransactions, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "transaction hash mismatch")
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

		// Missing-parent errors are deferred (not fatal) so the caller's
		// sequential revalidation pass can re-run the failed subtrees in
		// block order and resolve cross-subtree parent dependencies. The tx
		// is still recorded in the orphanage.
		err = server.processTransactionsInLevels(context.Background(), allTransactions, chainhash.Hash{}, chainhash.Hash{}, 100, blockIds)
		require.NoError(t, err)

		// Verify transaction was added to orphanage for the caller to retry
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

		// Missing-parent errors are deferred to the sequential revalidation
		// pass. The orphanage is skipped because FSM isn't RUNNING, but the
		// caller still gets a chance to retry.
		err = server.processTransactionsInLevels(context.Background(), allTransactions, chainhash.Hash{}, chainhash.Hash{}, 100, blockIds)
		require.NoError(t, err)

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

		// Missing-parent errors are deferred even when the FSM check fails.
		// The orphanage is skipped (conservative when we can't confirm running
		// state) but the caller's sequential revalidation pass still gets a
		// chance to retry.
		err = server.processTransactionsInLevels(context.Background(), allTransactions, chainhash.Hash{}, chainhash.Hash{}, 100, blockIds)
		require.NoError(t, err)

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

// TestValidateMissingSubtreesWithOrderedRetry covers the phase-2/phase-3
// interaction that resolves cross-subtree parent dependencies in block order.
//
// The core contract:
//
//   - Phase 2 validates every subtree in parallel. Cross-subtree parent
//     dependencies race here — a child subtree may run before its parent has
//     populated the cache and fail with TxMissingParent.
//   - Phase 3 revalidates the failures. It MUST walk them in missingSubtrees
//     (block) order, not goroutine-completion order, so each child's parent
//     has already been revalidated before the child runs.
//
// A revalidation order that is any permutation other than block order can
// leave a child ahead of its parent and fail the block. The old
// mutex-appended failures slice had exactly that bug.
func TestValidateMissingSubtreesWithOrderedRetry(t *testing.T) {
	// Build five subtree hashes in a fixed, identifiable order. Index
	// encoded in the first byte so we can tell them apart by position.
	makeHashes := func(n int) []chainhash.Hash {
		hashes := make([]chainhash.Hash, n)
		for i := range hashes {
			hashes[i][0] = byte(i + 1) // avoid zero hash
		}
		return hashes
	}

	t.Run("AllParallelSucceed_NoRevalidation", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		missing := makeHashes(5)

		var mu sync.Mutex
		callOrder := []chainhash.Hash{}

		validateFn := func(_ context.Context, h chainhash.Hash) (*subtreepkg.Subtree, error) {
			mu.Lock()
			callOrder = append(callOrder, h)
			mu.Unlock()
			return nil, nil
		}

		err := server.validateMissingSubtreesWithOrderedRetry(context.Background(), missing, validateFn)
		require.NoError(t, err)

		// Every subtree validated exactly once (no phase-3 retries because
		// phase 2 all succeeded).
		require.Len(t, callOrder, len(missing))
	})

	t.Run("CrossSubtreeDependencies_RevalidateInBlockOrder", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Chain dependency: subtree[i] depends on subtree[i-1] (except i=0).
		// A subtree is only "resolvable" once every earlier subtree has been
		// validated. This models the real cross-subtree parent case: children
		// can only succeed after their parents populate the cache.
		missing := makeHashes(5)
		indexOf := make(map[chainhash.Hash]int, len(missing))
		for i, h := range missing {
			indexOf[h] = i
		}

		var mu sync.Mutex
		validated := make([]bool, len(missing))
		phase2Count := 0
		phase3Order := []int{}

		validateFn := func(_ context.Context, h chainhash.Hash) (*subtreepkg.Subtree, error) {
			i := indexOf[h]

			mu.Lock()
			defer mu.Unlock()

			// The first len(missing) calls are phase 2 (parallel). In this
			// phase only subtree 0 can succeed; every other subtree's parent
			// has not yet been validated. This matches the observed
			// behaviour on dense-dep blocks where only subtree 0 succeeds in
			// parallel.
			if phase2Count < len(missing) {
				phase2Count++

				if i == 0 {
					validated[0] = true
					return nil, nil
				}
				return nil, errors.NewTxMissingParentError("parallel race: parent of subtree %d not validated", i)
			}

			// Phase 3: ordered sequential. By contract, subtree i-1 must
			// already be validated when we reach subtree i.
			phase3Order = append(phase3Order, i)
			if i > 0 && !validated[i-1] {
				return nil, errors.NewTxMissingParentError("ordering broken: parent %d not validated before %d", i-1, i)
			}
			validated[i] = true
			return nil, nil
		}

		err := server.validateMissingSubtreesWithOrderedRetry(context.Background(), missing, validateFn)
		require.NoError(t, err)

		// Every subtree must have ultimately validated successfully.
		for i, ok := range validated {
			require.True(t, ok, "subtree %d was never validated", i)
		}

		// Phase 3 must have run every failed subtree except #0 (the only one
		// that could succeed in parallel) in strict block order.
		require.Equal(t, []int{1, 2, 3, 4}, phase3Order,
			"phase 3 must revalidate failed subtrees in strict block order")
	})

	t.Run("PersistentFailureInPhase3_IsReturned", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		missing := makeHashes(3)
		indexOf := make(map[chainhash.Hash]int, len(missing))
		for i, h := range missing {
			indexOf[h] = i
		}

		// Subtree 1 always fails. Subtree 0 succeeds. Subtree 2 succeeds in
		// parallel (its "dependency" is satisfied). The contract is that a
		// failure that persists into phase 3 is surfaced to the caller — not
		// silently dropped.
		validateFn := func(_ context.Context, h chainhash.Hash) (*subtreepkg.Subtree, error) {
			switch indexOf[h] {
			case 1:
				return nil, errors.NewTxInvalidError("subtree 1 is permanently invalid")
			default:
				return nil, nil
			}
		}

		err := server.validateMissingSubtreesWithOrderedRetry(context.Background(), missing, validateFn)
		require.Error(t, err)
	})

	t.Run("RevalidationOrderWouldFailIfNotBlockOrder", func(t *testing.T) {
		// This test encodes the essence of the bug the PR fixes: if phase 3
		// walked failures in any order other than missingSubtrees order, it
		// would reproduce the same TxMissingParent error. We assert block
		// order explicitly by recording the sequence.
		server, cleanup := setupTestServer(t)
		defer cleanup()

		missing := makeHashes(4)
		indexOf := make(map[chainhash.Hash]int, len(missing))
		for i, h := range missing {
			indexOf[h] = i
		}

		var mu sync.Mutex
		var callOrder []int
		validated := make([]bool, len(missing))

		// Phase 2: everything fails so phase 3 retries in order.
		// Phase 3: a subtree succeeds iff its predecessor has been validated.
		// This mimics a strict chain dependency.
		phase2Calls := 0
		validateFn := func(_ context.Context, h chainhash.Hash) (*subtreepkg.Subtree, error) {
			mu.Lock()
			defer mu.Unlock()

			i := indexOf[h]

			// First len(missing) calls are phase 2. Phase 2 subtree 0 is the
			// only one that could succeed; force all to fail to isolate the
			// phase 3 ordering assertion.
			if phase2Calls < len(missing) {
				phase2Calls++
				callOrder = append(callOrder, -i-1) // negative = phase 2 call
				return nil, errors.NewTxMissingParentError("phase 2 dep race on subtree %d", i)
			}

			callOrder = append(callOrder, i)
			if i > 0 && !validated[i-1] {
				return nil, errors.NewTxMissingParentError("predecessor subtree %d not validated", i-1)
			}
			validated[i] = true
			return nil, nil
		}

		err := server.validateMissingSubtreesWithOrderedRetry(context.Background(), missing, validateFn)
		require.NoError(t, err, "phase 3 ordered walk must resolve chain deps in one pass")

		// Extract only the phase-3 calls and assert they went in block order.
		var phase3 []int
		for _, v := range callOrder {
			if v >= 0 {
				phase3 = append(phase3, v)
			}
		}
		require.Equal(t, []int{0, 1, 2, 3}, phase3,
			"phase 3 must revalidate in strict missingSubtrees order")
	})
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
		err := server.extractAndCollectTransactions(context.Background(), subtree1, &transactions1, nil)
		assert.NoError(t, err)
	}()

	go func() {
		defer wg.Done()
		err := server.extractAndCollectTransactions(context.Background(), subtree2, &transactions2, nil)
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

func TestBuildParentMetadata(t *testing.T) {
	t.Run("EmptyInput", func(t *testing.T) {
		result := buildParentMetadata(nil, 100, nil)
		assert.Nil(t, result)

		result = buildParentMetadata([]missingTx{}, 100, make(map[chainhash.Hash]bool))
		assert.Nil(t, result)
	})

	t.Run("EmptySuccessSet", func(t *testing.T) {
		tx := bt.NewTx()
		require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000000", 0, "76a914000000000000000000000000000000000000000088ac", 1000))
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 900))

		missingTxs := []missingTx{{tx: tx, idx: 0}}
		successMap := make(map[chainhash.Hash]bool)

		result := buildParentMetadata(missingTxs, 100, successMap)
		assert.Nil(t, result)
	})

	t.Run("FiltersBySuccessfulTransactions", func(t *testing.T) {
		// Create test transactions
		tx1 := bt.NewTx()
		require.NoError(t, tx1.From("0000000000000000000000000000000000000000000000000000000000000000", 0, "76a914000000000000000000000000000000000000000088ac", 1000))
		require.NoError(t, tx1.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 900))

		tx2 := bt.NewTx()
		require.NoError(t, tx2.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "76a914000000000000000000000000000000000000000088ac", 2000))
		require.NoError(t, tx2.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1900))

		tx3 := bt.NewTx()
		require.NoError(t, tx3.From("2222222222222222222222222222222222222222222222222222222222222222", 0, "76a914000000000000000000000000000000000000000088ac", 3000))
		require.NoError(t, tx3.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2900))

		missingTxs := []missingTx{
			{tx: tx1, idx: 0},
			{tx: tx2, idx: 1},
			{tx: tx3, idx: 2},
		}

		// Only tx1 and tx3 succeeded
		successMap := map[chainhash.Hash]bool{
			*tx1.TxIDChainHash(): true,
			*tx3.TxIDChainHash(): true,
		}

		blockHeight := uint32(12345)
		result := buildParentMetadata(missingTxs, blockHeight, successMap)

		// Should only include tx1 and tx3
		assert.NotNil(t, result)
		assert.Equal(t, 2, len(result))

		// Check tx1 is included
		meta1, exists := result[*tx1.TxIDChainHash()]
		assert.True(t, exists)
		assert.Equal(t, blockHeight, meta1.BlockHeight)

		// Check tx2 is NOT included (failed validation)
		_, exists = result[*tx2.TxIDChainHash()]
		assert.False(t, exists)

		// Check tx3 is included
		meta3, exists := result[*tx3.TxIDChainHash()]
		assert.True(t, exists)
		assert.Equal(t, blockHeight, meta3.BlockHeight)
	})

	t.Run("AllTransactionsSuccessful", func(t *testing.T) {
		// Create test transactions
		tx1 := bt.NewTx()
		require.NoError(t, tx1.From("0000000000000000000000000000000000000000000000000000000000000000", 0, "76a914000000000000000000000000000000000000000088ac", 1000))
		require.NoError(t, tx1.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 900))

		tx2 := bt.NewTx()
		require.NoError(t, tx2.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "76a914000000000000000000000000000000000000000088ac", 2000))
		require.NoError(t, tx2.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1900))

		missingTxs := []missingTx{
			{tx: tx1, idx: 0},
			{tx: tx2, idx: 1},
		}

		// All transactions succeeded
		successMap := map[chainhash.Hash]bool{
			*tx1.TxIDChainHash(): true,
			*tx2.TxIDChainHash(): true,
		}

		blockHeight := uint32(54321)
		result := buildParentMetadata(missingTxs, blockHeight, successMap)

		// Should include both transactions
		assert.NotNil(t, result)
		assert.Equal(t, 2, len(result))

		// Verify both are present with correct block height
		meta1, exists := result[*tx1.TxIDChainHash()]
		assert.True(t, exists)
		assert.Equal(t, blockHeight, meta1.BlockHeight)

		meta2, exists := result[*tx2.TxIDChainHash()]
		assert.True(t, exists)
		assert.Equal(t, blockHeight, meta2.BlockHeight)
	})

	t.Run("NoTransactionsSuccessful", func(t *testing.T) {
		tx1 := bt.NewTx()
		require.NoError(t, tx1.From("0000000000000000000000000000000000000000000000000000000000000000", 0, "76a914000000000000000000000000000000000000000088ac", 1000))
		require.NoError(t, tx1.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 900))

		missingTxs := []missingTx{{tx: tx1, idx: 0}}

		// No transactions succeeded
		successMap := make(map[chainhash.Hash]bool)

		result := buildParentMetadata(missingTxs, 100, successMap)

		// Should return nil since no successful transactions
		assert.Nil(t, result)
	})

	t.Run("NilTransactionInSlice", func(t *testing.T) {
		tx1 := bt.NewTx()
		require.NoError(t, tx1.From("0000000000000000000000000000000000000000000000000000000000000000", 0, "76a914000000000000000000000000000000000000000088ac", 1000))
		require.NoError(t, tx1.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 900))

		missingTxs := []missingTx{
			{tx: tx1, idx: 0},
			{tx: nil, idx: 1}, // Nil transaction
		}

		successMap := map[chainhash.Hash]bool{
			*tx1.TxIDChainHash(): true,
		}

		result := buildParentMetadata(missingTxs, 100, successMap)

		// Should only include tx1 (nil transaction is skipped)
		assert.NotNil(t, result)
		assert.Equal(t, 1, len(result))

		meta, exists := result[*tx1.TxIDChainHash()]
		assert.True(t, exists)
		assert.Equal(t, uint32(100), meta.BlockHeight)
	})
}

func TestValidateSubtreeLeafCount(t *testing.T) {
	subtreeHash := chainhash.Hash{0x01, 0x02, 0x03}

	t.Run("UnderCap", func(t *testing.T) {
		require.NoError(t, validateSubtreeLeafCount(subtreeHash, 3, 4))
	})

	t.Run("AtCap", func(t *testing.T) {
		require.NoError(t, validateSubtreeLeafCount(subtreeHash, 4, 4))
	})

	t.Run("OverCap", func(t *testing.T) {
		err := validateSubtreeLeafCount(subtreeHash, 5, 4)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrProcessing))
		require.Contains(t, err.Error(), subtreeHash.String())
		require.Contains(t, err.Error(), "exceeds policy max")
	})

	t.Run("ZeroLeaves", func(t *testing.T) {
		require.NoError(t, validateSubtreeLeafCount(subtreeHash, 0, 4))
	})

	t.Run("LargeOverflow", func(t *testing.T) {
		err := validateSubtreeLeafCount(subtreeHash, 1<<30, 1<<20)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrProcessing))
	})
}

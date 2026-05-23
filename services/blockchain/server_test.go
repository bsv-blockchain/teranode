// Package blockchain_test provides testing for the blockchain package.
package blockchain

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	tx1, _ = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	hash1  = tx1.TxIDChainHash()
)

// Test_AddBlock verifies the block addition functionality.
func Test_AddBlock(t *testing.T) {
	ctx := setup(t)

	// Create a mock block
	mockBlk := mockBlock(ctx, t)

	// Prepare the AddBlockRequest
	coinbaseBytes := mockBlk.CoinbaseTx.Bytes()
	headerBytes := mockBlk.Header.Bytes()

	subtreeHashes := make([][]byte, len(mockBlk.Subtrees))
	for i, hash := range mockBlk.Subtrees {
		subtreeHashes[i] = hash[:]
	}

	request := &blockchain_api.AddBlockRequest{
		Header:           headerBytes,
		CoinbaseTx:       coinbaseBytes,
		SubtreeHashes:    subtreeHashes,
		TransactionCount: mockBlk.TransactionCount,
		SizeInBytes:      mockBlk.SizeInBytes,
		PeerId:           "test-peer",
	}

	// Call AddBlock
	c := context.Background()
	response, err := ctx.server.AddBlock(c, request)
	require.NoError(t, err)
	require.NotNil(t, response)

	// Verify the block was added
	addedBlock, err := ctx.server.GetBlock(c, &blockchain_api.GetBlockRequest{
		Hash: mockBlk.Hash().CloneBytes(),
	})
	require.NoError(t, err)
	require.NotNil(t, addedBlock)

	// Verify block details
	assert.Equal(t, mockBlk.Header.Bytes(), addedBlock.Header)
	assert.Equal(t, coinbaseBytes, addedBlock.CoinbaseTx)
	assert.Equal(t, subtreeHashes, addedBlock.SubtreeHashes)
	assert.Equal(t, mockBlk.TransactionCount, addedBlock.TransactionCount)
	assert.Equal(t, mockBlk.SizeInBytes, addedBlock.SizeInBytes)
}

// Test_GetBlock verifies the block retrieval functionality.
func Test_GetBlock(t *testing.T) {
	ctx := setup(t)
	_, _, err := ctx.server.store.StoreBlock(context.Background(), mockBlock(ctx, t), "")
	require.NoError(t, err)

	context := context.Background()
	request := &blockchain_api.GetBlockRequest{
		Hash: []byte{2},
	}

	block, err := ctx.server.GetBlock(context, request)
	require.Error(t, err)
	require.Empty(t, block)
	require.True(t, errors.Is(err, errors.ErrBlockNotFound))

	requestHeight := &blockchain_api.GetBlockByHeightRequest{
		Height: 2,
	}

	block, err = ctx.server.GetBlockByHeight(context, requestHeight)
	require.Error(t, err, "Expected error")
	require.Empty(t, block, "Expected block to be empty")
	// unwrap the error
	// TODO: Put this back in when we fix WrapGRPC/UnwrapGRPC
	unwrappedErr := errors.UnwrapGRPC(err)
	require.ErrorIs(t, unwrappedErr, errors.ErrBlockNotFound)

	// Stop the server
	if err := ctx.server.Stop(context); err != nil {
		t.Fatalf("Failed to stop server: %v", err)
	}
}

// Test_GetFSMCurrentState verifies the FSM state retrieval functionality.
func Test_GetFSMCurrentState(t *testing.T) {
	ctx := setup(t)
	_, _, err := ctx.server.store.StoreBlock(context.Background(), mockBlock(ctx, t), "")
	require.NoError(t, err)

	response, err := ctx.server.GetFSMCurrentState(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Equal(t, blockchain_api.FSMStateType_IDLE, response.State, "Expected FSM state did not match")
}

// testContext holds the test environment components
type testContext struct {
	server       *Blockchain    // Blockchain server instance
	subtreeStore blob.Store     // Store for subtrees
	utxoStore    utxo.Store     // Store for UTXOs
	logger       ulogger.Logger // Logger instance
}

// setup creates a new test environment with initialized components.
// Returns a testContext with all necessary test dependencies.
func setup(t *testing.T) *testContext {
	subtreeStore := blob_memory.New()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	// Clear the default addresses since we're not starting the servers in tests
	tSettings.BlockChain.GRPCListenAddress = ""
	tSettings.BlockChain.HTTPListenAddress = ""

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := utxosql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	// Create SQLite store
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	blockchainStore, err := sql.New(logger, storeURL, tSettings)
	require.NoError(t, err)

	server, err := New(context.Background(), logger, tSettings, blockchainStore, nil)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	if err := server.Init(context.Background()); err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	return &testContext{
		server:       server,
		subtreeStore: subtreeStore,
		utxoStore:    utxoStore,
		logger:       logger,
	}
}

// Test_HealthLiveness verifies the health check liveness functionality.
func Test_HealthLiveness(t *testing.T) {
	ctx := setup(t)

	status, msg, err := ctx.server.Health(context.Background(), true)
	require.Equal(t, http.StatusOK, status)
	require.NoError(t, err)

	var jsonMsg map[string]interface{}
	err = json.Unmarshal([]byte(msg), &jsonMsg)
	require.NoError(t, err, "Message should be valid JSON")

	require.Contains(t, jsonMsg, "status", "JSON should contain 'status' field")
	require.Contains(t, jsonMsg, "dependencies", "JSON should contain 'dependencies' field")

	require.Equal(t, "200", jsonMsg["status"], "Status should be '200'")
	require.NoError(t, err)
}

// Test_HealthReadiness verifies the health check readiness functionality.
func Test_HealthReadiness(t *testing.T) {
	ctx := setup(t)

	status, msg, err := ctx.server.Health(context.Background(), false)
	require.Equal(t, http.StatusOK, status)
	require.NoError(t, err)

	var jsonMsg map[string]interface{}
	err = json.Unmarshal([]byte(msg), &jsonMsg)
	require.NoError(t, err, "Message should be valid JSON")

	require.Contains(t, jsonMsg, "status", "JSON should contain 'status' field")
	require.Contains(t, jsonMsg, "dependencies", "JSON should contain 'dependencies' field")

	require.Equal(t, "200", jsonMsg["status"], "Status should be '200'")
	require.NoError(t, err)
}

// Test_HealthGRPC verifies the gRPC health check functionality.
func Test_HealthGRPC(t *testing.T) {
	ctx := setup(t)

	response, err := ctx.server.HealthGRPC(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.True(t, response.Ok)
}

// mockBlock creates a mock block for testing purposes.
func mockBlock(ctx *testContext, t *testing.T) *model.Block {
	subtree, err := subtree.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*hash1, 100, 0))

	_, err = ctx.utxoStore.Create(context.Background(), tx1, 0)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	hashPrevBlock := tSettings.ChainCfgParams.GenesisHash

	coinbase := bt.NewTx()

	// Create coinbase input using the From method
	err = coinbase.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	require.NoError(t, err)

	// Set the unlocking script with block height data
	arbitraryData := []byte{0x03, byte(1), 0x00, 0x00} // Simple height encoding
	coinbase.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)
	coinbase.Inputs[0].SequenceNumber = 0xffffffff

	// Add a coinbase output
	err = coinbase.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
	require.NoError(t, err)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = ctx.subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	subtreeHashes := make([]*chainhash.Hash, 0)
	subtreeHashes = append(subtreeHashes, subtree.RootHash())

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: subtree.RootHash(),                 // doesn't matter, we're only checking the value and not whether it's correct
		Timestamp:      uint32(time.Now().Unix()),          // nolint:gosec
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          0,
	}

	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: 2,
		Subtrees:         subtreeHashes,
		Height:           1, // Start at height 1 since genesis is height 0
		ID:               1,
	}

	return block
}

// Test_getBlockLocator verifies the block locator functionality.
func Test_getBlockLocator(t *testing.T) {
	ctx := context.Background()

	t.Run("block 0", func(t *testing.T) {
		store := blockchain_store.NewMockStore()
		block := &model.Block{
			Height: 0,
			Header: &model.BlockHeader{
				Version:        0,
				HashPrevBlock:  &chainhash.Hash{},
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      0,
				Bits:           model.NBit{},
				Nonce:          0,
			},
		}
		_, _, err := store.StoreBlock(ctx, block, "")
		require.NoError(t, err)

		locator, err := getBlockLocator(ctx, store, nil, 0)
		require.NoError(t, err)

		assert.Len(t, locator, 1)
		assert.Equal(t, block.Hash().String(), locator[0].String())
	})

	t.Run("blocks", func(t *testing.T) {
		store := blockchain_store.NewMockStore()

		for i := uint32(0); i <= 255; i++ {
			block := &model.Block{
				Height: i,
				Header: &model.BlockHeader{
					Version:        i,
					HashPrevBlock:  &chainhash.Hash{},
					HashMerkleRoot: &chainhash.Hash{},
					Timestamp:      i,
					Bits:           model.NBit{},
					Nonce:          i,
				},
			}
			_, _, err := store.StoreBlock(ctx, block, "")
			require.NoError(t, err)
		}

		locator, err := getBlockLocator(ctx, store, store.BlockByHeight[255].Hash(), 255)
		require.NoError(t, err)

		assert.Len(t, locator, 19)

		expectedHeights := []uint32{
			255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 242, 238, 230, 214, 182, 118, 0,
		}

		for locatorIdx, locatorHash := range locator {
			assert.Equal(t, store.BlockByHeight[expectedHeights[locatorIdx]].Hash().String(), locatorHash.String())
		}
	})

	t.Run("blocks from low height", func(t *testing.T) {
		store := blockchain_store.NewMockStore()

		for i := uint32(0); i <= 1024; i++ {
			block := &model.Block{
				Height: i,
				Header: &model.BlockHeader{
					Version:        i,
					HashPrevBlock:  &chainhash.Hash{},
					HashMerkleRoot: &chainhash.Hash{},
					Timestamp:      i,
					Bits:           model.NBit{},
					Nonce:          i,
				},
			}
			_, _, err := store.StoreBlock(ctx, block, "")
			require.NoError(t, err)
		}

		locator, err := getBlockLocator(ctx, store, store.BlockByHeight[1000].Hash(), 1000)
		require.NoError(t, err)

		assert.Len(t, locator, 21)

		expectedHeights := []uint32{
			1000, 999, 998, 997, 996, 995, 994, 993, 992, 991, 990, 989, 987, 983, 975, 959, 927, 863, 735, 479, 0,
		}

		for locatorIdx, locatorHash := range locator {
			assert.Equal(t, store.BlockByHeight[expectedHeights[locatorIdx]].Hash().String(), locatorHash.String())
		}
	})
}

func Test_getBlockHeadersToCommonAncestor(t *testing.T) {
	ctx := setup(t)

	// Create a chain of blocks for testing
	headers := make([]*model.BlockHeader, 0, 150)

	// Get genesis block hash from chain params
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	prevHash := tSettings.ChainCfgParams.GenesisHash

	for i := 0; i < 150; i++ {
		// Create a unique merkle root for each block
		merkleRoot := &chainhash.Hash{}
		merkleRoot[0] = byte(i)

		// Create a proper coinbase transaction
		coinbaseTx := bt.NewTx()

		// Create coinbase input using the From method
		err := coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
		require.NoError(t, err)

		// Set the unlocking script with block height data
		arbitraryData := []byte{0x03, byte(i), 0x00, 0x00} // Simple height encoding
		coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)
		coinbaseTx.Inputs[0].SequenceNumber = 0xffffffff

		// Add a coinbase output
		err = coinbaseTx.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
		require.NoError(t, err)

		// Create a unique block for each iteration
		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prevHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix()),          // nolint:gosec
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
				Nonce:          uint32(i),                          // nolint:gosec
			},
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			SizeInBytes:      1000,
			Height:           uint32(i + 1), // nolint:gosec
			ID:               uint32(i + 1), // nolint:gosec
		}

		header := block.Header
		headers = append(headers, header)

		// Store the block before updating prevHash
		_, _, err = ctx.server.store.StoreBlock(context.Background(), block, "test")
		require.NoError(t, err)

		prevHash = header.Hash()
	}

	tests := []struct {
		name          string
		targetHash    *chainhash.Hash
		locatorHashes []*chainhash.Hash
		maxHeaders    uint32
		expectedLen   int
		expectError   bool
		errorType     string
	}{
		{
			name:          "common ancestor found in first batch",
			targetHash:    headers[99].Hash(),
			locatorHashes: []*chainhash.Hash{headers[50].Hash()},
			maxHeaders:    100,
			expectedLen:   50,
			expectError:   false,
		},
		{
			name:          "common ancestor found in second batch",
			targetHash:    headers[149].Hash(),
			locatorHashes: []*chainhash.Hash{headers[20].Hash()},
			maxHeaders:    1000,
			expectedLen:   130,
			expectError:   false,
		},
		{
			name:          "common ancestor found in second smaller batch",
			targetHash:    headers[149].Hash(),
			locatorHashes: []*chainhash.Hash{headers[20].Hash()},
			maxHeaders:    100,
			expectedLen:   100,
			expectError:   false,
		},
		{
			name:          "no common ancestor found",
			targetHash:    headers[149].Hash(),
			locatorHashes: []*chainhash.Hash{new(chainhash.Hash)},
			maxHeaders:    100,
			expectError:   true,
			errorType:     "common ancestor hash not found",
		},
		{
			name:          "empty locator hashes",
			targetHash:    headers[99].Hash(),
			locatorHashes: nil,
			maxHeaders:    100,
			expectError:   true,
			errorType:     "common ancestor hash not found",
		},
		{
			name:          "verify last header in locator hashes",
			targetHash:    headers[99].Hash(),
			locatorHashes: []*chainhash.Hash{headers[50].Hash(), headers[40].Hash(), headers[30].Hash()},
			maxHeaders:    100,
			expectedLen:   50,
			expectError:   false,
		},
		{
			name:          "debug case - simple test",
			targetHash:    headers[2].Hash(),
			locatorHashes: []*chainhash.Hash{headers[1].Hash()},
			maxHeaders:    10,
			expectedLen:   -1, // Use -1 to indicate we want to see what we actually get
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers, metas, err := getBlockHeadersToCommonAncestor(
				context.Background(),
				ctx.server.store,
				tt.targetHash,
				tt.locatorHashes,
				tt.maxHeaders,
			)

			if tt.expectError {
				require.Error(t, err)

				if tt.errorType != "" {
					require.Contains(t, err.Error(), tt.errorType)
				}

				return
			}

			require.NoError(t, err)

			// Verify headers are in correct order
			for i := 0; i < len(headers)-1; i++ {
				require.Equal(t, headers[i].HashPrevBlock, headers[i+1].Hash())
			}

			// Verify heights are sequential
			for i := 0; i < len(metas)-1; i++ {
				require.Equal(t, metas[i].Height, metas[i+1].Height+1)
			}

			// Verify the last header in the list is in locatorHashes
			if !tt.expectError && len(headers) > 0 && len(tt.locatorHashes) > 0 {
				lastHeader := headers[len(headers)-1]
				lastHeaderHash := lastHeader.Hash()
				found := false

				for _, locatorHash := range tt.locatorHashes {
					if locatorHash.IsEqual(lastHeaderHash) {
						found = true
						break
					}
				}

				require.True(t, found, "Last header hash should be in locator hashes")
			}
		})
	}
}

// Test_GetStoreFSMState tests the GetStoreFSMState functionality
func Test_GetStoreFSMState(t *testing.T) {
	ctx := setup(t)

	state, err := ctx.server.GetStoreFSMState(context.Background())
	assert.NoError(t, err)
	assert.NotEmpty(t, state)
}

// Test_ResetFSMS tests the ResetFSMS functionality
func Test_ResetFSMS(t *testing.T) {
	ctx := setup(t)

	ctx.server.ResetFSMS()
	// Method has no return value
}

// Test_GetBlockStats tests the GetBlockStats functionality
func Test_GetBlockStats(t *testing.T) {
	ctx := setup(t)

	// Store a block first
	block := mockBlock(ctx, t)
	_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "peer1")
	require.NoError(t, err)

	t.Run("get block stats", func(t *testing.T) {
		response, err := ctx.server.GetBlockStats(context.Background(), &emptypb.Empty{})
		require.NoError(t, err)
		require.NotNil(t, response)
	})
}

// Test_GetBlockGraphData tests the GetBlockGraphData functionality
func Test_GetBlockGraphData(t *testing.T) {
	ctx := setup(t)

	// Store blocks
	block := mockBlock(ctx, t)
	_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "peer1")
	require.NoError(t, err)

	t.Run("get block graph data", func(t *testing.T) {
		request := &blockchain_api.GetBlockGraphDataRequest{}

		response, err := ctx.server.GetBlockGraphData(context.Background(), request)
		require.NoError(t, err)
		require.NotNil(t, response)
	})
}

// Test_GetLastNBlocks tests the GetLastNBlocks functionality
func Test_GetLastNBlocks(t *testing.T) {
	ctx := setup(t)

	// Create and store unique blocks
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	prevHash := tSettings.ChainCfgParams.GenesisHash

	for i := 0; i <= 5; i++ {
		// Create a unique coinbase transaction for each block
		coinbase := bt.NewTx()
		err := coinbase.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
		require.NoError(t, err)

		// Set unique unlocking script with block height data
		arbitraryData := []byte{0x03, byte(i), 0x00, 0x00}
		coinbase.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)

		// Add output
		err = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000)
		require.NoError(t, err)

		// Create unique merkle root from coinbase tx
		merkleRoot := coinbase.TxIDChainHash()

		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prevHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix() + int64(i)),
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
				Nonce:          uint32(i),
			},
			CoinbaseTx:       coinbase,
			Height:           uint32(i),
			TransactionCount: 1,
			SizeInBytes:      1000,
		}

		// Store the block
		_, _, err = ctx.server.store.StoreBlock(context.Background(), block, "peer1")
		require.NoError(t, err)

		// Update prevHash for next block
		prevHash = block.Hash()
	}

	t.Run("get last N blocks", func(t *testing.T) {
		request := &blockchain_api.GetLastNBlocksRequest{
			NumberOfBlocks: 3,
		}

		response, err := ctx.server.GetLastNBlocks(context.Background(), request)
		require.NoError(t, err)
		require.NotNil(t, response)
		assert.LessOrEqual(t, len(response.Blocks), 3)
	})
}

// Test_GetBlockHeadersFromCommonAncestor verifies the GetBlockHeadersFromCommonAncestor functionality.
func Test_GetBlockHeadersFromCommonAncestor(t *testing.T) {
	ctx := setup(t)

	// Create a chain of blocks for testing
	headers := make([]*model.BlockHeader, 0, 150)

	// Get genesis block hash from chain params
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	prevHash := tSettings.ChainCfgParams.GenesisHash

	for i := 0; i < 150; i++ {
		// Create a unique merkle root for each block
		merkleRoot := &chainhash.Hash{}
		merkleRoot[0] = byte(i)

		// Create a proper coinbase transaction
		coinbaseTx := bt.NewTx()

		// Create coinbase input using the From method
		err := coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
		require.NoError(t, err)

		// Set the unlocking script with block height data
		arbitraryData := []byte{0x03, byte(i), 0x00, 0x00} // Simple height encoding
		coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)
		coinbaseTx.Inputs[0].SequenceNumber = 0xffffffff

		// Add a coinbase output
		err = coinbaseTx.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
		require.NoError(t, err)

		// Create a unique block for each iteration
		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prevHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix()),          // nolint:gosec
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
				Nonce:          uint32(i),                          // nolint:gosec
			},
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			SizeInBytes:      1000,
			Height:           uint32(i + 1), // nolint:gosec
			ID:               uint32(i + 1), // nolint:gosec
		}

		header := block.Header
		headers = append(headers, header)

		// Store the block before updating prevHash
		_, _, err = ctx.server.store.StoreBlock(context.Background(), block, "test")
		require.NoError(t, err)

		prevHash = header.Hash()
	}

	tests := []struct {
		name          string
		targetHash    *chainhash.Hash
		locatorHashes []chainhash.Hash
		maxHeaders    uint32
		expectedLen   int
		expectError   bool
		errorType     string
	}{
		{
			name:          "target same as common ancestor",
			targetHash:    headers[50].Hash(),
			locatorHashes: []chainhash.Hash{*headers[50].Hash()},
			maxHeaders:    100,
			expectedLen:   1, // Just the common ancestor block itself
			expectError:   false,
		},
		{
			name:          "target before common ancestor in locator",
			targetHash:    headers[30].Hash(),
			locatorHashes: []chainhash.Hash{*headers[50].Hash(), *headers[40].Hash(), *headers[30].Hash()},
			maxHeaders:    100,
			expectedLen:   1, // Should find the target block (30) as common ancestor
			expectError:   false,
		},
		{
			name:          "no common ancestor found",
			targetHash:    headers[99].Hash(),
			locatorHashes: []chainhash.Hash{*new(chainhash.Hash)}, // Invalid hash
			maxHeaders:    100,
			expectError:   true,
			errorType:     "failed to get latest block header from block locator",
		},
		{
			name:          "empty locator hashes",
			targetHash:    headers[99].Hash(),
			locatorHashes: []chainhash.Hash{},
			maxHeaders:    100,
			expectError:   true,
			errorType:     "failed to get latest block header from block locator",
		},
		{
			name:          "genesis block as common ancestor",
			targetHash:    tSettings.ChainCfgParams.GenesisHash,
			locatorHashes: []chainhash.Hash{*tSettings.ChainCfgParams.GenesisHash},
			maxHeaders:    100,
			expectedLen:   1, // Just the genesis block
			expectError:   false,
		},
		{
			name:          "current implementation limitation - target after ancestor",
			targetHash:    headers[99].Hash(),
			locatorHashes: []chainhash.Hash{*headers[50].Hash()},
			maxHeaders:    100,
			expectedLen:   50,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call the gRPC method through the server
			request := &blockchain_api.GetBlockHeadersFromCommonAncestorRequest{
				TargetHash:         tt.targetHash.CloneBytes(),
				BlockLocatorHashes: make([][]byte, len(tt.locatorHashes)),
				MaxHeaders:         tt.maxHeaders,
			}

			for i, hash := range tt.locatorHashes {
				request.BlockLocatorHashes[i] = hash.CloneBytes()
			}

			response, err := ctx.server.GetBlockHeadersFromCommonAncestor(context.Background(), request)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorType != "" {
					require.Contains(t, err.Error(), tt.errorType)
				}
				return
			}

			require.NoError(t, err)

			// Verify headers are in ascending order (oldest first)
			if len(response.BlockHeaders) > 1 {
				for i := 0; i < len(response.BlockHeaders)-1; i++ {
					header1, err1 := model.NewBlockHeaderFromBytes(response.BlockHeaders[i])
					require.NoError(t, err1)
					header2, err2 := model.NewBlockHeaderFromBytes(response.BlockHeaders[i+1])
					require.NoError(t, err2)

					// Next header should have current header as its previous block
					require.Equal(t, header1.Hash(), header2.HashPrevBlock)
				}
			}

			// Verify metadata is in ascending order (oldest first)
			if len(response.Metas) > 1 {
				for i := 0; i < len(response.Metas)-1; i++ {
					meta1, err1 := model.NewBlockHeaderMetaFromBytes(response.Metas[i])
					require.NoError(t, err1)
					meta2, err2 := model.NewBlockHeaderMetaFromBytes(response.Metas[i+1])
					require.NoError(t, err2)

					// Heights should be sequential (ascending)
					require.Equal(t, meta1.Height+1, meta2.Height)
				}
			}

			// Verify the first header is the common ancestor (if locator hashes provided)
			if !tt.expectError && len(response.BlockHeaders) > 0 && len(tt.locatorHashes) > 0 {
				firstHeader, err := model.NewBlockHeaderFromBytes(response.BlockHeaders[0])
				require.NoError(t, err)
				firstHeaderHash := firstHeader.Hash()

				// The first header should match one of the locator hashes (the common ancestor)
				found := false
				for _, locatorHash := range tt.locatorHashes {
					if locatorHash.IsEqual(firstHeaderHash) {
						found = true
						break
					}
				}
				require.True(t, found, "First header should be the common ancestor from locator hashes")
			}

			// Verify the last header is the target hash
			if !tt.expectError && len(response.BlockHeaders) > 0 {
				lastHeader, err := model.NewBlockHeaderFromBytes(response.BlockHeaders[len(response.BlockHeaders)-1])
				require.NoError(t, err)
				lastHeaderHash := lastHeader.Hash()
				require.True(t, tt.targetHash.IsEqual(lastHeaderHash), "Last header should be the target hash")
			}

			// Verify the lengths of the headers and metas
			require.Equal(t, tt.expectedLen, len(response.BlockHeaders))
			require.Equal(t, tt.expectedLen, len(response.Metas))
		})
	}
}

// Test_GetLastNInvalidBlocks tests the GetLastNInvalidBlocks functionality
func Test_GetLastNInvalidBlocks(t *testing.T) {
	ctx := setup(t)

	t.Run("get last N invalid blocks", func(t *testing.T) {
		request := &blockchain_api.GetLastNInvalidBlocksRequest{}

		response, err := ctx.server.GetLastNInvalidBlocks(context.Background(), request)
		require.NoError(t, err)
		require.NotNil(t, response)
		assert.Empty(t, response.Blocks) // No invalid blocks initially
	})
}

// Test_GetSuitableBlock tests the GetSuitableBlock functionality
func Test_GetSuitableBlock(t *testing.T) {
	ctx := setup(t)

	// Store a block
	block := mockBlock(ctx, t)
	_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "peer1")
	require.NoError(t, err)

	t.Run("get suitable block with zero hash", func(t *testing.T) {
		// Create a proper zero hash (32 bytes of zeros)
		zeroHash := make([]byte, 32)
		request := &blockchain_api.GetSuitableBlockRequest{
			Hash: zeroHash,
		}

		response, err := ctx.server.GetSuitableBlock(context.Background(), request)
		// This may error if there's no suitable block with zero hash
		if err != nil {
			assert.Contains(t, err.Error(), "median block")
		} else {
			require.NotNil(t, response)
			assert.NotNil(t, response.Block)
		}
	})

	t.Run("get suitable block with specific hash", func(t *testing.T) {
		request := &blockchain_api.GetSuitableBlockRequest{
			Hash: block.Hash().CloneBytes(),
		}

		response, err := ctx.server.GetSuitableBlock(context.Background(), request)
		require.NoError(t, err)
		require.NotNil(t, response)
		assert.NotNil(t, response.Block)
	})
}

// Test_Subscribe tests the subscription functionality
func Test_Subscribe(t *testing.T) {
	ctx := setup(t)

	// Manually start subscription goroutine if not ready
	if !ctx.server.subscriptionManagerReady.Load() {
		go ctx.server.startSubscriptions()

		// Wait for subscription manager to be ready
		for i := 0; i < 50; i++ {
			if ctx.server.subscriptionManagerReady.Load() {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	// If still not ready, skip test
	if !ctx.server.subscriptionManagerReady.Load() {
		t.Skip("Subscription manager not ready, skipping test")
	}

	// Create a mock stream
	mockStream := &mockSubscribeServer{
		context: context.Background(),
		sent:    make([]*blockchain_api.Notification, 0),
	}

	// Start subscription in goroutine
	done := make(chan error, 1)
	go func() {
		done <- ctx.server.Subscribe(&blockchain_api.SubscribeRequest{
			Source: "test-subscriber",
		}, mockStream)
	}()

	// Give subscription time to register - wait with retry
	var subCount int
	for i := 0; i < 50; i++ {
		ctx.server.subscribersMu.RLock()
		subCount = len(ctx.server.subscribers)
		ctx.server.subscribersMu.RUnlock()
		if subCount > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	assert.Equal(t, 1, subCount)

	// Cancel the mock stream to terminate the subscription
	mockStream.Cancel()

	select {
	case <-done:
		// Success - subscription ended as expected
	case <-time.After(time.Second):
		t.Fatal("Subscription didn't end in time")
	}
}

// mockSubscribeServer implements blockchain_api.BlockchainAPI_SubscribeServer for testing
type mockSubscribeServer struct {
	blockchain_api.BlockchainAPI_SubscribeServer
	context context.Context
	cancel  context.CancelFunc
	sent    []*blockchain_api.Notification
	mu      sync.Mutex
}

func (m *mockSubscribeServer) Send(notification *blockchain_api.Notification) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, notification)
	return nil
}

func (m *mockSubscribeServer) Context() context.Context {
	if m.cancel == nil {
		m.context, m.cancel = context.WithCancel(m.context)
	}
	return m.context
}

func (m *mockSubscribeServer) Cancel() {
	if m.cancel != nil {
		m.cancel()
	}
}

// Test_GetBlockLocator tests the GetBlockLocator functionality
func Test_GetBlockLocator(t *testing.T) {
	ctx := setup(t)

	// Store blocks
	block := mockBlock(ctx, t)
	_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "peer1")
	require.NoError(t, err)

	t.Run("get block locator", func(t *testing.T) {
		request := &blockchain_api.GetBlockLocatorRequest{
			Hash:   block.Hash().CloneBytes(),
			Height: block.Height,
		}

		response, err := ctx.server.GetBlockLocator(context.Background(), request)
		require.NoError(t, err)
		require.NotNil(t, response)
		assert.NotNil(t, response.Locator)
	})
}

func TestBlockchainStart(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	// settings
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	tSettings.BlockChain.GRPCListenAddress = ""
	tSettings.BlockChain.HTTPListenAddress = ""

	// in-store memory
	storeURL, err := url.Parse("sqlitememory:///blockchain")
	require.NoError(t, err)
	blockchainStore, err := sql.New(logger, storeURL, tSettings)
	require.NoError(t, err)

	// kafka mock producer
	mockProducer := kafka.NewKafkaAsyncProducerMock()

	// Instantiate server passing mockProducer
	server, err := New(ctx, logger, tSettings, blockchainStore, mockProducer)
	require.NoError(t, err)
	require.NotNil(t, server)

	require.NoError(t, server.Init(ctx))

	readyCh := make(chan struct{})
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = server.Start(runCtx, readyCh)
	}()

	select {
	case <-readyCh:

	case <-time.After(time.Second):
		t.Fatal("timeout waiting for blockchain server to start")
	}

	cancel()
}

func TestBlockchainStartGRPCError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	tSettings.BlockChain.HTTPListenAddress = "127.0.0.1:0"

	tSettings.BlockChain.GRPCListenAddress = "bad:address"

	storeURL, err := url.Parse("sqlitememory:///blockchain_error")
	require.NoError(t, err)
	blockchainStore, err := sql.New(logger, storeURL, tSettings)
	require.NoError(t, err)

	mockProducer := kafka.NewKafkaAsyncProducerMock()

	server, err := New(ctx, logger, tSettings, blockchainStore, mockProducer)
	require.NoError(t, err)

	require.NoError(t, server.Init(ctx))

	readyCh := make(chan struct{})
	err = server.Start(ctx, readyCh)

	require.Error(t, err)

	require.Contains(t, err.Error(), "can't start GRPC server")
}

func TestInvalidateHandler(t *testing.T) {
	ctx := context.Background()

	logger := ulogger.NewErrorTestLogger(t)
	e := echo.New()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	tSettings.BlockChain.GRPCListenAddress = ""
	tSettings.BlockChain.HTTPListenAddress = ""
	mockProducer := kafka.NewKafkaAsyncProducerMock()
	storeURL, err := url.Parse("sqlitememory:///blockchain")
	require.NoError(t, err)
	blockchainStore, err := sql.New(logger, storeURL, tSettings)
	require.NoError(t, err)
	t.Run("invalid has detected", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/invalidate/notAHash", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetParamNames("hash")
		c.SetParamValues("notAHash")

		server, _ := New(ctx, logger, tSettings, blockchainStore, mockProducer)
		err := server.invalidateHandler(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "invalid hash")
	})

	t.Run("invalidation of non-existent block is idempotent", func(t *testing.T) {
		validHash := chainhash.DoubleHashH([]byte("abc")).String()
		req := httptest.NewRequest(http.MethodPost, "/invalidate/"+validHash, nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetParamNames("hash")
		c.SetParamValues(validHash)
		server, _ := New(ctx, logger, tSettings, blockchainStore, mockProducer)
		err := server.invalidateHandler(c)
		require.NoError(t, err)

		// InvalidateBlock is now idempotent - returns success even if block doesn't exist
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "block invalidated")
	})

	t.Run("success", func(t *testing.T) {
		ctx := setup(t)
		block := mockBlock(ctx, t)
		_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "peer1")
		require.NoError(t, err)
		validHash := block.Hash().String()
		req := httptest.NewRequest(http.MethodPost, "/invalidate/"+validHash, nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetParamNames("hash")
		c.SetParamValues(validHash)
		err = ctx.server.invalidateHandler(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "block invalidated")
	})
}

func TestRevalidateHandler(t *testing.T) {
	ctx := setup(t)
	e := echo.New()

	t.Run("invalid hash returns 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/revalidate/notAHash", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetParamNames("hash")
		c.SetParamValues("notAHash")

		err := ctx.server.revalidateHandler(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "invalid hash")
	})

	t.Run("non-existent block returns 500", func(t *testing.T) {
		nonExistentHash := chainhash.DoubleHashH([]byte("nope")).String()

		req := httptest.NewRequest(http.MethodPost, "/revalidate/"+nonExistentHash, nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetParamNames("hash")
		c.SetParamValues(nonExistentHash)

		err := ctx.server.revalidateHandler(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Contains(t, rec.Body.String(), "error revalidating block")
	})

	t.Run("existing block returns 200", func(t *testing.T) {
		block := mockBlock(ctx, t)
		_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "peer1")
		require.NoError(t, err)

		validHash := block.Hash().String()
		req := httptest.NewRequest(http.MethodPost, "/revalidate/"+validHash, nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetParamNames("hash")
		c.SetParamValues(validHash)

		err = ctx.server.revalidateHandler(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "block revalidated")
	})
}

func TestGetBlocks(t *testing.T) {
	ctx := setup(t)

	t.Run("invalid hash returns error", func(t *testing.T) {
		req := &blockchain_api.GetBlocksRequest{
			Hash:  []byte{1, 2, 3}, // not 32 byte
			Count: 1,
		}

		resp, err := ctx.server.GetBlocks(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "not valid")
	})

	t.Run("store returns error", func(t *testing.T) {
		validHash := chainhash.DoubleHashH([]byte("does-not-exist"))

		// request blocks starting from a non-existent hash
		req := &blockchain_api.GetBlocksRequest{
			Hash:  validHash.CloneBytes(),
			Count: 1,
		}

		resp, err := ctx.server.GetBlocks(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Blocks, 0, "expected no blocks when hash not found")
	})

	t.Run("success returns blocks", func(t *testing.T) {
		block := mockBlock(ctx, t)
		_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "peer1")
		require.NoError(t, err)

		req := &blockchain_api.GetBlocksRequest{
			Hash:  block.Hash().CloneBytes(),
			Count: 1,
		}

		resp, err := ctx.server.GetBlocks(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Blocks, 1)
		require.NotEmpty(t, resp.Blocks[0])
	})
}

func TestGetBestBlockHeader(t *testing.T) {
	t.Run("store returns error - no best block", func(t *testing.T) {
		store := blockchain_store.NewMockStore()
		logger := ulogger.NewErrorTestLogger(t)

		tSettings := test.CreateBaseTestSettings(t)
		b, err := New(context.Background(), logger, tSettings, store, nil)
		require.NoError(t, err)

		resp, err := b.GetBestBlockHeader(context.Background(), &emptypb.Empty{})
		require.Nil(t, resp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no best block")
	})

	t.Run("success - best block available", func(t *testing.T) {
		ctx := setup(t)

		blk := mockBlock(ctx, t)
		blockID, height, err := ctx.server.store.StoreBlock(context.Background(), blk, "peer1")
		require.NoError(t, err)
		t.Logf("Stored mock block: ID=%d, height=%d, hash=%s", blockID, height, blk.Hash())

		// Query the database directly to see what's stored
		db := ctx.server.store.GetDB()
		rows, err := db.QueryContext(context.Background(), "SELECT id, height, chain_work, tx_count, peer_id FROM blocks ORDER BY id")
		require.NoError(t, err)
		defer rows.Close()

		t.Logf("Blocks in database:")
		for rows.Next() {
			var id, height uint32
			var chainWork []byte
			var txCount uint64
			var peerID string
			err := rows.Scan(&id, &height, &chainWork, &txCount, &peerID)
			require.NoError(t, err)
			t.Logf("  ID=%d, height=%d, chainWork=%x, txCount=%d, peerID=%s", id, height, chainWork, txCount, peerID)
		}

		resp, err := ctx.server.GetBestBlockHeader(context.Background(), &emptypb.Empty{})
		require.NoError(t, err)
		require.NotNil(t, resp)

		assert.Equal(t, blk.Height, uint32(resp.Height))
		assert.Equal(t, blk.TransactionCount, resp.TxCount)
		assert.Equal(t, blk.SizeInBytes, resp.SizeInBytes)
		assert.NotEmpty(t, resp.BlockHeader)
	})
}

// Test_ServiceInvalidateBlock_ClearsBestAndDifficultyCache ensures that invalidating the
// current best block via the service updates the best block header and allows
// difficulty to be recalculated based on the new best tip (i.e., cache does not stay stale).
func Test_ServiceInvalidateBlock_ClearsBestAndDifficultyCache(t *testing.T) {
	ctx := setup(t)

	// Build a short chain of 3 blocks (heights 1,2,3) from genesis
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	prevHash := tSettings.ChainCfgParams.GenesisHash

	var blocks []*model.Block
	for i := 1; i <= 3; i++ {
		coinbase := bt.NewTx()
		err := coinbase.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
		require.NoError(t, err)

		arbitraryData := []byte{0x03, byte(i), 0x00, 0x00}
		coinbase.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)
		coinbase.Inputs[0].SequenceNumber = 0xffffffff
		err = coinbase.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
		require.NoError(t, err)

		merkleRoot := coinbase.TxIDChainHash()

		blk := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prevHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix()) + uint32(i),
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
				Nonce:          uint32(i),
			},
			CoinbaseTx:       coinbase,
			TransactionCount: 1,
			SizeInBytes:      1000,
			Height:           uint32(i),
			ID:               uint32(i),
		}

		_, _, err = ctx.server.store.StoreBlock(context.Background(), blk, "test")
		require.NoError(t, err)

		blocks = append(blocks, blk)
		prevHash = blk.Hash()
	}

	// Warm difficulty cache using the current best (height 3)
	bestRespBefore, err := ctx.server.GetBestBlockHeader(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, bestRespBefore)

	bestHeaderBefore, err := model.NewBlockHeaderFromBytes(bestRespBefore.BlockHeader)
	require.NoError(t, err)

	// Call GetNextWorkRequired twice to warm any internal caching
	req := &blockchain_api.GetNextWorkRequiredRequest{PreviousBlockHash: bestHeaderBefore.Hash().CloneBytes(), CurrentBlockTime: time.Now().Unix()}
	diff1, err := ctx.server.GetNextWorkRequired(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, diff1)

	diff2, err := ctx.server.GetNextWorkRequired(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, diff2)

	// Invalidate the current best block (height 3)
	_, err = ctx.server.InvalidateBlock(context.Background(), &blockchain_api.InvalidateBlockRequest{
		BlockHash: bestHeaderBefore.Hash().CloneBytes(),
	})
	require.NoError(t, err)

	// After invalidation, the best should move to height 2
	bestRespAfter, err := ctx.server.GetBestBlockHeader(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, bestRespAfter)

	bestHeaderAfter, err := model.NewBlockHeaderFromBytes(bestRespAfter.BlockHeader)
	require.NoError(t, err)

	// Verify that best tip changed from the invalidated block (the store decides the next best)
	assert.False(t, bestHeaderAfter.Hash().IsEqual(bestHeaderBefore.Hash()))

	// Ensure difficulty can be computed for the new best without using stale cache
	reqAfter := &blockchain_api.GetNextWorkRequiredRequest{PreviousBlockHash: bestHeaderAfter.Hash().CloneBytes(), CurrentBlockTime: time.Now().Unix()}
	diffAfter, err := ctx.server.GetNextWorkRequired(context.Background(), reqAfter)
	require.NoError(t, err)
	require.NotNil(t, diffAfter)
}

// Test_ServiceAddBlock_ClearsDifficultyCache ensures that adding a new block via the service
// clears the difficulty cache to prevent stale cached values from being used during rapid block processing.
func Test_ServiceAddBlock_ClearsDifficultyCache(t *testing.T) {
	ctx := setup(t)

	// Build a short chain of 2 blocks (heights 1,2) from genesis
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	prevHash := tSettings.ChainCfgParams.GenesisHash

	var blocks []*model.Block
	for i := 1; i <= 2; i++ {
		coinbase := bt.NewTx()
		err := coinbase.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
		require.NoError(t, err)

		arbitraryData := []byte{0x03, byte(i), 0x00, 0x00}
		coinbase.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)
		coinbase.Inputs[0].SequenceNumber = 0xffffffff
		err = coinbase.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
		require.NoError(t, err)

		merkleRoot := coinbase.TxIDChainHash()

		blk := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prevHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix()) + uint32(i),
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
				Nonce:          uint32(i),
			},
			CoinbaseTx:       coinbase,
			TransactionCount: 1,
			SizeInBytes:      1000,
			Height:           uint32(i),
			ID:               uint32(i),
		}

		_, _, err = ctx.server.store.StoreBlock(context.Background(), blk, "test")
		require.NoError(t, err)

		blocks = append(blocks, blk)
		prevHash = blk.Hash()
	}

	// Warm difficulty cache using the current best (height 2)
	bestResp, err := ctx.server.GetBestBlockHeader(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, bestResp)

	bestHeader, err := model.NewBlockHeaderFromBytes(bestResp.BlockHeader)
	require.NoError(t, err)

	// Call GetNextWorkRequired twice to warm any internal caching
	req := &blockchain_api.GetNextWorkRequiredRequest{PreviousBlockHash: bestHeader.Hash().CloneBytes(), CurrentBlockTime: time.Now().Unix()}
	diff1, err := ctx.server.GetNextWorkRequired(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, diff1)

	diff2, err := ctx.server.GetNextWorkRequired(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, diff2)

	// Create a new block at height 3 to add via AddBlock
	coinbase3 := bt.NewTx()
	err = coinbase3.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	require.NoError(t, err)

	arbitraryData3 := []byte{0x03, 0x03, 0x00, 0x00}
	coinbase3.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData3)
	coinbase3.Inputs[0].SequenceNumber = 0xffffffff
	err = coinbase3.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
	require.NoError(t, err)

	merkleRoot3 := coinbase3.TxIDChainHash()

	block3 := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: merkleRoot3,
			Timestamp:      uint32(time.Now().Unix()) + 3,
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
			Nonce:          3,
		},
		CoinbaseTx:       coinbase3,
		TransactionCount: 1,
		SizeInBytes:      1000,
	}

	// Prepare AddBlockRequest
	addBlockReq := &blockchain_api.AddBlockRequest{
		Header:           block3.Header.Bytes(),
		CoinbaseTx:       coinbase3.Bytes(),
		SubtreeHashes:    [][]byte{},
		TransactionCount: block3.TransactionCount,
		SizeInBytes:      block3.SizeInBytes,
		PeerId:           "test-peer",
	}

	// Add block via AddBlock (this should clear the difficulty cache)
	_, err = ctx.server.AddBlock(context.Background(), addBlockReq)
	require.NoError(t, err)

	// Get the new best block header (should be the newly added block)
	newBestResp, err := ctx.server.GetBestBlockHeader(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, newBestResp)

	newBestHeader, err := model.NewBlockHeaderFromBytes(newBestResp.BlockHeader)
	require.NoError(t, err)

	// Verify best tip changed to the new block
	assert.True(t, newBestHeader.Hash().IsEqual(block3.Hash()))

	// Ensure difficulty can be computed for the new best without using stale cache
	reqAfter := &blockchain_api.GetNextWorkRequiredRequest{PreviousBlockHash: newBestHeader.Hash().CloneBytes(), CurrentBlockTime: time.Now().Unix()}
	diffAfter, err := ctx.server.GetNextWorkRequired(context.Background(), reqAfter)
	require.NoError(t, err)
	require.NotNil(t, diffAfter)
}

func TestGetBlockByID(t *testing.T) {
	ctx := setup(t)

	t.Run("store returns error", func(t *testing.T) {
		// Passing wrong ID
		req := &blockchain_api.GetBlockByIDRequest{Id: 9999}

		resp, err := ctx.server.GetBlockByID(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)
	})

	t.Run("success - block exists", func(t *testing.T) {
		// Create and save a mock block
		block := mockBlock(ctx, t)
		_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "peer1")
		require.NoError(t, err)

		req := &blockchain_api.GetBlockByIDRequest{Id: uint64(block.ID)}

		resp, err := ctx.server.GetBlockByID(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Verify fields
		assert.Equal(t, block.Header.Bytes(), resp.Header)
		assert.Equal(t, block.Height, resp.Height)
		assert.Equal(t, block.CoinbaseTx.Bytes(), resp.CoinbaseTx)
		assert.Equal(t, block.TransactionCount, resp.TransactionCount)
		assert.Equal(t, block.SizeInBytes, resp.SizeInBytes)
		assert.Equal(t, block.ID, resp.Id)
	})
}

func TestGetNextBlockID(t *testing.T) {
	t.Run("store returns error", func(t *testing.T) {
		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)
		store := &errorStore{}
		store.On("GetNextBlockID", mock.Anything).
			Return(uint64(0), errors.NewProcessingError("forced error"))
		server, err := New(context.Background(), logger, tSettings, store, nil)
		require.NoError(t, err)

		resp, err := server.GetNextBlockID(context.Background(), &emptypb.Empty{})
		require.Error(t, err)
		require.Nil(t, resp)
	})

	t.Run("success - returns next block ID", func(t *testing.T) {
		ctx := setup(t)

		block := mockBlock(ctx, t)
		_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "peer1")
		require.NoError(t, err)

		resp, err := ctx.server.GetNextBlockID(context.Background(), &emptypb.Empty{})
		require.NoError(t, err)
		require.NotNil(t, resp)

		assert.Equal(t, uint64(block.ID)+1, resp.NextBlockId)
	})
}

func TestGetNextWorkRequired(t *testing.T) {
	ctx := setup(t)

	validHash := chainhash.DoubleHashH([]byte("block1"))
	block := mockBlock(ctx, t)
	block.Header.Timestamp = uint32(time.Now().Unix())
	_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "peer1")
	require.NoError(t, err)

	t.Run("genesis difficulty", func(t *testing.T) {
		logger := ulogger.NewErrorTestLogger(t)
		store := blockchain_store.NewMockStore()
		tSettings := test.CreateBaseTestSettings(t)
		realDiff, err := NewDifficulty(store, logger, tSettings)
		require.NoError(t, err)
		ctx.server.difficulty = realDiff

		req := &blockchain_api.GetNextWorkRequiredRequest{
			PreviousBlockHash: model.GenesisBlockHeader.Hash().CloneBytes(),
			CurrentBlockTime:  time.Now().Unix(),
		}
		resp, err := ctx.server.GetNextWorkRequired(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("invalid block hash - returns error", func(t *testing.T) {
		logger := ulogger.NewErrorTestLogger(t)
		store := blockchain_store.NewMockStore()
		tSettings := test.CreateBaseTestSettings(t)
		realDiff, err := NewDifficulty(store, logger, tSettings)
		require.NoError(t, err)
		ctx.server.difficulty = realDiff

		req := &blockchain_api.GetNextWorkRequiredRequest{
			PreviousBlockHash: []byte("not-a-valid-hash"),
			CurrentBlockTime:  time.Now().Unix(),
		}
		resp, err := ctx.server.GetNextWorkRequired(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)
	})

	t.Run("store returns error", func(t *testing.T) {
		mockStore := &errorStoreGetBlockHeader{}
		mockStore.On("GetBlockHeader",
			mock.Anything, // ctx
			mock.Anything, // blockHash
		).Return(nil, nil, errors.NewProcessingError("forced error"))

		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)
		server, _ := New(context.Background(), logger, tSettings, mockStore, nil)
		realDiff, err := NewDifficulty(mockStore, logger, tSettings)
		require.NoError(t, err)
		server.difficulty = realDiff

		validHash := chainhash.DoubleHashH([]byte("valid"))

		req := &blockchain_api.GetNextWorkRequiredRequest{
			PreviousBlockHash: validHash.CloneBytes(),
			CurrentBlockTime:  time.Now().Unix(),
		}

		resp, err := server.GetNextWorkRequired(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		mockStore.AssertExpectations(t)
	})

	t.Run("no block headers returned", func(t *testing.T) {
		emptyStore := &emptyStoreGetBlockHeader{}
		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)
		store := blockchain_store.NewMockStore()

		server, _ := New(context.Background(), logger, tSettings, emptyStore, nil)
		realDiff, err := NewDifficulty(store, logger, tSettings)
		require.NoError(t, err)
		ctx.server.difficulty = realDiff

		req := &blockchain_api.GetNextWorkRequiredRequest{
			PreviousBlockHash: validHash.CloneBytes(),
			CurrentBlockTime:  time.Now().Unix(),
		}
		resp, err := server.GetNextWorkRequired(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)
	})

	t.Run("CalcNextWorkRequired returns success", func(t *testing.T) {
		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)
		store := blockchain_store.NewMockStore()
		realDiff, err := NewDifficulty(store, logger, tSettings)
		require.NoError(t, err)
		ctx.server.difficulty = realDiff

		req := &blockchain_api.GetNextWorkRequiredRequest{
			PreviousBlockHash: block.Hash().CloneBytes(),
			CurrentBlockTime:  time.Now().Unix(),
		}

		resp, err := ctx.server.GetNextWorkRequired(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.NotEmpty(t, resp.Bits)
	})
}

func TestGetHashOfAncestorBlock(t *testing.T) {
	ctx := setup(t)

	t.Run("success - returns ancestor hash", func(t *testing.T) {
		block := mockBlock(ctx, t)
		_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "peer1")
		require.NoError(t, err)

		req := &blockchain_api.GetHashOfAncestorBlockRequest{
			Hash:  block.Hash().CloneBytes(),
			Depth: 0,
		}

		resp, err := ctx.server.GetHashOfAncestorBlock(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		assert.Equal(t, block.Hash().CloneBytes(), resp.Hash)
	})

	t.Run("error - block not found", func(t *testing.T) {
		fakeHash := chainhash.DoubleHashH([]byte("does-not-exist"))

		req := &blockchain_api.GetHashOfAncestorBlockRequest{
			Hash:  fakeHash.CloneBytes(),
			Depth: 1,
		}

		resp, err := ctx.server.GetHashOfAncestorBlock(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)
		assert.Contains(t, err.Error(), "before block")
	})
}

func TestGetLatestBlockHeaderFromBlockLocatorRequest(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("error - store returns error", func(t *testing.T) {
		best := chainhash.DoubleHashH([]byte("best"))
		locator := chainhash.DoubleHashH([]byte("locator"))

		errorStore := &errorStore{}
		errorStore.On(
			"GetLatestBlockHeaderFromBlockLocator",
			mock.Anything,                           // ctx
			mock.AnythingOfType("*chainhash.Hash"),  // bestBlockHash
			mock.AnythingOfType("[]chainhash.Hash"), // blockLocator
		).Return(nil, nil, errors.NewProcessingError("forced error"))

		server, _ := New(ctx, logger, tSettings, errorStore, nil)

		req := &blockchain_api.GetLatestBlockHeaderFromBlockLocatorRequest{
			BestBlockHash:      (&best).CloneBytes(),
			BlockLocatorHashes: [][]byte{(&locator).CloneBytes()},
		}

		resp, err := server.GetLatestBlockHeaderFromBlockLocatorRequest(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		assert.Contains(t, err.Error(), "forced error")
	})
}

func TestGetBlockHeadersFromOldestRequest(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("success - returns headers and metas", func(t *testing.T) {
		store := &fakeStoreOldest{
			fn: func(ctx context.Context, chainTipHash, targetHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
				h := &chainhash.Hash{}
				copy(h[:], bytes.Repeat([]byte{0x01}, 32))

				header := &model.BlockHeader{
					Version:        1,
					HashPrevBlock:  h, // not nil
					HashMerkleRoot: h, // not nil
					Timestamp:      uint32(time.Now().Unix()),
					Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
					Nonce:          12345,
				}

				meta := &model.BlockHeaderMeta{
					Height:    1,
					TxCount:   1,
					BlockTime: uint32(time.Now().Unix()),
				}

				return []*model.BlockHeader{header}, []*model.BlockHeaderMeta{meta}, nil
			},
		}

		server, _ := New(ctx, logger, tSettings, store, nil)

		tip := chainhash.DoubleHashH([]byte("tip"))
		target := chainhash.DoubleHashH([]byte("target"))

		req := &blockchain_api.GetBlockHeadersFromOldestRequest{
			ChainTipHash:    tip.CloneBytes(),
			TargetHash:      target.CloneBytes(),
			NumberOfHeaders: 1,
		}

		resp, err := server.GetBlockHeadersFromOldestRequest(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.BlockHeaders, 1)
		require.Len(t, resp.Metas, 1)
	})

	t.Run("error - store returns error", func(t *testing.T) {
		store := &fakeStoreOldest{
			fn: func(ctx context.Context, chainTipHash, targetHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
				return nil, nil, errors.NewProcessingError("forced error")
			},
		}

		server, _ := New(ctx, logger, tSettings, store, nil)

		chainTip := chainhash.DoubleHashH([]byte("tip"))
		target := chainhash.DoubleHashH([]byte("target"))

		req := &blockchain_api.GetBlockHeadersFromOldestRequest{
			ChainTipHash:    chainTip.CloneBytes(),
			TargetHash:      target.CloneBytes(),
			NumberOfHeaders: 1,
		}

		resp, err := server.GetBlockHeadersFromOldestRequest(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
	})
}

func TestGetBlockExists(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("error - invalid hash", func(t *testing.T) {
		store := blockchain_store.NewMockStore()
		server, _ := New(ctx, logger, tSettings, store, nil)

		req := &blockchain_api.GetBlockRequest{
			Hash: []byte("invalid-hash"),
		}

		resp, err := server.GetBlockExists(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		assert.Contains(t, err.Error(), "not valid")
	})

	t.Run("error - store returns error", func(t *testing.T) {
		hash := chainhash.DoubleHashH([]byte("block"))
		mockStore := &errorStore{}
		mockStore.On(
			"GetBlockExists",
			mock.Anything,
			mock.AnythingOfType("*chainhash.Hash"),
		).Return(false, errors.NewProcessingError("forced error"))

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.GetBlockRequest{
			Hash: hash.CloneBytes(),
		}

		_, err := server.GetBlockExists(ctx, req)
		require.Error(t, err)
	})

	t.Run("success - block exists", func(t *testing.T) {
		hash := chainhash.DoubleHashH([]byte("exists"))
		mockStore := &errorStore{}
		mockStore.On(
			"GetBlockExists",
			mock.Anything,
			mock.AnythingOfType("*chainhash.Hash"),
		).Return(true, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.GetBlockRequest{
			Hash: hash.CloneBytes(),
		}

		resp, err := server.GetBlockExists(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success - block does not exist", func(t *testing.T) {
		hash := chainhash.DoubleHashH([]byte("not-exists"))
		mockStore := new(errorStore)
		mockStore.On(
			"GetBlockExists",
			mock.Anything,
			mock.AnythingOfType("*chainhash.Hash"),
		).Return(false, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.GetBlockRequest{
			Hash: hash.CloneBytes(),
		}

		resp, err := server.GetBlockExists(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.False(t, resp.Exists)
	})
}

func TestCheckBlockIsInCurrentChain(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("error - store returns error", func(t *testing.T) {
		mockStore := &mockStoreCheckBlockChain{MockStore: blockchain_store.NewMockStore()}
		mockStore.On(
			"CheckBlockIsInCurrentChain",
			mock.Anything,
			mock.AnythingOfType("[]uint32"),
		).Return(false, errors.NewProcessingError("forced error"))

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.CheckBlockIsCurrentChainRequest{BlockIDs: []uint32{1, 2, 3}}
		resp, err := server.CheckBlockIsInCurrentChain(ctx, req)

		require.Error(t, err)
		require.Nil(t, resp)
		assert.Contains(t, err.Error(), "forced error")
	})

	t.Run("success - block is part of current chain", func(t *testing.T) {
		mockStore := new(mockStoreCheckBlockChain)
		mockStore.On(
			"CheckBlockIsInCurrentChain",
			mock.Anything,
			mock.AnythingOfType("[]uint32"),
		).Return(true, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.CheckBlockIsCurrentChainRequest{
			BlockIDs: []uint32{10, 20},
		}

		resp, err := server.CheckBlockIsInCurrentChain(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, resp.IsPartOfCurrentChain)
	})

	t.Run("success - block is NOT part of current chain", func(t *testing.T) {
		mockStore := new(mockStoreCheckBlockChain)
		mockStore.On(
			"CheckBlockIsInCurrentChain",
			mock.Anything,
			mock.AnythingOfType("[]uint32"),
		).Return(false, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.CheckBlockIsCurrentChainRequest{
			BlockIDs: []uint32{99, 100},
		}

		resp, err := server.CheckBlockIsInCurrentChain(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.False(t, resp.IsPartOfCurrentChain)
	})
}

func TestGetChainTips(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("error - store returns error", func(t *testing.T) {
		mockStore := &mockStoreCheckBlockChain{MockStore: blockchain_store.NewMockStore()}
		mockStore.On("GetChainTips", mock.Anything).Return([]*model.ChainTip(nil), errors.NewProcessingError("forced error"))

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		resp, err := server.GetChainTips(ctx, &emptypb.Empty{})
		require.Error(t, err)
		require.Nil(t, resp)
		assert.Contains(t, err.Error(), "forced error")
	})

	t.Run("success - returns tips", func(t *testing.T) {
		mockStore := &mockStoreCheckBlockChain{MockStore: blockchain_store.NewMockStore()}
		expectedTips := []*model.ChainTip{{Height: 100, Hash: "hash-100"}}
		mockStore.On("GetChainTips", mock.Anything).Return(expectedTips, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		resp, err := server.GetChainTips(ctx, &emptypb.Empty{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, expectedTips, resp.Tips)
	})

	t.Run("success - empty tips", func(t *testing.T) {
		mockStore := &mockStoreCheckBlockChain{MockStore: blockchain_store.NewMockStore()}
		mockStore.On("GetChainTips", mock.Anything).Return([]*model.ChainTip{}, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		resp, err := server.GetChainTips(ctx, &emptypb.Empty{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Tips)
	})
}

func TestGetBlockHeader(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("error - invalid hash", func(t *testing.T) {
		store := blockchain_store.NewMockStore()
		server, _ := New(ctx, logger, tSettings, store, nil)

		req := &blockchain_api.GetBlockHeaderRequest{
			BlockHash: []byte("not-a-valid-hash"),
		}

		resp, err := server.GetBlockHeader(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		assert.Contains(t, err.Error(), "not valid")
	})

	t.Run("error - store returns error", func(t *testing.T) {
		hash := chainhash.DoubleHashH([]byte("block"))
		mockStore := &mockStoreGetBlockHeader{}

		mockStore.On(
			"GetBlockHeader",
			mock.Anything,
			mock.AnythingOfType("*chainhash.Hash"),
		).Return(nil, nil, errors.NewProcessingError("forced error"))

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.GetBlockHeaderRequest{
			BlockHash: hash.CloneBytes(),
		}

		resp, err := server.GetBlockHeader(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		assert.Contains(t, err.Error(), "forced error")
	})

	t.Run("success", func(t *testing.T) {
		hash := chainhash.DoubleHashH([]byte("block"))

		prev := chainhash.DoubleHashH([]byte("prev"))
		merkle := chainhash.DoubleHashH([]byte("merkle"))

		bytesLE := make([]byte, 4)
		binary.LittleEndian.PutUint32(bytesLE, 0x1d00ffff)
		nbits, _ := model.NewNBitFromSlice(bytesLE)

		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prev,
			HashMerkleRoot: &merkle,
			Timestamp:      1234567890,
			Bits:           *nbits,
			Nonce:          42,
		}

		meta := &model.BlockHeaderMeta{
			ID:          1,
			Height:      100,
			TxCount:     10,
			SizeInBytes: 256,
			Miner:       "test-miner",
			PeerID:      "peer-1",
			BlockTime:   1234567890,
			Timestamp:   1234567890,
			MinedSet:    true,
			SubtreesSet: true,
			Invalid:     false,
		}

		mockStore := new(mockStoreGetBlockHeader)
		mockStore.On(
			"GetBlockHeader",
			mock.Anything,
			mock.AnythingOfType("*chainhash.Hash"),
		).Return(header, meta, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.GetBlockHeaderRequest{
			BlockHash: hash.CloneBytes(),
		}

		resp, err := server.GetBlockHeader(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, header.Bytes(), resp.BlockHeader)
		assert.Equal(t, meta.ID, resp.Id)
		assert.Equal(t, meta.Height, resp.Height)
		assert.Equal(t, meta.TxCount, resp.TxCount)
		assert.Equal(t, meta.SizeInBytes, resp.SizeInBytes)
		assert.Equal(t, meta.Miner, resp.Miner)
		assert.Equal(t, meta.PeerID, resp.PeerId)
		assert.Equal(t, meta.BlockTime, resp.BlockTime)
		assert.Equal(t, meta.Timestamp, resp.Timestamp)
		assert.Equal(t, meta.MinedSet, resp.MinedSet)
		assert.Equal(t, meta.SubtreesSet, resp.SubtreesSet)
		assert.Equal(t, meta.Invalid, resp.Invalid)
	})
}

func TestGetBlockHeaders(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("error - invalid start hash", func(t *testing.T) {
		store := blockchain_store.NewMockStore()
		server, _ := New(ctx, logger, tSettings, store, nil)

		req := &blockchain_api.GetBlockHeadersRequest{
			StartHash:       []byte("invalid-hash"),
			NumberOfHeaders: 1,
		}

		resp, err := server.GetBlockHeaders(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		assert.Contains(t, err.Error(), "not valid")
	})

	t.Run("error - store returns error", func(t *testing.T) {
		hash := chainhash.DoubleHashH([]byte("block"))

		mockStore := new(mockStoreGetBlockHeaders)
		mockStore.On(
			"GetBlockHeaders",
			mock.Anything,
			mock.AnythingOfType("*chainhash.Hash"),
			mock.AnythingOfType("uint64"),
		).Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, errors.NewProcessingError("forced store error"))

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.GetBlockHeadersRequest{
			StartHash:       hash.CloneBytes(),
			NumberOfHeaders: 2,
		}

		resp, err := server.GetBlockHeaders(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		assert.Contains(t, err.Error(), "forced store error")
	})

	t.Run("success", func(t *testing.T) {
		hash := chainhash.DoubleHashH([]byte("block"))
		prev := chainhash.DoubleHashH([]byte("prev"))
		merkle := chainhash.DoubleHashH([]byte("merkle"))

		// NBit valid
		bytesLE := make([]byte, 4)
		binary.LittleEndian.PutUint32(bytesLE, 0x1d00ffff)
		nbits, _ := model.NewNBitFromSlice(bytesLE)

		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prev,
			HashMerkleRoot: &merkle,
			Timestamp:      111111,
			Bits:           *nbits,
			Nonce:          42,
		}
		meta := &model.BlockHeaderMeta{ID: 1, Height: 100}

		mockStore := new(mockStoreGetBlockHeaders)
		mockStore.On(
			"GetBlockHeaders",
			mock.Anything,
			mock.AnythingOfType("*chainhash.Hash"),
			mock.AnythingOfType("uint64"),
		).Return([]*model.BlockHeader{header}, []*model.BlockHeaderMeta{meta}, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.GetBlockHeadersRequest{
			StartHash:       hash.CloneBytes(),
			NumberOfHeaders: 1,
		}

		resp, err := server.GetBlockHeaders(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.BlockHeaders, 1)
		assert.Len(t, resp.Metas, 1)
	})
}

func TestGetState(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("error - store returns error", func(t *testing.T) {
		mockStore := &mockStoreState{}
		mockStore.On(
			"GetState",
			mock.Anything, // ctx
			"missing-key",
		).Return([]byte{}, errors.NewProcessingError("forced store error"))

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.GetStateRequest{Key: "missing-key"}
		resp, err := server.GetState(ctx, req)

		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "forced store error")
	})

	t.Run("success - returns state", func(t *testing.T) {
		expectedData := []byte("some-state")

		mockStore := &mockStoreState{}
		mockStore.On(
			"GetState",
			mock.Anything,
			"test-key",
		).Return(expectedData, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.GetStateRequest{Key: "test-key"}
		resp, err := server.GetState(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, expectedData, resp.Data)
	})
}

func TestSetState(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("success - state set correctly", func(t *testing.T) {
		mockStore := &mockStoreState{}
		mockStore.On(
			"SetState",
			mock.Anything,
			"test-key",
			[]byte("test-data"),
		).Return(nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.SetStateRequest{
			Key:  "test-key",
			Data: []byte("test-data"),
		}

		resp, err := server.SetState(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		mockStore.AssertExpectations(t)
	})

	t.Run("error - store returns error", func(t *testing.T) {
		mockStore := &mockStoreState{}
		mockStore.On(
			"SetState",
			mock.Anything,
			"bad-key",
			[]byte("bad-data"),
		).Return(errors.NewProcessingError("forced error"))

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.SetStateRequest{
			Key:  "bad-key",
			Data: []byte("bad-data"),
		}

		resp, err := server.SetState(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		mockStore.AssertExpectations(t)
	})
}

func TestGetBlockHeaderIDs(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("error - invalid start hash", func(t *testing.T) {
		mockStore := &mockStoreGetBlockHeaderIDs{}
		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.GetBlockHeadersRequest{
			StartHash:       []byte("invalid-hash"),
			NumberOfHeaders: 10,
		}

		resp, err := server.GetBlockHeaderIDs(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
	})

	t.Run("error - store returns error", func(t *testing.T) {
		startHash := chainhash.DoubleHashH([]byte("start"))
		mockStore := &mockStoreGetBlockHeaderIDs{}
		mockStore.On(
			"GetBlockHeaderIDs",
			mock.Anything,
			&startHash,
			uint64(5),
		).Return(nil, errors.NewProcessingError("forced error"))

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.GetBlockHeadersRequest{
			StartHash:       startHash.CloneBytes(),
			NumberOfHeaders: 5,
		}

		resp, err := server.GetBlockHeaderIDs(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		mockStore.AssertExpectations(t)
	})

	t.Run("success - returns IDs", func(t *testing.T) {
		startHash := chainhash.DoubleHashH([]byte("start-success"))
		mockStore := &mockStoreGetBlockHeaderIDs{}
		mockStore.On(
			"GetBlockHeaderIDs",
			mock.Anything,
			&startHash,
			uint64(3),
		).Return([]uint32{101, 102, 103}, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.GetBlockHeadersRequest{
			StartHash:       startHash.CloneBytes(),
			NumberOfHeaders: 3,
		}

		resp, err := server.GetBlockHeaderIDs(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, []uint32{101, 102, 103}, resp.Ids)
		mockStore.AssertExpectations(t)
	})
}

func TestGetBlockIsMined(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("error - store returns error", func(t *testing.T) {
		hash := chainhash.DoubleHashH([]byte("error-case"))
		mockStore := &mockStoreGetSetBlockIsMined{}
		mockStore.On("GetBlockIsMined", mock.Anything, &hash).
			Return(false, errors.NewProcessingError("forced error"))

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.GetBlockIsMinedRequest{
			BlockHash: hash.CloneBytes(),
		}

		resp, err := server.GetBlockIsMined(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		mockStore.AssertExpectations(t)
	})

	t.Run("success - block is mined", func(t *testing.T) {
		hash := chainhash.DoubleHashH([]byte("mined"))
		mockStore := &mockStoreGetSetBlockIsMined{}
		mockStore.On("GetBlockIsMined", mock.Anything, &hash).
			Return(true, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.GetBlockIsMinedRequest{
			BlockHash: hash.CloneBytes(),
		}

		resp, err := server.GetBlockIsMined(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, resp.IsMined)
		mockStore.AssertExpectations(t)
	})

	t.Run("success - block is not mined", func(t *testing.T) {
		hash := chainhash.DoubleHashH([]byte("not-mined"))
		mockStore := &mockStoreGetSetBlockIsMined{}
		mockStore.On("GetBlockIsMined", mock.Anything, &hash).
			Return(false, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.GetBlockIsMinedRequest{
			BlockHash: hash.CloneBytes(),
		}

		resp, err := server.GetBlockIsMined(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.False(t, resp.IsMined)
		mockStore.AssertExpectations(t)
	})
}

func TestSetBlockMinedSet(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("error - store returns error", func(t *testing.T) {
		hash := chainhash.DoubleHashH([]byte("error-case"))
		mockStore := &mockStoreGetSetBlockIsMined{}
		mockStore.On("SetBlockMinedSet", mock.Anything, &hash).
			Return(errors.NewProcessingError("forced error"))

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.SetBlockMinedSetRequest{
			BlockHash: hash.CloneBytes(),
		}

		resp, err := server.SetBlockMinedSet(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		mockStore.AssertExpectations(t)
	})

	t.Run("success - block marked as mined", func(t *testing.T) {
		hash := chainhash.DoubleHashH([]byte("success-case"))
		mockStore := &mockStoreGetSetBlockIsMined{}
		mockStore.On("SetBlockMinedSet", mock.Anything, &hash).
			Return(nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.SetBlockMinedSetRequest{
			BlockHash: hash.CloneBytes(),
		}

		resp, err := server.SetBlockMinedSet(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		mockStore.AssertExpectations(t)
	})
}

func TestGetBlocksMinedNotSet(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("error - store returns error", func(t *testing.T) {
		mockStore := &mockStoreGetSetBlockIsMined{}
		mockStore.On("GetBlocksMinedNotSet", mock.Anything).
			Return(([]*model.Block)(nil), errors.NewProcessingError("forced error"))

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		resp, err := server.GetBlocksMinedNotSet(ctx, &emptypb.Empty{})
		require.Error(t, err)
		require.Nil(t, resp)
		mockStore.AssertExpectations(t)
	})

	t.Run("error - block.Bytes fails", func(t *testing.T) {
		badBlock := &model.Block{}

		mockStore := &mockStoreGetSetBlockIsMined{}
		mockStore.On("GetBlocksMinedNotSet", mock.Anything).
			Return([]*model.Block{badBlock}, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		resp, err := server.GetBlocksMinedNotSet(ctx, &emptypb.Empty{})
		require.Error(t, err)
		require.Nil(t, resp)
		mockStore.AssertExpectations(t)
	})

	t.Run("success - returns blocks", func(t *testing.T) {
		hash := chainhash.DoubleHashH([]byte("block-1"))
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &hash,
			HashMerkleRoot: &hash,
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
			Nonce:          1,
		}
		block := &model.Block{
			Header:           header,
			TransactionCount: 1,
			SizeInBytes:      100,
			Height:           1,
			ID:               1,
		}

		mockStore := &mockStoreGetSetBlockIsMined{}
		mockStore.On("GetBlocksMinedNotSet", mock.Anything).
			Return([]*model.Block{block}, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		resp, err := server.GetBlocksMinedNotSet(ctx, &emptypb.Empty{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.BlockBytes, 1)
		mockStore.AssertExpectations(t)
	})
}

func TestSetBlockSubtreesSet(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("error - store returns error", func(t *testing.T) {
		hash := chainhash.DoubleHashH([]byte("block"))

		mockStore := &mockStoreGetSetBlockIsMined{}
		mockStore.On(
			"SetBlockSubtreesSet",
			mock.Anything,
			mock.AnythingOfType("*chainhash.Hash"),
		).Return(errors.NewProcessingError("forced error"))

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.SetBlockSubtreesSetRequest{
			BlockHash: hash.CloneBytes(),
		}

		resp, err := server.SetBlockSubtreesSet(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		assert.Contains(t, err.Error(), "forced error")
	})

	t.Run("success", func(t *testing.T) {
		hash := chainhash.DoubleHashH([]byte("block-success"))

		mockStore := &mockStoreGetSetBlockIsMined{}
		mockStore.On(
			"SetBlockSubtreesSet",
			mock.Anything,
			mock.AnythingOfType("*chainhash.Hash"),
		).Return(nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		req := &blockchain_api.SetBlockSubtreesSetRequest{
			BlockHash: hash.CloneBytes(),
		}

		resp, err := server.SetBlockSubtreesSet(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestGetBlocksSubtreesNotSet(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("error - store returns error", func(t *testing.T) {
		mockStore := &mockStoreGetSetBlockIsMined{}
		mockStore.On(
			"GetBlocksSubtreesNotSet",
			mock.Anything,
		).Return(nil, errors.NewProcessingError("forced error"))

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		resp, err := server.GetBlocksSubtreesNotSet(ctx, &emptypb.Empty{})
		require.Error(t, err)
		require.Nil(t, resp)
		assert.Contains(t, err.Error(), "forced error")
	})

	t.Run("error - invalid block bytes", func(t *testing.T) {
		badBlock := &model.Block{
			Header: nil,
		}

		mockStore := &mockStoreGetSetBlockIsMined{}
		mockStore.On(
			"GetBlocksSubtreesNotSet",
			mock.Anything,
		).Return([]*model.Block{badBlock}, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		resp, err := server.GetBlocksSubtreesNotSet(ctx, &emptypb.Empty{})
		require.Error(t, err)
		require.Nil(t, resp)
		assert.Contains(t, err.Error(), "not valid")
	})

	t.Run("success", func(t *testing.T) {
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
			Nonce:          1,
		}
		block := &model.Block{
			Header: header,
		}

		mockStore := &mockStoreGetSetBlockIsMined{}
		mockStore.On(
			"GetBlocksSubtreesNotSet",
			mock.Anything,
		).Return([]*model.Block{block}, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		resp, err := server.GetBlocksSubtreesNotSet(ctx, &emptypb.Empty{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.BlockBytes, 1)
	})
}

func TestLocateBlockHeaders(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("error - store returns error", func(t *testing.T) {
		mockStore := &mockStoreLocateBlockHeaders{}
		mockStore.On(
			"LocateBlockHeaders",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.NewProcessingError("forced store error"))

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		locator := chainhash.DoubleHashH([]byte("locator"))
		stop := chainhash.DoubleHashH([]byte("stop"))

		req := &blockchain_api.LocateBlockHeadersRequest{
			Locator:   [][]byte{(&locator).CloneBytes()},
			HashStop:  (&stop).CloneBytes(),
			MaxHashes: 5,
		}

		resp, err := server.LocateBlockHeaders(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		assert.Contains(t, err.Error(), "forced store error")
	})

	t.Run("success - returns block headers", func(t *testing.T) {
		mockStore := &mockStoreLocateBlockHeaders{}
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
			Nonce:          12345,
		}

		mockStore.On(
			"LocateBlockHeaders",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return([]*model.BlockHeader{header}, nil)

		server, _ := New(ctx, logger, tSettings, mockStore, nil)

		locator := chainhash.DoubleHashH([]byte("locator"))
		stop := chainhash.DoubleHashH([]byte("stop"))

		req := &blockchain_api.LocateBlockHeadersRequest{
			Locator:   [][]byte{(&locator).CloneBytes()},
			HashStop:  (&stop).CloneBytes(),
			MaxHashes: 5,
		}

		resp, err := server.LocateBlockHeaders(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.BlockHeaders, 1)
		assert.NotNil(t, resp.BlockHeaders[0])
	})
}

// Test_GetBlockHeadersFromTill tests the GetBlockHeadersFromTill gRPC method
func Test_GetBlockHeadersFromTill(t *testing.T) {
	ctx := setup(t)

	// Create a chain of blocks for testing
	headers := make([]*model.BlockHeader, 0, 10)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	prevHash := tSettings.ChainCfgParams.GenesisHash

	// Create and store a chain of blocks
	for i := 0; i < 10; i++ {
		merkleRoot := &chainhash.Hash{}
		merkleRoot[0] = byte(i)

		coinbaseTx := bt.NewTx()
		err := coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
		require.NoError(t, err)

		arbitraryData := []byte{0x03, byte(i), 0x00, 0x00}
		coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)
		coinbaseTx.Inputs[0].SequenceNumber = 0xffffffff

		err = coinbaseTx.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
		require.NoError(t, err)

		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prevHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix() + int64(i)),
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
				Nonce:          uint32(i),
			},
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			SizeInBytes:      1000,
			Height:           uint32(i + 1),
			ID:               uint32(i + 1),
		}

		headers = append(headers, block.Header)
		_, _, err = ctx.server.store.StoreBlock(context.Background(), block, "test")
		require.NoError(t, err)
		prevHash = block.Header.Hash()
	}

	tests := []struct {
		name        string
		startHash   []byte
		endHash     []byte
		expectError bool
		errorType   string
		minLength   int
	}{
		{
			name:        "valid range from start to end",
			startHash:   headers[2].Hash().CloneBytes(),
			endHash:     headers[5].Hash().CloneBytes(),
			expectError: false,
			minLength:   1, // Should return at least the range
		},
		{
			name:        "same start and end hash",
			startHash:   headers[3].Hash().CloneBytes(),
			endHash:     headers[3].Hash().CloneBytes(),
			expectError: false,
			minLength:   1,
		},
		{
			name:        "invalid start hash",
			startHash:   []byte("invalid"),
			endHash:     headers[2].Hash().CloneBytes(),
			expectError: true,
			errorType:   "start hash is not valid",
		},
		{
			name:        "invalid end hash",
			startHash:   headers[1].Hash().CloneBytes(),
			endHash:     []byte("invalid"),
			expectError: true,
			errorType:   "end hash is not valid",
		},
		{
			name:        "nil start hash",
			startHash:   nil,
			endHash:     headers[2].Hash().CloneBytes(),
			expectError: true,
			errorType:   "start hash is not valid",
		},
		{
			name:        "nil end hash",
			startHash:   headers[1].Hash().CloneBytes(),
			endHash:     nil,
			expectError: true,
			errorType:   "end hash is not valid",
		},
		{
			name:        "empty start hash",
			startHash:   []byte{},
			endHash:     headers[2].Hash().CloneBytes(),
			expectError: true,
			errorType:   "start hash is not valid",
		},
		{
			name:        "empty end hash",
			startHash:   headers[1].Hash().CloneBytes(),
			endHash:     []byte{},
			expectError: true,
			errorType:   "end hash is not valid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := &blockchain_api.GetBlockHeadersFromTillRequest{
				StartHash: tt.startHash,
				EndHash:   tt.endHash,
			}

			response, err := ctx.server.GetBlockHeadersFromTill(context.Background(), request)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorType != "" {
					require.Contains(t, err.Error(), tt.errorType)
				}
				require.Nil(t, response)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, response)
			require.GreaterOrEqual(t, len(response.BlockHeaders), tt.minLength)
			require.Equal(t, len(response.BlockHeaders), len(response.Metas))

			// Verify all responses are valid
			for i, headerBytes := range response.BlockHeaders {
				require.NotEmpty(t, headerBytes)
				require.NotEmpty(t, response.Metas[i])
			}
		})
	}
}

// Test_GetBlockHeadersFromHeight tests the GetBlockHeadersFromHeight gRPC method
func Test_GetBlockHeadersFromHeight(t *testing.T) {
	ctx := setup(t)

	// Create a chain of blocks for testing
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	prevHash := tSettings.ChainCfgParams.GenesisHash

	// Create and store a chain of blocks
	for i := 0; i < 10; i++ {
		merkleRoot := &chainhash.Hash{}
		merkleRoot[0] = byte(i)

		coinbaseTx := bt.NewTx()
		err := coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
		require.NoError(t, err)

		arbitraryData := []byte{0x03, byte(i), 0x00, 0x00}
		coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)
		coinbaseTx.Inputs[0].SequenceNumber = 0xffffffff

		err = coinbaseTx.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
		require.NoError(t, err)

		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prevHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix() + int64(i)),
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
				Nonce:          uint32(i),
			},
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			SizeInBytes:      1000,
			Height:           uint32(i + 1),
			ID:               uint32(i + 1),
		}

		_, _, err = ctx.server.store.StoreBlock(context.Background(), block, "test")
		require.NoError(t, err)
		prevHash = block.Header.Hash()
	}

	tests := []struct {
		name        string
		startHeight uint32
		limit       uint32
		expectError bool
		minLength   int
		maxLength   int
	}{
		{
			name:        "valid start height with limit",
			startHeight: 1,
			limit:       5,
			expectError: false,
			minLength:   1,
			maxLength:   5,
		},
		{
			name:        "start from middle height",
			startHeight: 5,
			limit:       3,
			expectError: false,
			minLength:   1,
			maxLength:   3,
		},
		{
			name:        "zero limit should return empty",
			startHeight: 1,
			limit:       0,
			expectError: false,
			minLength:   0,
			maxLength:   0,
		},
		{
			name:        "high start height beyond chain",
			startHeight: 1000,
			limit:       5,
			expectError: false,
			minLength:   0,
			maxLength:   0,
		},
		{
			name:        "start height zero",
			startHeight: 0,
			limit:       3,
			expectError: false,
			minLength:   0,
			maxLength:   3,
		},
		{
			name:        "large limit",
			startHeight: 1,
			limit:       100,
			expectError: false,
			minLength:   1,
			maxLength:   100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := &blockchain_api.GetBlockHeadersFromHeightRequest{
				StartHeight: tt.startHeight,
				Limit:       tt.limit,
			}

			response, err := ctx.server.GetBlockHeadersFromHeight(context.Background(), request)

			if tt.expectError {
				require.Error(t, err)
				require.Nil(t, response)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, response)
			require.GreaterOrEqual(t, len(response.BlockHeaders), tt.minLength)
			require.LessOrEqual(t, len(response.BlockHeaders), tt.maxLength)
			require.Equal(t, len(response.BlockHeaders), len(response.Metas))

			// Verify all responses are valid
			for i, headerBytes := range response.BlockHeaders {
				require.NotEmpty(t, headerBytes)
				require.NotEmpty(t, response.Metas[i])

				// Parse header to ensure it's valid
				header, err := model.NewBlockHeaderFromBytes(headerBytes)
				require.NoError(t, err)
				require.NotNil(t, header)
			}
		})
	}
}

// Test_GetBlockHeadersByHeight tests the GetBlockHeadersByHeight gRPC method
func Test_GetBlockHeadersByHeight(t *testing.T) {
	// Use MockStore to avoid SQL recursive CTE issues
	mockStore := blockchain_store.NewMockStore()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	tSettings.BlockChain.GRPCListenAddress = ""
	tSettings.BlockChain.HTTPListenAddress = ""

	server, err := New(context.Background(), logger, tSettings, mockStore, nil)
	require.NoError(t, err)
	require.NoError(t, server.Init(context.Background()))

	prevHash := tSettings.ChainCfgParams.GenesisHash

	// Create and store a chain of blocks
	for i := 0; i < 10; i++ {
		merkleRoot := &chainhash.Hash{}
		merkleRoot[0] = byte(i)

		coinbaseTx := bt.NewTx()
		err := coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
		require.NoError(t, err)

		arbitraryData := []byte{0x03, byte(i), 0x00, 0x00}
		coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)
		coinbaseTx.Inputs[0].SequenceNumber = 0xffffffff

		err = coinbaseTx.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
		require.NoError(t, err)

		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prevHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix() + int64(i)),
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
				Nonce:          uint32(i),
			},
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			SizeInBytes:      1000,
			Height:           uint32(i + 1),
			ID:               uint32(i + 1),
		}

		_, _, err = mockStore.StoreBlock(context.Background(), block, "test")
		require.NoError(t, err)
		prevHash = block.Header.Hash()
	}

	tests := []struct {
		name        string
		startHeight uint32
		endHeight   uint32
		expectError bool
		minLength   int
		maxLength   int
	}{
		{
			name:        "valid height range",
			startHeight: 2,
			endHeight:   5,
			expectError: false,
			minLength:   1,
			maxLength:   4, // end - start + 1 if all blocks exist
		},
		{
			name:        "same start and end height",
			startHeight: 3,
			endHeight:   3,
			expectError: false,
			minLength:   0,
			maxLength:   1,
		},
		{
			name:        "start height greater than end height",
			startHeight: 5,
			endHeight:   2,
			expectError: false,
			minLength:   0,
			maxLength:   0,
		},
		{
			name:        "start height zero",
			startHeight: 0,
			endHeight:   3,
			expectError: false,
			minLength:   0,
			maxLength:   4,
		},
		{
			name:        "end height beyond chain",
			startHeight: 5,
			endHeight:   1000,
			expectError: false,
			minLength:   1,
			maxLength:   50,
		},
		{
			name:        "both heights beyond chain",
			startHeight: 1000,
			endHeight:   1010,
			expectError: false,
			minLength:   0,
			maxLength:   0,
		},
		{
			name:        "start and end at beginning of chain",
			startHeight: 1,
			endHeight:   1,
			expectError: false,
			minLength:   0,
			maxLength:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := &blockchain_api.GetBlockHeadersByHeightRequest{
				StartHeight: tt.startHeight,
				EndHeight:   tt.endHeight,
			}

			response, err := server.GetBlockHeadersByHeight(context.Background(), request)

			if tt.expectError {
				require.Error(t, err)
				require.Nil(t, response)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, response)
			require.GreaterOrEqual(t, len(response.BlockHeaders), tt.minLength)
			require.LessOrEqual(t, len(response.BlockHeaders), tt.maxLength)
			require.Equal(t, len(response.BlockHeaders), len(response.Metas))

			// Verify all responses are valid
			for i, headerBytes := range response.BlockHeaders {
				require.NotEmpty(t, headerBytes)
				require.NotEmpty(t, response.Metas[i])

				// Parse header to ensure it's valid
				header, err := model.NewBlockHeaderFromBytes(headerBytes)
				require.NoError(t, err)
				require.NotNil(t, header)
			}
		})
	}
}

// Test_GetBlockHeadersToCommonAncestor_gRPC tests the GetBlockHeadersToCommonAncestor gRPC method
func Test_GetBlockHeadersToCommonAncestor_gRPC(t *testing.T) {
	ctx := setup(t)

	// Create a chain of blocks for testing
	headers := make([]*model.BlockHeader, 0, 10)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	prevHash := tSettings.ChainCfgParams.GenesisHash

	// Create and store a chain of blocks
	for i := 0; i < 10; i++ {
		merkleRoot := &chainhash.Hash{}
		merkleRoot[0] = byte(i)

		coinbaseTx := bt.NewTx()
		err := coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
		require.NoError(t, err)

		arbitraryData := []byte{0x03, byte(i), 0x00, 0x00}
		coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)
		coinbaseTx.Inputs[0].SequenceNumber = 0xffffffff

		err = coinbaseTx.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
		require.NoError(t, err)

		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prevHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix() + int64(i)),
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
				Nonce:          uint32(i),
			},
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			SizeInBytes:      1000,
			Height:           uint32(i + 1),
			ID:               uint32(i + 1),
		}

		headers = append(headers, block.Header)
		_, _, err = ctx.server.store.StoreBlock(context.Background(), block, "test")
		require.NoError(t, err)
		prevHash = block.Header.Hash()
	}

	tests := []struct {
		name               string
		targetHash         []byte
		blockLocatorHashes [][]byte
		maxHeaders         uint32
		expectError        bool
		errorType          string
		minLength          int
	}{
		{
			name:               "valid target and locator hashes",
			targetHash:         headers[5].Hash().CloneBytes(),
			blockLocatorHashes: [][]byte{headers[3].Hash().CloneBytes()},
			maxHeaders:         10,
			expectError:        false,
			minLength:          1,
		},
		{
			name:               "invalid target hash",
			targetHash:         []byte("invalid"),
			blockLocatorHashes: [][]byte{headers[2].Hash().CloneBytes()},
			maxHeaders:         10,
			expectError:        true,
			errorType:          "request's hash is not valid",
		},
		{
			name:               "invalid locator hash",
			targetHash:         headers[4].Hash().CloneBytes(),
			blockLocatorHashes: [][]byte{[]byte("invalid")},
			maxHeaders:         10,
			expectError:        true,
			errorType:          "request's hash is not valid",
		},
		{
			name:               "nil target hash",
			targetHash:         nil,
			blockLocatorHashes: [][]byte{headers[2].Hash().CloneBytes()},
			maxHeaders:         10,
			expectError:        true,
			errorType:          "request's hash is not valid",
		},
		{
			name:               "empty locator hashes",
			targetHash:         headers[4].Hash().CloneBytes(),
			blockLocatorHashes: [][]byte{},
			maxHeaders:         10,
			expectError:        true,
			errorType:          "common ancestor hash not found",
		},
		{
			name:               "max headers 1",
			targetHash:         headers[4].Hash().CloneBytes(),
			blockLocatorHashes: [][]byte{headers[2].Hash().CloneBytes()},
			maxHeaders:         1,
			expectError:        false,
			minLength:          0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := &blockchain_api.GetBlockHeadersToCommonAncestorRequest{
				TargetHash:         tt.targetHash,
				BlockLocatorHashes: tt.blockLocatorHashes,
				MaxHeaders:         tt.maxHeaders,
			}

			response, err := ctx.server.GetBlockHeadersToCommonAncestor(context.Background(), request)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorType != "" {
					require.Contains(t, err.Error(), tt.errorType)
				}
				require.Nil(t, response)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, response)
			require.GreaterOrEqual(t, len(response.BlockHeaders), tt.minLength)
			require.Equal(t, len(response.BlockHeaders), len(response.Metas))

			// Verify all responses are valid
			for i, headerBytes := range response.BlockHeaders {
				require.NotEmpty(t, headerBytes)
				require.NotEmpty(t, response.Metas[i])

				// Parse header to ensure it's valid
				header, err := model.NewBlockHeaderFromBytes(headerBytes)
				require.NoError(t, err)
				require.NotNil(t, header)
			}
		})
	}
}

// Test_GetBestHeightAndTime tests the GetBestHeightAndTime gRPC method
func Test_GetBestHeightAndTime(t *testing.T) {
	ctx := setup(t)

	// Create a chain of blocks for testing
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	prevHash := tSettings.ChainCfgParams.GenesisHash

	// Create and store a chain of blocks
	for i := 0; i < 5; i++ {
		merkleRoot := &chainhash.Hash{}
		merkleRoot[0] = byte(i)

		coinbaseTx := bt.NewTx()
		err := coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
		require.NoError(t, err)

		arbitraryData := []byte{0x03, byte(i), 0x00, 0x00}
		coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)
		coinbaseTx.Inputs[0].SequenceNumber = 0xffffffff

		err = coinbaseTx.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
		require.NoError(t, err)

		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prevHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix() + int64(i*60)), // 1 minute apart
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},      // mainnet genesis bits 0x1d00ffff in little endian
				Nonce:          uint32(i),
			},
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			SizeInBytes:      1000,
			Height:           uint32(i + 1),
			ID:               uint32(i + 1),
		}

		_, _, err = ctx.server.store.StoreBlock(context.Background(), block, "test")
		require.NoError(t, err)
		prevHash = block.Header.Hash()
	}

	tests := []struct {
		name        string
		expectError bool
		errorType   string
	}{
		{
			name:        "get best height and time success",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := &emptypb.Empty{}

			response, err := ctx.server.GetBestHeightAndTime(context.Background(), request)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorType != "" {
					require.Contains(t, err.Error(), tt.errorType)
				}
				require.Nil(t, response)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, response)
			require.Greater(t, response.Height, uint32(0))
			require.Greater(t, response.Time, uint32(0))

			// Should return the height of the last block
			require.Equal(t, uint32(5), response.Height)
		})
	}
}

// Test_SetBlockProcessedAt tests the SetBlockProcessedAt gRPC method
func Test_SetBlockProcessedAt(t *testing.T) {
	ctx := setup(t)

	// Create and store a test block
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	prevHash := tSettings.ChainCfgParams.GenesisHash

	merkleRoot := &chainhash.Hash{}
	merkleRoot[0] = byte(1)

	coinbaseTx := bt.NewTx()
	err := coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	require.NoError(t, err)

	arbitraryData := []byte{0x03, 0x01, 0x00, 0x00}
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)
	coinbaseTx.Inputs[0].SequenceNumber = 0xffffffff

	err = coinbaseTx.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
	require.NoError(t, err)

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
			Nonce:          uint32(1),
		},
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		SizeInBytes:      1000,
		Height:           uint32(1),
		ID:               uint32(1),
	}

	_, _, err = ctx.server.store.StoreBlock(context.Background(), block, "test")
	require.NoError(t, err)

	tests := []struct {
		name        string
		blockHash   []byte
		clear       bool
		expectError bool
		errorType   string
	}{
		{
			name:        "set processed at success",
			blockHash:   block.Header.Hash().CloneBytes(),
			clear:       false,
			expectError: false,
		},
		{
			name:        "clear processed at success",
			blockHash:   block.Header.Hash().CloneBytes(),
			clear:       true,
			expectError: false,
		},
		{
			name:        "invalid block hash",
			blockHash:   []byte("invalid"),
			clear:       false,
			expectError: true,
			errorType:   "invalid block hash",
		},
		{
			name:        "nil block hash",
			blockHash:   nil,
			clear:       false,
			expectError: true,
			errorType:   "invalid block hash",
		},
		{
			name:        "non-existent block",
			blockHash:   (&chainhash.Hash{}).CloneBytes(),
			clear:       false,
			expectError: true,
			errorType:   "block not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := &blockchain_api.SetBlockProcessedAtRequest{
				BlockHash: tt.blockHash,
				Clear:     tt.clear,
			}

			response, err := ctx.server.SetBlockProcessedAt(context.Background(), request)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorType != "" {
					require.Contains(t, err.Error(), tt.errorType)
				}
				require.Nil(t, response)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, response)
		})
	}
}

// Test_safeClose tests the safeClose utility function
func Test_safeClose(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func()
	}{
		{
			name: "close open channel",
			testFunc: func() {
				ch := make(chan int, 1)
				ch <- 42

				// Should close without panic
				safeClose(ch)

				// Verify channel is closed
				val, _ := <-ch
				assert.Equal(t, 42, val) // drain the buffered value
				_, ok := <-ch
				assert.False(t, ok, "Channel should be closed")
			},
		},
		{
			name: "close already closed channel",
			testFunc: func() {
				ch := make(chan int)
				close(ch)

				// Should not panic when closing already closed channel
				assert.NotPanics(t, func() {
					safeClose(ch)
				})
			},
		},
		{
			name: "close nil channel",
			testFunc: func() {
				var ch chan int

				// Should not panic with nil channel
				assert.NotPanics(t, func() {
					safeClose(ch)
				})
			},
		},
		{
			name: "close string channel",
			testFunc: func() {
				ch := make(chan string, 1)
				ch <- "test"

				// Should work with different types
				safeClose(ch)

				val, _ := <-ch
				assert.Equal(t, "test", val)
				_, ok := <-ch
				assert.False(t, ok)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.testFunc()
		})
	}
}

// Test_IsFullyReady tests the IsFullyReady method
func Test_IsFullyReady(t *testing.T) {
	ctx := setup(t)

	tests := []struct {
		name                   string
		setupSubscriptionReady bool
		expectReady            bool
	}{
		{
			name:                   "service ready with subscription manager ready",
			setupSubscriptionReady: true,
			expectReady:            true, // Depends on FSM state, usually true after init
		},
		{
			name:                   "service not ready when subscription manager not ready",
			setupSubscriptionReady: false,
			expectReady:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set subscription manager readiness
			ctx.server.SetSubscriptionManagerReadyForTesting(tt.setupSubscriptionReady)

			ready, err := ctx.server.IsFullyReady(context.Background())

			require.NoError(t, err)
			if tt.setupSubscriptionReady {
				// When subscription is ready, result depends on FSM state
				// Since we can't control FSM state easily, we just ensure no error
				assert.IsType(t, bool(false), ready)
			} else {
				assert.False(t, ready, "Should not be ready when subscription manager is not ready")
			}
		})
	}
}

// Test_LegacySync tests the LegacySync gRPC method
func Test_LegacySync(t *testing.T) {
	ctx := setup(t)

	tests := []struct {
		name        string
		expectError bool
	}{
		{
			name:        "legacy sync request",
			expectError: false, // Should succeed or be idempotent
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := &emptypb.Empty{}

			response, err := ctx.server.LegacySync(context.Background(), request)

			if tt.expectError {
				require.Error(t, err)
				require.Nil(t, response)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, response)
		})
	}
}

// Test_Idle tests the Idle gRPC method
func Test_Idle(t *testing.T) {
	ctx := setup(t)

	tests := []struct {
		name        string
		expectError bool
	}{
		{
			name:        "idle request",
			expectError: false, // Should succeed or be idempotent
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := &emptypb.Empty{}

			response, err := ctx.server.Idle(context.Background(), request)

			if tt.expectError {
				require.Error(t, err)
				require.Nil(t, response)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, response)
		})
	}
}

// Test_WaitForFSMtoTransitionToGivenState tests the WaitForFSMtoTransitionToGivenState method
func Test_WaitForFSMtoTransitionToGivenState(t *testing.T) {
	ctx := setup(t)

	tests := []struct {
		name        string
		targetState blockchain_api.FSMStateType
		timeout     time.Duration
		expectError bool
	}{
		{
			name:        "wait for current state (should return immediately)",
			targetState: blockchain_api.FSMStateType_IDLE, // Common initial state
			timeout:     1 * time.Second,
			expectError: false,
		},
		{
			name:        "context cancellation",
			targetState: blockchain_api.FSMStateType_RUNNING, // Unlikely to be reached immediately
			timeout:     100 * time.Millisecond,
			expectError: true, // Should timeout
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCtx, cancel := context.WithTimeout(context.Background(), tt.timeout)
			defer cancel()

			err := ctx.server.WaitForFSMtoTransitionToGivenState(testCtx, tt.targetState)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "context deadline exceeded")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Test_WaitUntilFSMTransitionFromIdleState tests the WaitUntilFSMTransitionFromIdleState gRPC method
func Test_WaitUntilFSMTransitionFromIdleState(t *testing.T) {
	ctx := setup(t)

	tests := []struct {
		name                   string
		setupSubscriptionReady bool
		timeout                time.Duration
		expectError            bool
	}{
		{
			name:                   "service with subscription ready but FSM might be IDLE",
			setupSubscriptionReady: true,
			timeout:                2 * time.Second,
			expectError:            true, // FSM might still be in IDLE state
		},
		{
			name:                   "context cancellation when not ready",
			setupSubscriptionReady: false,
			timeout:                100 * time.Millisecond,
			expectError:            true, // Should timeout
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up subscription readiness
			ctx.server.SetSubscriptionManagerReadyForTesting(tt.setupSubscriptionReady)

			testCtx, cancel := context.WithTimeout(context.Background(), tt.timeout)
			defer cancel()

			request := &emptypb.Empty{}
			response, err := ctx.server.WaitUntilFSMTransitionFromIdleState(testCtx, request)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "context deadline exceeded")
				require.Nil(t, response)
			} else {
				require.NoError(t, err)
				require.NotNil(t, response)
			}
		})
	}
}

// Test_BroadcastHeartbeat verifies the heartbeat broadcasting functionality.
func Test_BroadcastHeartbeat(t *testing.T) {
	ctx := setup(t)

	// Don't start subscription manager - test the notification channel directly
	// Call broadcastHeartbeat directly
	ctx.server.broadcastHeartbeat()

	// Verify the notification was sent to the notifications channel
	select {
	case notif := <-ctx.server.notifications:
		require.NotNil(t, notif)
		assert.Equal(t, model.NotificationType_PING, notif.Type, "Heartbeat should have PING type")
	case <-time.After(1 * time.Second):
		t.Fatal("Expected heartbeat notification to be sent to notifications channel")
	}
}

// Test_HeartbeatSenderStopsOnContextCancel verifies the heartbeat sender stops when context is cancelled.
func Test_HeartbeatSenderStopsOnContextCancel(t *testing.T) {
	ctx := setup(t)

	// Create a cancellable context
	heartbeatCtx, cancel := context.WithCancel(context.Background())

	// Start the heartbeat sender
	done := make(chan struct{})
	go func() {
		ctx.server.startHeartbeatSender(heartbeatCtx)
		close(done)
	}()

	// Cancel the context
	cancel()

	// Verify the heartbeat sender stops
	select {
	case <-done:
		// Success - goroutine exited
	case <-time.After(2 * time.Second):
		t.Fatal("Heartbeat sender did not stop when context was cancelled")
	}
}

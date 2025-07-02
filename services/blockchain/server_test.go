// Package blockchain_test provides testing for the blockchain package.
package blockchain

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/pkg/go-chaincfg"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/stores/blob"
	blob_memory "github.com/bitcoin-sv/teranode/stores/blob/memory"
	blockchain_store "github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/stores/blockchain/sql"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	utxosql "github.com/bitcoin-sv/teranode/stores/utxo/sql"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/assert"
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

	tSettings := test.CreateBaseTestSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

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
	fmt.Println(msg)
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
	fmt.Println(msg)
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

	nBits, _ := model.NewNBitFromString("1d00ffff") // Use mainnet genesis block bits
	tSettings := test.CreateBaseTestSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	hashPrevBlock := tSettings.ChainCfgParams.GenesisHash

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
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
		HashMerkleRoot: subtree.RootHash(),        // doesn't matter, we're only checking the value and not whether it's correct
		Timestamp:      uint32(time.Now().Unix()), // nolint:gosec
		Bits:           *nBits,
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
	tSettings := test.CreateBaseTestSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	prevHash := tSettings.ChainCfgParams.GenesisHash

	for i := 0; i < 150; i++ {
		// Create a unique merkle root for each block
		merkleRoot := &chainhash.Hash{}
		merkleRoot[0] = byte(i)

		// Create a unique block for each iteration
		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prevHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix()),          // nolint:gosec
				Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff}, // Set proper bits from mainnet genesis block
				Nonce:          uint32(i),                          // nolint:gosec
			},
			CoinbaseTx:       bt.NewTx(),
			TransactionCount: 1,
			SizeInBytes:      1000,
			Height:           uint32(i + 1), // nolint:gosec
			ID:               uint32(i + 1), // nolint:gosec
		}

		header := block.Header
		headers = append(headers, header)

		// Store the block before updating prevHash
		_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "test")
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
			require.Equal(t, tt.expectedLen, len(headers))
			require.Equal(t, tt.expectedLen, len(metas))

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

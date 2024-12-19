package blockchain

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	blob_memory "github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	utxo_memory "github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/test"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	tx1, _ = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	hash1  = tx1.TxIDChainHash()
)

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

func Test_GetBlock(t *testing.T) {
	ctx := setup(t)
	_, _, err := ctx.server.store.StoreBlock(context.Background(), mockBlock(ctx, t), "")
	require.NoError(t, err)

	context := context.Background()
	request := &blockchain_api.GetBlockRequest{
		Hash: []byte{1},
	}

	block, err := ctx.server.GetBlock(context, request)
	require.Error(t, err)
	require.Empty(t, block)

	// TODO: Put this back in when we fix WrapGRPC/UnwrapGRPC
	// unwrappedErr := errors.UnwrapGRPC(err)
	// // require.ErrorIs(t, unwrappedErr, errors.ErrBlockNotFound)
	// require.True(t, unwrappedErr.Is(errors.ErrBlockNotFound))

	requestHeight := &blockchain_api.GetBlockByHeightRequest{
		Height: 1,
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

func Test_GetFSMCurrentState(t *testing.T) {
	ctx := setup(t)
	_, _, err := ctx.server.store.StoreBlock(context.Background(), mockBlock(ctx, t), "")
	require.NoError(t, err)

	response, err := ctx.server.GetFSMCurrentState(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Equal(t, blockchain_api.FSMStateType_IDLE, response.State, "Expected FSM state did not match")
}

type testContext struct {
	server       *Blockchain
	subtreeStore blob.Store
	utxoStore    utxo.Store
	logger       ulogger.Logger
}

func setup(t *testing.T) *testContext {
	logger := ulogger.New("blockchain")

	subtreeStore := blob_memory.New()
	utxoStore := utxo_memory.New(logger)
	store := mockStore{}
	tSettings := test.CreateBaseTestSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	server, err := New(context.Background(), logger, tSettings, &store, nil)
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

func Test_HealthGRPC(t *testing.T) {
	ctx := setup(t)

	response, err := ctx.server.HealthGRPC(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.True(t, response.Ok)
}

func mockBlock(ctx *testContext, t *testing.T) *model.Block {
	subtree, err := util.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*hash1, 100, 0))

	_, err = ctx.utxoStore.Create(context.Background(), tx1, 0)
	require.NoError(t, err)

	nBits, _ := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
	coinbase, err := bt.NewTxFromString(coinbaseHex)
	require.NoError(t, err)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = ctx.subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
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
		Height:           0,
	}

	return block
}

func Test_getBlockLocator(t *testing.T) {
	ctx := context.Background()

	t.Run("block 0", func(t *testing.T) {
		store := newMockStore(nil)
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
		store.getBlockByHeight[0] = block

		locator, err := getBlockLocator(ctx, store, nil, 0)
		require.NoError(t, err)

		assert.Len(t, locator, 1)
		assert.Equal(t, block.Hash().String(), locator[0].String())
	})

	t.Run("blocks", func(t *testing.T) {
		store := newMockStore(nil)
		for i := uint32(0); i <= 1024; i++ {
			store.getBlockByHeight[i] = &model.Block{
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
		}

		locator, err := getBlockLocator(ctx, store, store.getBlockByHeight[1024].Hash(), 1024)
		require.NoError(t, err)

		assert.Len(t, locator, 21)

		expectedHeights := []uint32{
			1024,
			1023,
			1022,
			1021,
			1020,
			1019,
			1018,
			1017,
			1016,
			1015,
			1014,
			1013,
			1011,
			1007,
			999,
			983,
			951,
			887,
			759,
			503,
			0,
		}

		for locatorIdx, locatorHash := range locator {
			assert.Equal(t, store.getBlockByHeight[expectedHeights[locatorIdx]].Hash().String(), locatorHash.String())
		}
	})
}

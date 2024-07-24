package blockchain

import (
	"context"
	"testing"
	"time"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"

	blob_memory "github.com/bitcoin-sv/ubsv/stores/blob/memory"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	utxo_memory "github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/usql"
	"github.com/stretchr/testify/require"
)

func Test_GetBlock(t *testing.T) {
	ctx := setup(t)
	_, err := ctx.server.store.StoreBlock(context.Background(), mockBlock(ctx, t), "")
	require.NoError(t, err)

	// var wg sync.WaitGroup
	// wg.Add(1)

	// Start the server
	// ctx := context.Background()
	// go func() {
	// 	//defer wg.Done()
	// 	if err := server.Start(ctx); err != nil {
	// 		t.Errorf("Failed to start server: %v", err)
	// 	}
	// }()

	request := &blockchain_api.GetBlockRequest{
		Hash: []byte{1},
	}

	block, err := ctx.server.GetBlock(context.Background(), request)
	require.Empty(t, block)

	unwrappedErr := errors.UnwrapGRPC(err)
	require.ErrorIs(t, unwrappedErr, errors.ErrBlockNotFound)

	requestHeight := &blockchain_api.GetBlockByHeightRequest{
		Height: 1,
	}

	block, err = ctx.server.GetBlockByHeight(context.Background(), requestHeight)
	require.Empty(t, block)

	// unwrap the error
	unwrappedErr = errors.UnwrapGRPC(err)
	require.ErrorIs(t, unwrappedErr, errors.ErrBlockNotFound)
	var tErr *errors.Error
	assert.ErrorAs(t, unwrappedErr, &tErr)
	assert.Equal(t, tErr.Code, errors.ERR_BLOCK_NOT_FOUND)
	assert.Equal(t, tErr.Message, "block not found")

	//wg.Wait()
	// Stop the server
	// if err := server.Stop(ctx); err != nil {
	// 	t.Fatalf("Failed to stop server: %v", err)
	// }
}

func Test_GetFSMCurrentState(t *testing.T) {
	ctx := setup(t)
	_, err := ctx.server.store.StoreBlock(context.Background(), mockBlock(ctx, t), "")
	require.NoError(t, err)

	// var wg sync.WaitGroup
	// wg.Add(1)

	// Assuming that the setup or initialization of the server sets the FSM state
	// Start the server and the FSM in an initial state you control (not shown here)

	//ctx := context.Background()
	// go func() {
	// 	defer wg.Done()
	// if err := server.Start(ctx); err != nil {
	// 	t.Errorf("Failed to start server: %v", err)
	// }
	//	}()

	// Test the GetFSMCurrentState function
	response, err := ctx.server.GetFSMCurrentState(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Equal(t, blockchain_api.FSMStateType_STOPPED, response.State, "Expected FSM state did not match")

	//wg.Wait()
	// Stop the server
	// if err := server.Stop(ctx); err != nil {
	// 	t.Fatalf("Failed to stop server: %v", err)
	// }
}

type testContext struct {
	server *Blockchain
	logger ulogger.Logger
}

func setup(t *testing.T) *testContext {
	logger := ulogger.New("blockchain")

	subtreeStore := blob_memory.New()
	utxoStore := utxo_memory.New(logger)
	store := mockStore{}
	server, err := New(context.Background(), logger, &store, subtreeStore, utxoStore)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	require.NoError(t, server.Init(context.Background()))

	return &testContext{
		server: server,
		logger: logger,
	}
}

func mockBlock(ctx *testContext, t *testing.T) *model.Block {
	subtree, err := util.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(model.CoinbasePlaceholder, 0, 0))
	require.NoError(t, subtree.AddNode(*hash1, 100, 0))

	_, err = ctx.server.utxoStore.Create(context.Background(), tx1, 0)
	require.NoError(t, err)

	nBits := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

	coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
	coinbase, err := bt.NewTxFromString(coinbaseHex)
	require.NoError(t, err)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = ctx.server.subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes)
	require.NoError(t, err)

	subtreeHashes := make([]*chainhash.Hash, 0)
	subtreeHashes = append(subtreeHashes, subtree.RootHash())

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: subtree.RootHash(), // doesn't matter, we're only checking the value and not whether it's correct
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           nBits,
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

var (
	//tx, _  = hex.DecodeString("0100000001ae73759b118b9c8d54a13ad4ccd5de662bbaa2175ee7f4b413f402affb831ed3000000006b483045022100a6af846212b0c611056a9a30c22f0eed3adc29f8c688e804509b113f322459220220708fb79c66d235d937e8348ac022b3cfa6b64fec8d35a749ea0b2293ad95da014121039f271b930111fd7c818100ee1603d5c5094c68b3d15ad0a58f712e7d766225edffffffff0550c30000000000001976a91448bea2d45f4f6175e47ccb717e4f5d19d8f68f3b88ac204e0000000000001976a91442859b9bada6461d08a0aab8a18105ef30457a8b88ac10270000000000001976a914d0e2122bdeed7b2235f670cdc832f518fb63db9f88ac0c040000000000001976a914d56f84ae869e4a743e929e31218b198f02ce67fe88ac8d0c0100000000001976a91444a8e7fb1a426e4c60597d9d3f534c677d4f858388ac00000000")
	tx1, _ = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	hash1  = tx1.TxIDChainHash()
)

func NewMockStore(block *model.Block) blockchain_store.Store {
	return &mockStore{
		block: block,
	}
}

type mockStore struct {
	block *model.Block
}

func (s *mockStore) GetDB() *usql.DB {
	panic("not implemented")
}

func (s *mockStore) GetDBEngine() util.SQLEngine {
	panic("not implemented")
}
func (s *mockStore) GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint32, error) {
	return s.block, 0, nil
}
func (s *mockStore) GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlockss uint32) ([]*model.Block, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	return nil, errors.ErrBlockNotFound
}
func (s *mockStore) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	panic("not implemented")
}
func (s *mockStore) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	panic("not implemented")
}
func (s *mockStore) GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error) {
	panic("not implemented")
}
func (s *mockStore) GetHashOfAncestorBlock(ctx context.Context, blockHash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockHeight(ctx context.Context, blockHash *chainhash.Hash) (uint32, error) {
	panic("not implemented")
}
func (s *mockStore) StoreBlock(ctx context.Context, block *model.Block, peerID string) (uint64, error) {
	s.block = block
	return 0, nil
}
func (s *mockStore) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	panic("not implemented")
}
func (s *mockStore) GetForkedBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	panic("not implemented")
}
func (s *mockStore) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	panic("not implemented")
}
func (s *mockStore) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	panic("not implemented")
}
func (s *mockStore) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	panic("not implemented")
}
func (s *mockStore) GetState(ctx context.Context, key string) ([]byte, error) {
	panic("not implemented")
}
func (s *mockStore) SetState(ctx context.Context, key string, data []byte) error {
	panic("not implemented")
}
func (s *mockStore) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	panic("not implemented")
}
func (s *mockStore) GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error) {
	panic("not implemented")
}
func (s *mockStore) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	panic("not implemented")
}
func (s *mockStore) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlocksByTime(ctx context.Context, fromTime, toTime time.Time) ([][]byte, error) {
	panic("not implemented")
}
func (s *mockStore) LocateBlockHashes(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*chainhash.Hash, error) {
	//TODO implement me
	panic("implement me")
}

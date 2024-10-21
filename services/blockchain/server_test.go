package blockchain

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"

	"sync"

	"sync/atomic"

	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/file"
	blob_memory "github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	blockchain_options "github.com/bitcoin-sv/ubsv/stores/blockchain/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	utxo_memory "github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/usql"
	"github.com/stretchr/testify/require"
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
	assert.Equal(t, blockchain_api.FSMStateType_STOPPED, response.State, "Expected FSM state did not match")
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

	server, err := New(context.Background(), logger, &store)
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

var (
	tx1, _ = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	hash1  = tx1.TxIDChainHash()
)

func NewMockStore(block *model.Block) blockchain_store.Store {
	return &mockStore{
		block: block,
		state: "STOPPED",
	}
}

type mockStore struct {
	block *model.Block
	state string
}

func (s *mockStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return http.StatusOK, "OK", nil
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
func (s *mockStore) StoreBlock(ctx context.Context, block *model.Block, peerID string, opts ...blockchain_options.StoreBlockOption) (uint64, uint32, error) {
	s.block = block
	return 0, 0, nil
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
func (s *mockStore) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
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
func (s *mockStore) LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error) {
	// TODO implement me
	panic("implement me")
}

func (s *mockStore) ExportBlockDB(ctx context.Context, hash *chainhash.Hash) (*file.File, error) {
	panic("not implemented")
}

func (s *mockStore) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	panic("not implemented")
}

func (s *mockStore) SetFSMState(ctx context.Context, fsmState string) error {
	s.state = fsmState
	return nil
}

func (s *mockStore) GetFSMState(ctx context.Context) (string, error) {
	return s.state, nil
}

type mockKafkaProducer struct {
	messages [][]byte
	mu       sync.Mutex
}

func (m *mockKafkaProducer) Send(key []byte, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, data)
	return nil
}

func (m *mockKafkaProducer) Close() error {
	return nil
}

func (m *mockKafkaProducer) GetClient() sarama.ConsumerGroup {
	return nil
}

type mockKafkaProducerWithTemporaryError struct {
	successAfter time.Time
	messagesSent int32
}

func (m *mockKafkaProducerWithTemporaryError) Send(key []byte, data []byte) error {
	if time.Now().Before(m.successAfter) {
		return errors.New(errors.ERR_UNKNOWN, "temporary Kafka error")
	}

	atomic.AddInt32(&m.messagesSent, 1)
	return nil
}

func (m *mockKafkaProducerWithTemporaryError) Close() error {
	return nil
}

func (m *mockKafkaProducerWithTemporaryError) GetClient() sarama.ConsumerGroup {
	return nil
}

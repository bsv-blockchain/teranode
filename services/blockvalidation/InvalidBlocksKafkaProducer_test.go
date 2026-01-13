package blockvalidation

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/stretchr/testify/mock"
)

// MockInvalidBlockHandler mocks the InvalidBlockHandler interface
type MockInvalidBlockHandler struct {
	mock.Mock
}

func (m *MockInvalidBlockHandler) ReportInvalidBlock(ctx context.Context, blockHash string, reason string) error {
	args := m.Called(ctx, blockHash, reason)
	return args.Error(0)
}

// MockBlockchainClient mocks the blockchain client for testing
type MockBlockchainClient struct {
	mock.Mock
}

func (m *MockBlockchainClient) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(ctx, hash, numberOfHeaders)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}

	var meta []*model.BlockHeaderMeta
	if args.Get(1) != nil {
		meta = args.Get(1).([]*model.BlockHeaderMeta)
	}

	return args.Get(0).([]*model.BlockHeader), meta, args.Error(2)
}

func (m *MockBlockchainClient) InvalidateBlock(ctx context.Context, hash *chainhash.Hash) error {
	args := m.Called(ctx, hash)
	return args.Error(0)
}

func (m *MockBlockchainClient) GetBlockExists(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	args := m.Called(ctx, hash)
	return args.Bool(0), args.Error(1)
}

// AddBlock implements the blockchain.ClientI interface
func (m *MockBlockchainClient) AddBlock(ctx context.Context, block *model.Block, baseURL string) error {
	args := m.Called(ctx, block, baseURL)
	return args.Error(0)
}

// CatchUpBlocks implements the blockchain.ClientI interface
func (m *MockBlockchainClient) CatchUpBlocks(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// CheckBlockIsInCurrentChain implements the blockchain.ClientI interface
func (m *MockBlockchainClient) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	args := m.Called(ctx, blockIDs)
	return args.Bool(0), args.Error(1)
}

// GetBestBlockHeader implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}

	var meta *model.BlockHeaderMeta
	if args.Get(1) != nil {
		meta = args.Get(1).(*model.BlockHeaderMeta)
	}

	return args.Get(0).(*model.BlockHeader), meta, args.Error(2)
}

// GetBestHeightAndTime implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBestHeightAndTime(ctx context.Context) (uint32, uint32, error) {
	args := m.Called(ctx)
	return args.Get(0).(uint32), args.Get(1).(uint32), args.Error(2)
}

// GetBlock implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBlock(ctx context.Context, hash *chainhash.Hash) (*model.Block, error) {
	args := m.Called(ctx, hash)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.Block), args.Error(1)
}

// GetBlockByHeight implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	args := m.Called(ctx, height)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.Block), args.Error(1)
}

// GetBlockByID implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBlockByID(ctx context.Context, id uint64) (*model.Block, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.Block), args.Error(1)
}

// GetBlockGraphData implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	args := m.Called(ctx, periodMillis)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.BlockDataPoints), args.Error(1)
}

// GetBlockHeader implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	args := m.Called(ctx, hash)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}

	var meta *model.BlockHeaderMeta
	if args.Get(1) != nil {
		meta = args.Get(1).(*model.BlockHeaderMeta)
	}

	return args.Get(0).(*model.BlockHeader), meta, args.Error(2)
}

// GetBlockHeaderIDs implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	args := m.Called(ctx, blockHash, numberOfHeaders)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]uint32), args.Error(1)
}

// GetBlockHeadersByHeight implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(ctx, startHeight, endHeight)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}

	var meta []*model.BlockHeaderMeta
	if args.Get(1) != nil {
		meta = args.Get(1).([]*model.BlockHeaderMeta)
	}

	return args.Get(0).([]*model.BlockHeader), meta, args.Error(2)
}

// GetBlockHeadersFromHeight implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(ctx, height, limit)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}

	var meta []*model.BlockHeaderMeta
	if args.Get(1) != nil {
		meta = args.Get(1).([]*model.BlockHeaderMeta)
	}

	return args.Get(0).([]*model.BlockHeader), meta, args.Error(2)
}

// GetBlockHeadersFromTill implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(ctx, blockHashFrom, blockHashTill)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}

	var meta []*model.BlockHeaderMeta
	if args.Get(1) != nil {
		meta = args.Get(1).([]*model.BlockHeaderMeta)
	}

	return args.Get(0).([]*model.BlockHeader), meta, args.Error(2)
}

// GetBlockHeadersToCommonAncestor implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(ctx, hashTarget, blockLocatorHashes, maxHeaders)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}

	var meta []*model.BlockHeaderMeta
	if args.Get(1) != nil {
		meta = args.Get(1).([]*model.BlockHeaderMeta)
	}

	return args.Get(0).([]*model.BlockHeader), meta, args.Error(2)
}

// GetBlockIsMined implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBlockIsMined(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	args := m.Called(ctx, hash)
	return args.Bool(0), args.Error(1)
}

// GetBlockLocator implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBlockLocator(ctx context.Context, hash *chainhash.Hash, maxBlocks uint32) ([]*chainhash.Hash, error) {
	args := m.Called(ctx, hash, maxBlocks)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*chainhash.Hash), args.Error(1)
}

// GetBlockStats implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.BlockStats), args.Error(1)
}

// GetBlocks implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error) {
	args := m.Called(ctx, blockHash, numberOfBlocks)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.Block), args.Error(1)
}

// GetBlocksMinedNotSet implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.Block), args.Error(1)
}

// GetBlocksSubtreesNotSet implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.Block), args.Error(1)
}

// GetFSMCurrentState implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetFSMCurrentState(ctx context.Context) (*blockchain.FSMStateType, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	state := args.Get(0).(blockchain.FSMStateType)

	return &state, args.Error(1)
}

// GetFSMCurrentStateForE2ETestMode implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetFSMCurrentStateForE2ETestMode() blockchain.FSMStateType {
	args := m.Called()
	return args.Get(0).(blockchain.FSMStateType)
}

// GetHashOfAncestorBlock implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetHashOfAncestorBlock(ctx context.Context, hash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	args := m.Called(ctx, hash, depth)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*chainhash.Hash), args.Error(1)
}

// GetLastNBlocks implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	args := m.Called(ctx, n, includeOrphans, fromHeight)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.BlockInfo), args.Error(1)
}

// GetLastNInvalidBlocks implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetLastNInvalidBlocks(ctx context.Context, n int64) ([]*model.BlockInfo, error) {
	args := m.Called(ctx, n)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.BlockInfo), args.Error(1)
}

// GetNextWorkRequired implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetNextWorkRequired(ctx context.Context, hash *chainhash.Hash, currentBlockTime int64) (*model.NBit, error) {
	args := m.Called(ctx, hash, currentBlockTime)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.NBit), args.Error(1)
}

// GetState implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetState(ctx context.Context, key string) ([]byte, error) {
	args := m.Called(ctx, key)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]byte), args.Error(1)
}

// GetSuitableBlock implements the blockchain.ClientI interface
func (m *MockBlockchainClient) GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error) {
	args := m.Called(ctx, blockHash)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.SuitableBlock), args.Error(1)
}

// Health implements the blockchain.ClientI interface
func (m *MockBlockchainClient) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)
	return args.Int(0), args.String(1), args.Error(2)
}

// Idle implements the blockchain.ClientI interface
func (m *MockBlockchainClient) Idle(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// IsFSMCurrentState implements the blockchain.ClientI interface
func (m *MockBlockchainClient) IsFSMCurrentState(ctx context.Context, state blockchain.FSMStateType) (bool, error) {
	args := m.Called(ctx, state)
	return args.Bool(0), args.Error(1)
}

// LegacySync implements the blockchain.ClientI interface
func (m *MockBlockchainClient) LegacySync(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// LocateBlockHeaders implements the blockchain.ClientI interface
func (m *MockBlockchainClient) LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error) {
	args := m.Called(ctx, locator, hashStop, maxHashes)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.BlockHeader), args.Error(1)
}

// RevalidateBlock implements the blockchain.ClientI interface
func (m *MockBlockchainClient) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	args := m.Called(ctx, blockHash)
	return args.Error(0)
}

// Run implements the blockchain.ClientI interface
func (m *MockBlockchainClient) Run(ctx context.Context, source string) error {
	args := m.Called(ctx, source)
	return args.Error(0)
}

// SendFSMEvent implements the blockchain.ClientI interface
func (m *MockBlockchainClient) SendFSMEvent(ctx context.Context, event blockchain.FSMEventType) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

// SendNotification implements the blockchain.ClientI interface
func (m *MockBlockchainClient) SendNotification(ctx context.Context, notification interface{}) error {
	args := m.Called(ctx, notification)
	return args.Error(0)
}

// SetBlockMinedSet implements the blockchain.ClientI interface
func (m *MockBlockchainClient) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	args := m.Called(ctx, blockHash)
	return args.Error(0)
}

func (m *MockBlockchainClient) ClearBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	args := m.Called(ctx, blockHash)
	return args.Error(0)
}

// SetBlockProcessedAt implements the blockchain.ClientI interface
func (m *MockBlockchainClient) SetBlockProcessedAt(ctx context.Context, blockHash *chainhash.Hash, clear ...bool) error {
	args := m.Called(ctx, blockHash, clear)
	return args.Error(0)
}

// SetBlockSubtreesSet implements the blockchain.ClientI interface
func (m *MockBlockchainClient) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	args := m.Called(ctx, blockHash)
	return args.Error(0)
}

// SetState implements the blockchain.ClientI interface
func (m *MockBlockchainClient) SetState(ctx context.Context, key string, data []byte) error {
	args := m.Called(ctx, key, data)
	return args.Error(0)
}

// Subscribe implements the blockchain.ClientI interface
func (m *MockBlockchainClient) Subscribe(ctx context.Context, source string) (chan interface{}, error) {
	args := m.Called(ctx, source)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(chan interface{}), args.Error(1)
}

// WaitForFSMtoTransitionToGivenState implements the blockchain.ClientI interface
func (m *MockBlockchainClient) WaitForFSMtoTransitionToGivenState(ctx context.Context, state blockchain.FSMStateType) error {
	args := m.Called(ctx, state)
	return args.Error(0)
}

// WaitUntilFSMTransitionFromIdleState implements the blockchain.ClientI interface
func (m *MockBlockchainClient) WaitUntilFSMTransitionFromIdleState(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// MockKafkaAsyncProducer implements the KafkaAsyncProducerI interface for testing
type MockKafkaAsyncProducer struct {
	mock.Mock
}

func (m *MockKafkaAsyncProducer) Publish(msg *kafka.Message) {
	m.Called(msg)
}

func (m *MockKafkaAsyncProducer) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockKafkaAsyncProducer) Stop() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockKafkaAsyncProducer) BrokersURL() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

func (m *MockKafkaAsyncProducer) Start(ctx context.Context, ch chan *kafka.Message) {
	m.Called(ctx, ch)
}

package blockchain

import (
	"context"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/mock"
)

// Mock implements the blockchain.ClientI interface for testing purposes
type Mock struct {
	mock.Mock
}

// Health mocks the Health method
func (m *Mock) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)

	if args.Error(2) != nil {
		return 0, "", args.Error(2)
	}

	return args.Int(0), args.String(1), args.Error(2)
}

// AddBlock mocks the AddBlock method
func (m *Mock) AddBlock(ctx context.Context, block *model.Block, peerID string) error {
	args := m.Called(ctx, block, peerID)

	return args.Error(0)
}

// SendNotification mocks the SendNotification method
func (m *Mock) SendNotification(ctx context.Context, notification *blockchain_api.Notification) error {
	args := m.Called(ctx, notification)
	return args.Error(0)
}

// GetBlock mocks the GetBlock method
func (m *Mock) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	args := m.Called(ctx, blockHash)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.Block), args.Error(1)
}

// GetBlocks mocks the GetBlocks method
func (m *Mock) GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error) {
	args := m.Called(ctx, blockHash, numberOfBlocks)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.Block), args.Error(1)
}

// GetBlockByHeight mocks the GetBlockByHeight method
func (m *Mock) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	args := m.Called(ctx, height)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.Block), args.Error(1)
}

// GetBlockByID mocks the GetBlockByID method
func (m *Mock) GetBlockByID(ctx context.Context, id uint64) (*model.Block, error) {
	args := m.Called(ctx, id)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.Block), args.Error(1)
}

// GetBlockStats mocks the GetBlockStats method
func (m *Mock) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	args := m.Called(ctx)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.BlockStats), args.Error(1)
}

// GetBlockGraphData mocks the GetBlockGraphData method
func (m *Mock) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	args := m.Called(ctx, periodMillis)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.BlockDataPoints), args.Error(1)
}

// GetLastNBlocks mocks the GetLastNBlocks method
func (m *Mock) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	args := m.Called(ctx, n, includeOrphans, fromHeight)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.BlockInfo), args.Error(1)
}

// GetSuitableBlock mocks the GetSuitableBlock method
func (m *Mock) GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error) {
	args := m.Called(ctx, blockHash)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.SuitableBlock), args.Error(1)
}

// GetHashOfAncestorBlock mocks the GetHashOfAncestorBlock method
func (m *Mock) GetHashOfAncestorBlock(ctx context.Context, hash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	args := m.Called(ctx, hash, depth)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*chainhash.Hash), args.Error(1)
}

// GetNextWorkRequired mocks the GetNextWorkRequired method
func (m *Mock) GetNextWorkRequired(ctx context.Context, hash *chainhash.Hash) (*model.NBit, error) {
	args := m.Called(ctx, hash)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.NBit), args.Error(1)
}

// GetBlockExists mocks the GetBlockExists method
func (m *Mock) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	args := m.Called(ctx, blockHash)
	return args.Bool(0), args.Error(1)
}

// GetBestBlockHeader mocks the GetBestBlockHeader method
func (m *Mock) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	args := m.Called(ctx)

	if args.Error(2) != nil {
		return nil, nil, args.Error(2)
	}

	return args.Get(0).(*model.BlockHeader), args.Get(1).(*model.BlockHeaderMeta), args.Error(2)
}

// GetBlockHeader mocks the GetBlockHeader method
func (m *Mock) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	args := m.Called(ctx, blockHash)

	if args.Error(2) != nil {
		return nil, nil, args.Error(2)
	}

	return args.Get(0).(*model.BlockHeader), args.Get(1).(*model.BlockHeaderMeta), args.Error(2)
}

// GetBlockHeaders mocks the GetBlockHeaders method
func (m *Mock) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(ctx, blockHash, numberOfHeaders)

	if args.Error(2) != nil {
		return nil, nil, args.Error(2)
	}

	return args.Get(0).([]*model.BlockHeader), args.Get(1).([]*model.BlockHeaderMeta), args.Error(2)
}

// GetBlockHeadersToCommonAncestor mocks the GetBlockHeadersToCommonAncestor method
func (m *Mock) GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(ctx, hashTarget, blockLocatorHashes)

	if args.Error(2) != nil {
		return nil, nil, args.Error(2)
	}

	return args.Get(0).([]*model.BlockHeader), args.Get(1).([]*model.BlockHeaderMeta), args.Error(2)
}

// GetBlockHeadersFromTill mocks the GetBlockHeadersFromTill method
func (m *Mock) GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(ctx, blockHashFrom, blockHashTill)

	if args.Error(2) != nil {
		return nil, nil, args.Error(2)
	}

	return args.Get(0).([]*model.BlockHeader), args.Get(1).([]*model.BlockHeaderMeta), args.Error(2)
}

// GetBlockHeadersFromHeight mocks the GetBlockHeadersFromHeight method
func (m *Mock) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(ctx, height, limit)

	if args.Error(2) != nil {
		return nil, nil, args.Error(2)
	}

	return args.Get(0).([]*model.BlockHeader), args.Get(1).([]*model.BlockHeaderMeta), args.Error(2)
}

// GetBlockHeadersByHeight mocks the GetBlockHeadersByHeight method
func (m *Mock) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(ctx, startHeight, endHeight)

	if args.Error(2) != nil {
		return nil, nil, args.Error(2)
	}

	return args.Get(0).([]*model.BlockHeader), args.Get(1).([]*model.BlockHeaderMeta), args.Error(2)
}

// InvalidateBlock mocks the InvalidateBlock method
func (m *Mock) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	args := m.Called(ctx, blockHash)
	return args.Error(0)
}

// RevalidateBlock mocks the RevalidateBlock method
func (m *Mock) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	args := m.Called(ctx, blockHash)
	return args.Error(0)
}

// GetBlockHeaderIDs mocks the GetBlockHeaderIDs method
func (m *Mock) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	args := m.Called(ctx, blockHash, numberOfHeaders)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]uint32), args.Error(1)
}

// Subscribe mocks the Subscribe method
func (m *Mock) Subscribe(ctx context.Context, source string) (chan *blockchain_api.Notification, error) {
	args := m.Called(ctx, source)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(chan *blockchain_api.Notification), args.Error(1)
}

// GetState mocks the GetState method
func (m *Mock) GetState(ctx context.Context, key string) ([]byte, error) {
	args := m.Called(ctx, key)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]byte), args.Error(1)
}

// SetState mocks the SetState method
func (m *Mock) SetState(ctx context.Context, key string, data []byte) error {
	args := m.Called(ctx, key, data)
	return args.Error(0)
}

// SetBlockMinedSet mocks the SetBlockMinedSet method
func (m *Mock) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	args := m.Called(ctx, blockHash)
	return args.Error(0)
}

// GetBlockIsMined mocks the GetBlockIsMined method
func (m *Mock) GetBlockIsMined(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	args := m.Called(ctx, blockHash)
	return args.Bool(0), args.Error(1)
}

// GetBlocksMinedNotSet mocks the GetBlocksMinedNotSet method
func (m *Mock) GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error) {
	args := m.Called(ctx)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.Block), args.Error(1)
}

// SetBlockSubtreesSet mocks the SetBlockSubtreesSet method
func (m *Mock) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	args := m.Called(ctx, blockHash)
	return args.Error(0)
}

// GetBlocksSubtreesNotSet mocks the GetBlocksSubtreesNotSet method
func (m *Mock) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	args := m.Called(ctx)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.Block), args.Error(1)
}

// GetBestHeightAndTime mocks the GetBestHeightAndTime method
func (m *Mock) GetBestHeightAndTime(ctx context.Context) (uint32, uint32, error) {
	args := m.Called(ctx)

	if args.Error(2) != nil {
		return 0, 0, args.Error(2)
	}

	//nolint:gosec // This is a mock
	return uint32(args.Int(0)), uint32(args.Int(1)), args.Error(2)
}

// CheckBlockIsInCurrentChain mocks the CheckBlockIsInCurrentChain method
func (m *Mock) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	args := m.Called(ctx, blockIDs)
	return args.Bool(0), args.Error(1)
}

// GetFSMCurrentState mocks the GetFSMCurrentState method
func (m *Mock) GetFSMCurrentState(ctx context.Context) (*FSMStateType, error) {
	args := m.Called(ctx)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*FSMStateType), args.Error(1)
}

// IsFSMCurrentState mocks the IsFSMCurrentState method
func (m *Mock) IsFSMCurrentState(ctx context.Context, state FSMStateType) (bool, error) {
	args := m.Called(ctx, state)
	return args.Bool(0), args.Error(1)
}

// WaitForFSMtoTransitionToGivenState mocks the WaitForFSMtoTransitionToGivenState method
func (m *Mock) WaitForFSMtoTransitionToGivenState(ctx context.Context, state FSMStateType) error {
	args := m.Called(ctx, state)
	return args.Error(0)
}

// WaitUntilFSMTransitionFromIdleState mocks the WaitUntilFSMTransitionFromIdleState method
func (m *Mock) WaitUntilFSMTransitionFromIdleState(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// GetFSMCurrentStateForE2ETestMode mocks the GetFSMCurrentStateForE2ETestMode method
func (m *Mock) GetFSMCurrentStateForE2ETestMode() FSMStateType {
	args := m.Called()
	return args.Get(0).(FSMStateType)
}

// Run mocks the Run method
func (m *Mock) Run(ctx context.Context, source string) error {
	args := m.Called(ctx, source)
	return args.Error(0)
}

// CatchUpBlocks mocks the CatchUpBlocks method
func (m *Mock) CatchUpBlocks(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// LegacySync mocks the LegacySync method
func (m *Mock) LegacySync(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// Idle mocks the Idle method
func (m *Mock) Idle(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// SendFSMEvent mocks the SendFSMEvent method
func (m *Mock) SendFSMEvent(ctx context.Context, event FSMEventType) error {
	args := m.Called(ctx, event)

	return args.Error(0)
}

// GetBlockLocator mocks the GetBlockLocator method
func (m *Mock) GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, blockHeaderHeight uint32) ([]*chainhash.Hash, error) {
	args := m.Called(ctx, blockHeaderHash, blockHeaderHeight)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*chainhash.Hash), args.Error(1)
}

// LocateBlockHeaders mocks the LocateBlockHeaders method
func (m *Mock) LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error) {
	args := m.Called(ctx, locator, hashStop, maxHashes)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.BlockHeader), args.Error(1)
}

// GetLastNInvalidBlocks mocks the GetLastNInvalidBlocks method
func (m *Mock) GetLastNInvalidBlocks(ctx context.Context, n int64) ([]*model.BlockInfo, error) {
	args := m.Called(ctx, n)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.BlockInfo), args.Error(1)
}

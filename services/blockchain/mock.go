package blockchain

import (
	"context"
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Mock implements the blockchain.ClientI interface for testing purposes.
type Mock struct {
	mock.Mock
}

// Health mocks the Health method.
func (m *Mock) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)

	if args.Error(2) != nil {
		return 0, "", args.Error(2)
	}

	return args.Int(0), args.String(1), args.Error(2)
}

// AddBlock mocks the AddBlock method
func (m *Mock) AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) error {
	args := m.Called(ctx, block, peerID, opts)

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

// GetNextBlockID mocks the GetNextBlockID method
func (m *Mock) GetNextBlockID(ctx context.Context) (uint64, error) {
	args := m.Called(ctx)

	if args.Error(1) != nil {
		return 0, args.Error(1)
	}

	return args.Get(0).(uint64), args.Error(1)
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
func (m *Mock) GetNextWorkRequired(ctx context.Context, hash *chainhash.Hash, currentBlockTime int64) (*model.NBit, error) {
	args := m.Called(ctx, hash, currentBlockTime)

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
func (m *Mock) GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(ctx, hashTarget, blockLocatorHashes, maxHeaders)

	if args.Error(2) != nil {
		return nil, nil, args.Error(2)
	}

	return args.Get(0).([]*model.BlockHeader), args.Get(1).([]*model.BlockHeaderMeta), args.Error(2)
}

// GetBlockHeadersFromCommonAncestor mocks the GetBlockHeadersToCommonAncestor method
func (m *Mock) GetBlockHeadersFromCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(ctx, hashTarget, blockLocatorHashes, maxHeaders)

	if args.Error(2) != nil {
		return nil, nil, args.Error(2)
	}

	return args.Get(0).([]*model.BlockHeader), args.Get(1).([]*model.BlockHeaderMeta), args.Error(2)
}

// GetLatestBlockHeaderFromBlockLocator retrieves the latest block header from a block locator.
func (m *Mock) GetLatestBlockHeaderFromBlockLocator(ctx context.Context, bestBlockHash *chainhash.Hash, blockLocator []chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	args := m.Called(ctx, bestBlockHash, blockLocator)

	if args.Error(2) != nil {
		return nil, nil, args.Error(2)
	}

	return args.Get(0).(*model.BlockHeader), args.Get(1).(*model.BlockHeaderMeta), args.Error(2)
}

// GetBlockHeadersFromOldest retrieves block headers starting from the oldest block.
func (m *Mock) GetBlockHeadersFromOldest(ctx context.Context, chainTipHash, targetHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(ctx, chainTipHash, targetHash, numberOfHeaders)

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

// GetBlocksByHeight mocks the GetBlocksByHeight method
func (m *Mock) GetBlocksByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.Block, error) {
	args := m.Called(ctx, startHeight, endHeight)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.Block), args.Error(1)
}

func (m *Mock) FindBlocksContainingSubtree(ctx context.Context, subtreeHash *chainhash.Hash, maxBlocks uint32) ([]*model.Block, error) {
	args := m.Called(ctx, subtreeHash, maxBlocks)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.Block), args.Error(1)
}

// InvalidateBlock mocks the InvalidateBlock method
func (m *Mock) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) ([]chainhash.Hash, error) {
	args := m.Called(ctx, blockHash)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]chainhash.Hash), args.Error(1)
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

func (m *Mock) ClearBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	args := m.Called(ctx, blockHash)
	return args.Error(0)
}

// SetBlockProcessedAt mocks the SetBlockProcessedAt method
func (m *Mock) SetBlockProcessedAt(ctx context.Context, blockHash *chainhash.Hash, clear ...bool) error {
	args := m.Called(ctx, blockHash, clear)
	return args.Error(0)
}

// SetBlockPersistedAt mocks the SetBlockPersistedAt method
func (m *Mock) SetBlockPersistedAt(ctx context.Context, blockHash *chainhash.Hash) error {
	args := m.Called(ctx, blockHash)
	return args.Error(0)
}

// GetBlocksNotPersisted mocks the GetBlocksNotPersisted method
func (m *Mock) GetBlocksNotPersisted(ctx context.Context, limit int) ([]*model.Block, error) {
	args := m.Called(ctx, limit)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.Block), args.Error(1)
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

// GetChainTips mocks the GetChainTips method
func (m *Mock) GetChainTips(ctx context.Context) ([]*model.ChainTip, error) {
	args := m.Called(ctx)
	return args.Get(0).([]*model.ChainTip), args.Error(1)
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

// IsFullyReady mocks the IsFullyReady method
func (m *Mock) IsFullyReady(ctx context.Context) (bool, error) {
	args := m.Called(ctx)
	return args.Bool(0), args.Error(1)
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

// ReportPeerFailure mocks the ReportPeerFailure method
func (m *Mock) ReportPeerFailure(ctx context.Context, hash *chainhash.Hash, peerID string, failureType string, reason string) error {
	args := m.Called(ctx, hash, peerID, failureType, reason)
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

type mockDifficulty struct {
	mock.Mock
	forceError bool
}

func (m *mockDifficulty) CalcNextWorkRequired(ctx context.Context, header *model.BlockHeader, height uint32, testnetArgs ...int64) (*model.NBit, error) {
	args := m.Called(ctx)
	if m.forceError {
		return nil, args.Error(1)
	}
	bytesLittleEndian := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytesLittleEndian, 0x1d00ffff)
	nbits, _ := model.NewNBitFromSlice(bytesLittleEndian)
	return nbits, nil
}

type errorStoreGetBlockHeaders struct {
	mock.Mock
	blockchain_store.MockStore
}

func (s *errorStoreGetBlockHeaders) GetBlockHeaders(ctx context.Context, h *chainhash.Hash, n uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := s.Called(ctx)
	return nil, nil, args.Error(1)
}

type emptyStoreGetBlockHeaders struct {
	blockchain_store.MockStore
}

func (s *emptyStoreGetBlockHeaders) GetBlockHeaders(ctx context.Context, h *chainhash.Hash, n uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return []*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil
}

type errorStoreGetBlockHeader struct {
	mock.Mock
	blockchain_store.MockStore
}

func (s *errorStoreGetBlockHeader) GetBlockHeader(ctx context.Context, h *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	args := s.Called(ctx, h)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}
	return args.Get(0).(*model.BlockHeader), args.Get(1).(*model.BlockHeaderMeta), args.Error(2)
}

type emptyStoreGetBlockHeader struct {
	blockchain_store.MockStore
}

func (s *emptyStoreGetBlockHeader) GetBlockHeader(ctx context.Context, h *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return nil, nil, errors.NewBlockNotFoundError("block header not found")
}

type errorStore struct {
	mock.Mock
	blockchain_store.MockStore
}

func (e *errorStore) GetNextBlockID(ctx context.Context) (uint64, error) {
	args := e.Called(ctx)
	return 0, args.Error(1)
}

func (e *errorStore) GetLatestBlockHeaderFromBlockLocator(
	ctx context.Context,
	bestBlockHash *chainhash.Hash,
	locator []chainhash.Hash,
) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	args := e.Called(ctx, bestBlockHash, locator)
	return nil, nil, args.Error(2)
}

type fakeStoreOldest struct {
	blockchain_store.MockStore
	fn func(ctx context.Context, chainTipHash, targetHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
}

func (f *fakeStoreOldest) GetBlockHeadersFromOldest(ctx context.Context, chainTipHash, targetHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return f.fn(ctx, chainTipHash, targetHash, numberOfHeaders)
}

type errorStoreGetBlockExists struct {
	mock.Mock
}

func (e *errorStoreGetBlockExists) GetBlockExists(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	args := e.Called(ctx, hash)
	return args.Bool(0), args.Error(1)
}

func (e *errorStore) GetBlockExists(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	args := e.Called(ctx, hash)
	return args.Bool(0), args.Error(1)
}

type mockStoreCheckBlockChain struct {
	*blockchain_store.MockStore
	mock.Mock
}

func (m *mockStoreCheckBlockChain) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	args := m.Called(ctx, blockIDs)
	return args.Bool(0), args.Error(1)
}

func (m *mockStoreCheckBlockChain) GetChainTips(ctx context.Context) ([]*model.ChainTip, error) {
	args := m.Called(ctx)
	return args.Get(0).([]*model.ChainTip), args.Error(1)
}

type mockStoreGetBlockHeader struct {
	*blockchain_store.MockStore
	mock.Mock
}

func (m *mockStoreGetBlockHeader) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	args := m.Called(ctx, blockHash)

	if args.Error(2) != nil {
		return nil, nil, args.Error(2)
	}
	return args.Get(0).(*model.BlockHeader), args.Get(1).(*model.BlockHeaderMeta), nil
}

type mockStoreGetBlockHeaders struct {
	*blockchain_store.MockStore
	mock.Mock
}

func (m *mockStoreGetBlockHeaders) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(ctx, blockHash, numberOfHeaders)
	return args.Get(0).([]*model.BlockHeader), args.Get(1).([]*model.BlockHeaderMeta), args.Error(2)
}

type mockStoreState struct {
	*blockchain_store.MockStore
	mock.Mock
}

func (m *mockStoreState) GetState(ctx context.Context, key string) ([]byte, error) {
	args := m.Called(ctx, key)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), nil
}

func (m *mockStoreState) SetState(ctx context.Context, key string, data []byte) error {
	args := m.Called(ctx, key, data)
	return args.Error(0)
}

type mockStoreGetBlockHeaderIDs struct {
	*blockchain_store.MockStore
	mock.Mock
}

func (m *mockStoreGetBlockHeaderIDs) GetBlockHeaderIDs(ctx context.Context, startHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	args := m.Called(ctx, startHash, numberOfHeaders)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]uint32), nil
}

type mockStoreGetSetBlockIsMined struct {
	*blockchain_store.MockStore
	mock.Mock
}

func (m *mockStoreGetSetBlockIsMined) GetBlockIsMined(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	args := m.Called(ctx, hash)
	if args.Error(1) != nil {
		return false, args.Error(1)
	}
	return args.Bool(0), nil
}

func (m *mockStoreGetSetBlockIsMined) SetBlockMinedSet(ctx context.Context, hash *chainhash.Hash) error {
	args := m.Called(ctx, hash)
	return args.Error(0)
}

func (m *mockStoreGetSetBlockIsMined) GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error) {
	args := m.Called(ctx)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*model.Block), args.Error(1)
}

func (m *mockStoreGetSetBlockIsMined) SetBlockSubtreesSet(ctx context.Context, hash *chainhash.Hash) error {
	args := m.Called(ctx, hash)
	return args.Error(0)
}

func (m *mockStoreGetSetBlockIsMined) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	args := m.Called(ctx)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*model.Block), nil
}

type mockFSM struct {
	states []string
	index  int
}

func (m *mockFSM) Current() string {
	if m.index < len(m.states)-1 {
		val := m.states[m.index]
		m.index++
		return val
	}
	return m.states[len(m.states)-1]
}

type mockStoreLocateBlockHeaders struct {
	*blockchain_store.MockStore
	mock.Mock
}

func (m *mockStoreLocateBlockHeaders) LocateBlockHeaders(
	ctx context.Context,
	locator []*chainhash.Hash,
	hashStop *chainhash.Hash,
	maxHashes uint32,
) ([]*model.BlockHeader, error) {
	args := m.Called(ctx, locator, hashStop, maxHashes)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*model.BlockHeader), nil
}

type fakeServer struct {
	blockchain_api.UnimplementedBlockchainAPIServer
	subCh chan *blockchain_api.Notification
}

func (f *fakeServer) HealthGRPC(ctx context.Context, req *emptypb.Empty) (*blockchain_api.HealthResponse, error) {
	return &blockchain_api.HealthResponse{
		Ok:        true,
		Details:   "fake server healthy",
		Timestamp: timestamppb.Now(),
	}, nil
}

func (f *fakeServer) Subscribe(req *blockchain_api.SubscribeRequest, stream blockchain_api.BlockchainAPI_SubscribeServer) error {
	for notif := range f.subCh {
		if err := stream.Send(notif); err != nil {
			return err
		}
	}
	return nil
}

type mockHealthClient struct {
	blockchain_api.BlockchainAPIClient
	resp *blockchain_api.HealthResponse
	err  error
}

func (m *mockHealthClient) HealthGRPC(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*blockchain_api.HealthResponse, error) {
	return m.resp, m.err
}

type mockBlockClient struct {
	blockchain_api.BlockchainAPIClient
	responseGetBlock                             *blockchain_api.GetBlockResponse
	responseGetBlocks                            *blockchain_api.GetBlocksResponse
	responseGetBlockByHeight                     *blockchain_api.GetBlockResponse
	responseGetBlockByID                         *blockchain_api.GetBlockResponse
	responseGetNextBlockID                       *blockchain_api.GetNextBlockIDResponse
	responseGetBlockStats                        *model.BlockStats
	responseGetBlockGraphData                    *model.BlockDataPoints
	lastGetBlockGraphDataReq                     *blockchain_api.GetBlockGraphDataRequest
	responseGetLastNBlocks                       *blockchain_api.GetLastNBlocksResponse
	lastGetLastNBlocksReq                        *blockchain_api.GetLastNBlocksRequest
	responseGetLastNInvalidBlocks                *blockchain_api.GetLastNInvalidBlocksResponse
	lastGetLastNInvalidBlocksReq                 *blockchain_api.GetLastNInvalidBlocksRequest
	responseGetSuitableBlock                     *blockchain_api.GetSuitableBlockResponse
	lastGetSuitableBlockReq                      *blockchain_api.GetSuitableBlockRequest
	responseGetHashOfAncestorBlock               *blockchain_api.GetHashOfAncestorBlockResponse
	lastGetHashOfAncestorBlockReq                *blockchain_api.GetHashOfAncestorBlockRequest
	responseGetLatestBlockHeaderFromBlockLocator *blockchain_api.GetBlockHeaderResponse
	lastGetLatestBlockHeaderFromBlockLocatorReq  *blockchain_api.GetLatestBlockHeaderFromBlockLocatorRequest
	responseGetBlockHeadersFromOldest            *blockchain_api.GetBlockHeadersResponse
	lastGetBlockHeadersFromOldestReq             *blockchain_api.GetBlockHeadersFromOldestRequest
	responseGetNextWorkRequired                  *blockchain_api.GetNextWorkRequiredResponse
	lastGetNextWorkRequiredReq                   *blockchain_api.GetNextWorkRequiredRequest
	responseGetBlockExists                       *blockchain_api.GetBlockExistsResponse
	lastGetBlockExistsReq                        *blockchain_api.GetBlockRequest
	responseGetBestBlockHeader                   *blockchain_api.GetBlockHeaderResponse
	responseCheckBlockIsInCurrentChain           *blockchain_api.CheckBlockIsCurrentChainResponse
	lastCheckBlockIsInCurrentChainReq            *blockchain_api.CheckBlockIsCurrentChainRequest
	responseGetChainTips                         *blockchain_api.GetChainTipsResponse
	responseGetBlockHeader                       *blockchain_api.GetBlockHeaderResponse
	lastGetBlockHeaderReq                        *blockchain_api.GetBlockHeaderRequest
	responseGetBlockHeaders                      *blockchain_api.GetBlockHeadersResponse
	lastGetBlockHeadersReq                       *blockchain_api.GetBlockHeadersRequest
	responseGetBlockHeadersToCommonAncestor      *blockchain_api.GetBlockHeadersResponse
	lastGetBlockHeadersToCommonAncestorReq       *blockchain_api.GetBlockHeadersToCommonAncestorRequest
	responseGetBlockHeadersFromCommonAncestor    *blockchain_api.GetBlockHeadersResponse
	lastGetBlockHeadersFromCommonAncestorReq     *blockchain_api.GetBlockHeadersFromCommonAncestorRequest
	responseGetBlockHeadersFromTill              *blockchain_api.GetBlockHeadersResponse
	lastGetBlockHeadersFromTillReq               *blockchain_api.GetBlockHeadersFromTillRequest
	responseGetBlockHeadersFromHeight            *blockchain_api.GetBlockHeadersFromHeightResponse
	lastGetBlockHeadersFromHeightReq             *blockchain_api.GetBlockHeadersFromHeightRequest
	responseGetBlockHeadersByHeight              *blockchain_api.GetBlockHeadersByHeightResponse
	lastGetBlockHeadersByHeightReq               *blockchain_api.GetBlockHeadersByHeightRequest
	responseInvalidateBlock                      *blockchain_api.InvalidateBlockResponse
	lastInvalidateBlockReq                       *blockchain_api.InvalidateBlockRequest
	responseRevalidateBlock                      *emptypb.Empty
	lastRevalidateBlockReq                       *blockchain_api.RevalidateBlockRequest
	responseGetBlockHeaderIDs                    *blockchain_api.GetBlockHeaderIDsResponse
	lastGetBlockHeaderIDsReq                     *blockchain_api.GetBlockHeadersRequest
	responseSendNotification                     *emptypb.Empty
	lastSendNotificationReq                      *blockchain_api.Notification
	responseGetState                             *blockchain_api.StateResponse
	lastGetStateReq                              *blockchain_api.GetStateRequest
	responseSetState                             *emptypb.Empty
	lastSetStateReq                              *blockchain_api.SetStateRequest
	responseGetBlockIsMined                      *blockchain_api.GetBlockIsMinedResponse
	lastGetBlockIsMinedReq                       *blockchain_api.GetBlockIsMinedRequest
	responseSetBlockMinedSet                     *emptypb.Empty
	lastSetBlockMinedSetReq                      *blockchain_api.SetBlockMinedSetRequest
	responseGetBlocksMinedNotSet                 *blockchain_api.GetBlocksMinedNotSetResponse
	responseSetBlockProcessedAt                  *emptypb.Empty
	lastSetBlockProcessedAtReq                   *blockchain_api.SetBlockProcessedAtRequest
	responseSetBlockSubtreesSet                  *emptypb.Empty
	lastSetBlockSubtreesSetReq                   *blockchain_api.SetBlockSubtreesSetRequest
	responseGetBlocksSubtreesNotSet              *blockchain_api.GetBlocksSubtreesNotSetResponse
	responseGetFSMCurrentState                   *blockchain_api.GetFSMStateResponse
	responseSendFSMEvent                         *blockchain_api.GetFSMStateResponse
	lastSendFSMEventReq                          *blockchain_api.SendFSMEventRequest
	responseGetBlockLocator                      *blockchain_api.GetBlockLocatorResponse
	lastGetBlockLocatorReq                       *blockchain_api.GetBlockLocatorRequest
	responseLocateBlockHeaders                   *blockchain_api.LocateBlockHeadersResponse
	lastLocateBlockHeadersReq                    *blockchain_api.LocateBlockHeadersRequest
	responseGetBestHeightAndTime                 *blockchain_api.GetBestHeightAndTimeResponse
	err                                          error
}

func (m *mockBlockClient) AddBlock(ctx context.Context, in *blockchain_api.AddBlockRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &emptypb.Empty{}, nil
}

func (m *mockBlockClient) GetBlock(ctx context.Context, req *blockchain_api.GetBlockRequest, opts ...grpc.CallOption) (*blockchain_api.GetBlockResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.responseGetBlock, nil
}

func (m *mockBlockClient) GetBlocks(ctx context.Context, req *blockchain_api.GetBlocksRequest, opts ...grpc.CallOption) (*blockchain_api.GetBlocksResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.responseGetBlocks, nil
}

func (m *mockBlockClient) GetBlockByHeight(ctx context.Context, req *blockchain_api.GetBlockByHeightRequest, opts ...grpc.CallOption) (*blockchain_api.GetBlockResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.responseGetBlockByHeight, nil
}

func (m *mockBlockClient) GetBlockByID(ctx context.Context, req *blockchain_api.GetBlockByIDRequest, opts ...grpc.CallOption) (*blockchain_api.GetBlockResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.responseGetBlockByID, nil
}

func (m *mockBlockClient) GetNextBlockID(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*blockchain_api.GetNextBlockIDResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.responseGetNextBlockID, nil
}

func (m *mockBlockClient) GetBlockStats(
	ctx context.Context,
	in *emptypb.Empty,
	opts ...grpc.CallOption,
) (*model.BlockStats, error) {
	return m.responseGetBlockStats, m.err
}

func (m *mockBlockClient) GetBlockGraphData(
	ctx context.Context,
	in *blockchain_api.GetBlockGraphDataRequest,
	opts ...grpc.CallOption,
) (*model.BlockDataPoints, error) {
	m.lastGetBlockGraphDataReq = in
	return m.responseGetBlockGraphData, m.err
}

func (m *mockBlockClient) GetLastNBlocks(
	ctx context.Context,
	in *blockchain_api.GetLastNBlocksRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetLastNBlocksResponse, error) {
	m.lastGetLastNBlocksReq = in
	return m.responseGetLastNBlocks, m.err
}

func (m *mockBlockClient) GetLastNInvalidBlocks(
	ctx context.Context,
	in *blockchain_api.GetLastNInvalidBlocksRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetLastNInvalidBlocksResponse, error) {
	m.lastGetLastNInvalidBlocksReq = in
	return m.responseGetLastNInvalidBlocks, m.err
}

func (m *mockBlockClient) GetSuitableBlock(
	ctx context.Context,
	in *blockchain_api.GetSuitableBlockRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetSuitableBlockResponse, error) {
	m.lastGetSuitableBlockReq = in
	return m.responseGetSuitableBlock, m.err
}

func (m *mockBlockClient) GetHashOfAncestorBlock(
	ctx context.Context,
	in *blockchain_api.GetHashOfAncestorBlockRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetHashOfAncestorBlockResponse, error) {
	m.lastGetHashOfAncestorBlockReq = in
	return m.responseGetHashOfAncestorBlock, m.err
}

func (m *mockBlockClient) GetLatestBlockHeaderFromBlockLocator(
	ctx context.Context,
	in *blockchain_api.GetLatestBlockHeaderFromBlockLocatorRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetBlockHeaderResponse, error) {
	m.lastGetLatestBlockHeaderFromBlockLocatorReq = in
	return m.responseGetLatestBlockHeaderFromBlockLocator, m.err
}

func (m *mockBlockClient) GetBlockHeadersFromOldest(
	ctx context.Context,
	in *blockchain_api.GetBlockHeadersFromOldestRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetBlockHeadersResponse, error) {
	m.lastGetBlockHeadersFromOldestReq = in
	return m.responseGetBlockHeadersFromOldest, m.err
}

func (m *mockBlockClient) GetNextWorkRequired(
	ctx context.Context,
	in *blockchain_api.GetNextWorkRequiredRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetNextWorkRequiredResponse, error) {
	m.lastGetNextWorkRequiredReq = in
	return m.responseGetNextWorkRequired, m.err
}

func (m *mockBlockClient) GetBlockExists(
	ctx context.Context,
	in *blockchain_api.GetBlockRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetBlockExistsResponse, error) {
	m.lastGetBlockExistsReq = in
	return m.responseGetBlockExists, m.err
}

func (m *mockBlockClient) GetBestBlockHeader(
	ctx context.Context,
	in *emptypb.Empty,
	opts ...grpc.CallOption,
) (*blockchain_api.GetBlockHeaderResponse, error) {
	return m.responseGetBestBlockHeader, m.err
}

func (m *mockBlockClient) CheckBlockIsInCurrentChain(
	ctx context.Context,
	in *blockchain_api.CheckBlockIsCurrentChainRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.CheckBlockIsCurrentChainResponse, error) {
	m.lastCheckBlockIsInCurrentChainReq = in
	return m.responseCheckBlockIsInCurrentChain, m.err
}

func (m *mockBlockClient) GetChainTips(
	ctx context.Context,
	in *emptypb.Empty,
	opts ...grpc.CallOption,
) (*blockchain_api.GetChainTipsResponse, error) {
	return m.responseGetChainTips, m.err
}

func (m *mockBlockClient) GetBlockHeader(
	ctx context.Context,
	in *blockchain_api.GetBlockHeaderRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetBlockHeaderResponse, error) {
	m.lastGetBlockHeaderReq = in
	return m.responseGetBlockHeader, m.err
}

func (m *mockBlockClient) GetBlockHeaders(
	ctx context.Context,
	in *blockchain_api.GetBlockHeadersRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetBlockHeadersResponse, error) {
	m.lastGetBlockHeadersReq = in
	return m.responseGetBlockHeaders, m.err
}

func (m *mockBlockClient) GetBlockHeadersToCommonAncestor(
	ctx context.Context,
	in *blockchain_api.GetBlockHeadersToCommonAncestorRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetBlockHeadersResponse, error) {
	m.lastGetBlockHeadersToCommonAncestorReq = in
	return m.responseGetBlockHeadersToCommonAncestor, m.err
}

func (m *mockBlockClient) GetBlockHeadersFromCommonAncestor(
	ctx context.Context,
	in *blockchain_api.GetBlockHeadersFromCommonAncestorRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetBlockHeadersResponse, error) {
	m.lastGetBlockHeadersFromCommonAncestorReq = in
	return m.responseGetBlockHeadersFromCommonAncestor, m.err
}

func (m *mockBlockClient) GetBlockHeadersFromTill(
	ctx context.Context,
	in *blockchain_api.GetBlockHeadersFromTillRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetBlockHeadersResponse, error) {
	m.lastGetBlockHeadersFromTillReq = in
	return m.responseGetBlockHeadersFromTill, m.err
}

func (m *mockBlockClient) GetBlockHeadersFromHeight(
	ctx context.Context,
	in *blockchain_api.GetBlockHeadersFromHeightRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetBlockHeadersFromHeightResponse, error) {
	m.lastGetBlockHeadersFromHeightReq = in
	return m.responseGetBlockHeadersFromHeight, m.err
}

func (m *mockBlockClient) GetBlockHeadersByHeight(
	ctx context.Context,
	in *blockchain_api.GetBlockHeadersByHeightRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetBlockHeadersByHeightResponse, error) {
	m.lastGetBlockHeadersByHeightReq = in
	return m.responseGetBlockHeadersByHeight, m.err
}

func (m *mockBlockClient) InvalidateBlock(
	ctx context.Context,
	in *blockchain_api.InvalidateBlockRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.InvalidateBlockResponse, error) {
	m.lastInvalidateBlockReq = in
	return m.responseInvalidateBlock, m.err
}

func (m *mockBlockClient) RevalidateBlock(
	ctx context.Context,
	in *blockchain_api.RevalidateBlockRequest,
	opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	m.lastRevalidateBlockReq = in
	return m.responseRevalidateBlock, m.err
}

func (m *mockBlockClient) GetBlockHeaderIDs(
	ctx context.Context,
	in *blockchain_api.GetBlockHeadersRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetBlockHeaderIDsResponse, error) {
	m.lastGetBlockHeaderIDsReq = in
	return m.responseGetBlockHeaderIDs, m.err
}

func (m *mockBlockClient) SendNotification(
	ctx context.Context,
	in *blockchain_api.Notification,
	opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	m.lastSendNotificationReq = in
	return m.responseSendNotification, m.err
}

func (m *mockBlockClient) GetState(
	ctx context.Context,
	in *blockchain_api.GetStateRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.StateResponse, error) {
	m.lastGetStateReq = in
	return m.responseGetState, m.err
}

func (m *mockBlockClient) SetState(
	ctx context.Context,
	in *blockchain_api.SetStateRequest,
	opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	m.lastSetStateReq = in
	return m.responseSetState, m.err
}

func (m *mockBlockClient) GetBlockIsMined(
	ctx context.Context,
	in *blockchain_api.GetBlockIsMinedRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetBlockIsMinedResponse, error) {
	m.lastGetBlockIsMinedReq = in
	return m.responseGetBlockIsMined, m.err
}

func (m *mockBlockClient) SetBlockMinedSet(
	ctx context.Context,
	in *blockchain_api.SetBlockMinedSetRequest,
	opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	m.lastSetBlockMinedSetReq = in
	return m.responseSetBlockMinedSet, m.err
}

func (m *mockBlockClient) GetBlocksMinedNotSet(
	ctx context.Context,
	in *emptypb.Empty,
	opts ...grpc.CallOption,
) (*blockchain_api.GetBlocksMinedNotSetResponse, error) {
	return m.responseGetBlocksMinedNotSet, m.err
}

func (m *mockBlockClient) SetBlockProcessedAt(
	ctx context.Context,
	in *blockchain_api.SetBlockProcessedAtRequest,
	opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	m.lastSetBlockProcessedAtReq = in
	return m.responseSetBlockProcessedAt, m.err
}

func (m *mockBlockClient) SetBlockSubtreesSet(
	ctx context.Context,
	in *blockchain_api.SetBlockSubtreesSetRequest,
	opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	m.lastSetBlockSubtreesSetReq = in
	return m.responseSetBlockSubtreesSet, m.err
}

func (m *mockBlockClient) GetBlocksSubtreesNotSet(
	ctx context.Context,
	in *emptypb.Empty,
	opts ...grpc.CallOption,
) (*blockchain_api.GetBlocksSubtreesNotSetResponse, error) {
	return m.responseGetBlocksSubtreesNotSet, m.err
}

func (m *mockBlockClient) GetFSMCurrentState(
	ctx context.Context,
	in *emptypb.Empty,
	opts ...grpc.CallOption,
) (*blockchain_api.GetFSMStateResponse, error) {
	return m.responseGetFSMCurrentState, m.err
}

func (m *mockBlockClient) SendFSMEvent(
	ctx context.Context,
	in *blockchain_api.SendFSMEventRequest,
	opts ...grpc.CallOption,
) (*blockchain_api.GetFSMStateResponse, error) {
	m.lastSendFSMEventReq = in
	return m.responseSendFSMEvent, m.err
}

// Lifecycle methods
func (m *mockBlockClient) Run(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, m.err
}

func (m *mockBlockClient) CatchUpBlocks(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, m.err
}

func (m *mockBlockClient) LegacySync(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, m.err
}

func (m *mockBlockClient) Idle(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, m.err
}
func (m *mockBlockClient) GetBlockLocator(ctx context.Context, req *blockchain_api.GetBlockLocatorRequest, opts ...grpc.CallOption) (*blockchain_api.GetBlockLocatorResponse, error) {
	m.lastGetBlockLocatorReq = req
	return m.responseGetBlockLocator, m.err
}
func (m *mockBlockClient) LocateBlockHeaders(ctx context.Context, req *blockchain_api.LocateBlockHeadersRequest, opts ...grpc.CallOption) (*blockchain_api.LocateBlockHeadersResponse, error) {
	m.lastLocateBlockHeadersReq = req
	return m.responseLocateBlockHeaders, m.err
}
func (m *mockBlockClient) GetBestHeightAndTime(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*blockchain_api.GetBestHeightAndTimeResponse, error) {
	return m.responseGetBestHeightAndTime, m.err
}

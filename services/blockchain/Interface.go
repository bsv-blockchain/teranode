package blockchain

import (
	"context"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/libsv/go-bt/v2/chainhash"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ClientI interface {
	Health(ctx context.Context) (*blockchain_api.HealthResponse, error)
	AddBlock(ctx context.Context, block *model.Block, peerID string) error
	SendNotification(ctx context.Context, notification *model.Notification) error
	GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error)
	GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error)
	GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error)
	GetBlockStats(ctx context.Context) (*model.BlockStats, error)
	GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error)
	GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error)
	GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error)
	GetHashOfAncestorBlock(ctx context.Context, hash *chainhash.Hash, depth int) (*chainhash.Hash, error)
	GetNextWorkRequired(ctx context.Context, hash *chainhash.Hash) (*model.NBit, error)
	GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error)
	GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error)
	GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error)
	GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error
	RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error
	GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error)
	Subscribe(ctx context.Context, source string) (chan *model.Notification, error)
	GetState(ctx context.Context, key string) ([]byte, error)
	SetState(ctx context.Context, key string, data []byte) error
	SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error
	GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error)
	SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error
	GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error)
	GetFSMCurrentState(ctx context.Context) (*blockchain_api.FSMStateType, error)
	SendFSMEvent(ctx context.Context, state blockchain_api.FSMEventType) error

	// new legacy endpoints
	GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, blockHeaderHeight uint32) ([]*chainhash.Hash, error)
}

var _ ClientI = &MockBlockchain{}

type MockBlockchain struct {
	block *model.Block
}

// --------------------------------------------
// mockBlockchain
// --------------------------------------------
func (s *MockBlockchain) Health(ctx context.Context) (*blockchain_api.HealthResponse, error) {
	return &blockchain_api.HealthResponse{
		Ok:        true,
		Details:   "Mock Blockchain",
		Timestamp: timestamppb.Now(),
	}, nil
}
func (s *MockBlockchain) AddBlock(ctx context.Context, block *model.Block, peerID string) error {
	s.block = block
	return nil
}
func (s *MockBlockchain) SendNotification(ctx context.Context, notification *model.Notification) error {
	return nil
}
func (s *MockBlockchain) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	return s.block, nil
}
func (s *MockBlockchain) GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error) {
	return []*model.Block{s.block}, nil
}
func (s *MockBlockchain) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	return s.block, nil
}
func (s *MockBlockchain) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	return &model.BlockStats{
		BlockCount:         1,
		TxCount:            s.block.TransactionCount,
		MaxHeight:          uint64(s.block.Height),
		AvgBlockSize:       float64(s.block.SizeInBytes),
		AvgTxCountPerBlock: float64(s.block.TransactionCount),
		FirstBlockTime:     s.block.Header.Timestamp,
		LastBlockTime:      s.block.Header.Timestamp,
	}, nil
}
func (s *MockBlockchain) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	panic("not implemented")
}
func (s *MockBlockchain) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	panic("not implemented")
}
func (s *MockBlockchain) GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error) {
	panic("not implemented")
}
func (s *MockBlockchain) GetHashOfAncestorBlock(ctx context.Context, hash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	panic("not implemented")
}
func (s *MockBlockchain) GetNextWorkRequired(ctx context.Context, hash *chainhash.Hash) (*model.NBit, error) {
	panic("not implemented")
}
func (s *MockBlockchain) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	if s.block == nil {
		return false, nil
	}

	return *s.block.Hash() == *blockHash, nil
}
func (s *MockBlockchain) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return s.block.Header, nil, nil
}
func (s *MockBlockchain) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return s.block.Header, nil, nil
}
func (s *MockBlockchain) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return []*model.BlockHeader{s.block.Header}, nil, nil
}
func (s *MockBlockchain) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return []*model.BlockHeader{s.block.Header}, nil, nil
}
func (s *MockBlockchain) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	return nil
}
func (s *MockBlockchain) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	return nil
}
func (s *MockBlockchain) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	return []uint32{0}, nil
}
func (s *MockBlockchain) Subscribe(ctx context.Context, source string) (chan *model.Notification, error) {
	return nil, nil
}
func (s *MockBlockchain) GetState(ctx context.Context, key string) ([]byte, error) {
	panic("not implemented")
}
func (s *MockBlockchain) SetState(ctx context.Context, key string, data []byte) error {
	panic("not implemented")
}
func (s *MockBlockchain) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	panic("not implemented")
}
func (s *MockBlockchain) GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error) {
	panic("not implemented")
}
func (s *MockBlockchain) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	panic("not implemented")
}
func (s *MockBlockchain) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	panic("not implemented")
}
func (s *MockBlockchain) GetFSMCurrentState(ctx context.Context) (*blockchain_api.FSMStateType, error) {
	panic("not implemented")
}
func (s *MockBlockchain) SendFSMEvent(ctx context.Context, state blockchain_api.FSMEventType) error {
	panic("not implemented")
}
func (s *MockBlockchain) GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, blockHeaderHeight uint32) ([]*chainhash.Hash, error) {
	panic("not implemented")
}
func (s *MockBlockchain) HeightToHashRange(startHeight uint32, endHash *chainhash.Hash, maxResults int) ([]chainhash.Hash, error) {
	panic("not implemented")
}
func (s *MockBlockchain) IntervalBlockHashes(endHash *chainhash.Hash, interval int) ([]chainhash.Hash, error) {
	panic("not implemented")
}

// Package blockchain provides functionality for managing the Bitcoin blockchain.
package blockchain

import (
	"context"
	"net/http"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

// ClientI defines the interface for blockchain client operations.
type ClientI interface {
	// Health checks the health status of the blockchain service.
	Health(ctx context.Context, checkLiveness bool) (int, string, error)

	// AddBlock adds a new block to the blockchain.
	AddBlock(ctx context.Context, block *model.Block, peerID string) error

	// SendNotification broadcasts a notification to subscribers.
	SendNotification(ctx context.Context, notification *blockchain_api.Notification) error

	// GetBlock retrieves a block by its hash.
	GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error)

	// GetBlocks retrieves multiple blocks starting from a specific hash.
	GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error)

	// GetBlockByHeight retrieves a block at a specific height.
	GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error)

	// GetBlockByID retrieves a block by its ID.
	GetBlockByID(ctx context.Context, id uint64) (*model.Block, error)

	// GetBlockStats retrieves statistical information about the blockchain.
	GetBlockStats(ctx context.Context) (*model.BlockStats, error)

	// GetBlockGraphData retrieves data points for blockchain visualization.
	GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error)

	// GetLastNBlocks retrieves the most recent N blocks.
	GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error)

	// GetSuitableBlock finds a suitable block for mining purposes.
	GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error)

	// GetHashOfAncestorBlock retrieves the hash of an ancestor block at a specific depth.
	GetHashOfAncestorBlock(ctx context.Context, hash *chainhash.Hash, depth int) (*chainhash.Hash, error)

	// GetNextWorkRequired calculates the required proof of work for the next block.
	GetNextWorkRequired(ctx context.Context, hash *chainhash.Hash) (*model.NBit, error)

	// GetBlockExists checks if a block exists in the blockchain.
	GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error)

	// GetBestBlockHeader retrieves the current best block header.
	GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error)

	// GetBlockHeader retrieves a specific block header.
	GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error)

	// GetBlockHeaders retrieves multiple block headers.
	GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// GetBlockHeadersFromTill retrieves block headers between two blocks.
	GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// GetBlockHeadersFromHeight retrieves block headers from a specific height.
	GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// GetBlockHeadersByHeight retrieves block headers between two heights.
	GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// InvalidateBlock marks a block as invalid.
	InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error

	// RevalidateBlock restores a previously invalidated block.
	RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error

	// GetBlockHeaderIDs retrieves block header IDs.
	GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error)

	// Subscribe creates a subscription to blockchain notifications.
	Subscribe(ctx context.Context, source string) (chan *blockchain_api.Notification, error)

	// GetState retrieves state data by key.
	GetState(ctx context.Context, key string) ([]byte, error)

	// SetState stores state data with a key.
	SetState(ctx context.Context, key string, data []byte) error

	// SetBlockMinedSet marks a block as mined.
	SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error

	// GetBlockIsMined retrieves whether a block has been marked as mined
	GetBlockIsMined(ctx context.Context, blockHash *chainhash.Hash) (bool, error)

	// GetBlocksMinedNotSet retrieves blocks not marked as mined.
	GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error)

	// SetBlockSubtreesSet marks a block's subtrees as set.
	SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error

	// GetBlocksSubtreesNotSet retrieves blocks with unset subtrees.
	GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error)

	// GetBestHeightAndTime retrieves the current best height and time.
	GetBestHeightAndTime(ctx context.Context) (uint32, uint32, error)

	// CheckBlockIsInCurrentChain checks if blocks are in the current chain.
	CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error)

	// FSM related endpoints
	GetFSMCurrentState(ctx context.Context) (*FSMStateType, error)
	IsFSMCurrentState(ctx context.Context, state FSMStateType) (bool, error)
	WaitForFSMtoTransitionToGivenState(context.Context, FSMStateType) error
	WaitUntilFSMTransitionFromIdleState(ctx context.Context) error
	GetFSMCurrentStateForE2ETestMode() FSMStateType
	Run(ctx context.Context, source string) error
	CatchUpBlocks(ctx context.Context) error
	LegacySync(ctx context.Context) error
	Idle(ctx context.Context) error
	SendFSMEvent(ctx context.Context, event FSMEventType) error

	// Legacy endpoints
	GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, blockHeaderHeight uint32) ([]*chainhash.Hash, error)
	LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error)
}

// // LocateBlocks returns the hashes of the blocks after the first known Block in
// // the locator until the provided stop hash is reached, or up to the provided
// // max number of Block hashes.
// //
// // In addition, there are two special cases:
// //
// //   - When no locators are provided, the stop hash is treated as a request for
// //     that Block, so it will either return the stop hash itself if it is known,
// //     or nil if it is unknown
// //   - When locators are provided, but none of them are known, hashes starting
// //     after the genesis Block will be returned
// //
// // This function is safe for concurrent access.
// func (b *BlockChain) LocateBlocks(locator BlockLocator, hashStop *chainhash.Hash, maxHashes uint32) []chainhash.Hash {
//	b.chainLock.RLock()
//	hashes := b.locateBlocks(locator, hashStop, maxHashes)
//	b.chainLock.RUnlock()
//	return hashes
// }

var _ ClientI = &MockBlockchain{}

type MockBlockchain struct {
	Block        *model.Block
	CurrentState FSMStateType
}

// --------------------------------------------
// mockBlockchain
// --------------------------------------------
func (s *MockBlockchain) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return http.StatusOK, "OK", nil
}
func (s *MockBlockchain) AddBlock(ctx context.Context, block *model.Block, peerID string) error {
	s.Block = block
	return nil
}
func (s *MockBlockchain) SendNotification(ctx context.Context, notification *blockchain_api.Notification) error {
	return nil
}
func (s *MockBlockchain) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	return s.Block, nil
}
func (s *MockBlockchain) GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error) {
	return []*model.Block{s.Block}, nil
}
func (s *MockBlockchain) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	return s.Block, nil
}
func (s *MockBlockchain) GetBlockByID(ctx context.Context, id uint64) (*model.Block, error) {
	return s.Block, nil
}
func (s *MockBlockchain) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	return &model.BlockStats{
		BlockCount:         1,
		TxCount:            s.Block.TransactionCount,
		MaxHeight:          uint64(s.Block.Height),
		AvgBlockSize:       float64(s.Block.SizeInBytes),
		AvgTxCountPerBlock: float64(s.Block.TransactionCount),
		FirstBlockTime:     s.Block.Header.Timestamp,
		LastBlockTime:      s.Block.Header.Timestamp,
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
	if s.Block == nil {
		return false, nil
	}

	return *s.Block.Hash() == *blockHash, nil
}
func (s *MockBlockchain) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	timeUint32, err := util.SafeInt64ToUint32(time.Now().Unix())
	if err != nil {
		return nil, nil, err
	}

	return s.Block.Header, &model.BlockHeaderMeta{
		ID:          1,
		Height:      s.Block.Height,
		TxCount:     s.Block.TransactionCount,
		SizeInBytes: s.Block.SizeInBytes,
		Miner:       "test",
		BlockTime:   timeUint32,
		Timestamp:   timeUint32,
		ChainWork:   nil,
	}, nil
}
func (s *MockBlockchain) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	timeUint32, err := util.SafeInt64ToUint32(time.Now().Unix())
	if err != nil {
		return nil, nil, err
	}

	return s.Block.Header, &model.BlockHeaderMeta{
		ID:          1,
		Height:      s.Block.Height,
		TxCount:     s.Block.TransactionCount,
		SizeInBytes: s.Block.SizeInBytes,
		Miner:       "test",
		BlockTime:   timeUint32,
		Timestamp:   timeUint32,
		ChainWork:   nil,
	}, nil
}
func (s *MockBlockchain) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return []*model.BlockHeader{s.Block.Header}, nil, nil
}
func (s *MockBlockchain) GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return []*model.BlockHeader{s.Block.Header}, nil, nil
}
func (s *MockBlockchain) GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return []*model.BlockHeader{s.Block.Header}, nil, nil
}
func (s *MockBlockchain) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return []*model.BlockHeader{s.Block.Header}, nil, nil
}
func (s *MockBlockchain) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return []*model.BlockHeader{s.Block.Header}, nil, nil
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
func (s *MockBlockchain) Subscribe(ctx context.Context, source string) (chan *blockchain_api.Notification, error) {
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
func (s *MockBlockchain) GetBlockIsMined(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
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
func (s *MockBlockchain) GetFSMCurrentState(_ context.Context) (*blockchain_api.FSMStateType, error) {
	panic("not implemented")
}
func (s *MockBlockchain) IsFSMCurrentState(_ context.Context, state FSMStateType) (bool, error) {
	return s.CurrentState == state, nil
}
func (s *MockBlockchain) WaitForFSMtoTransitionToGivenState(_ context.Context, _ blockchain_api.FSMStateType) error {
	panic("not implemented")
}
func (s *MockBlockchain) WaitUntilFSMTransitionFromIdleState(_ context.Context) error {
	return nil
}
func (s *MockBlockchain) GetFSMCurrentStateForE2ETestMode() blockchain_api.FSMStateType {
	panic("not implemented")
}
func (s *MockBlockchain) Run(ctx context.Context, source string) error {
	panic("not implemented")
}
func (s *MockBlockchain) CatchUpBlocks(ctx context.Context) error {
	panic("not implemented")
}
func (s *MockBlockchain) LegacySync(ctx context.Context) error {
	panic("not implemented")
}
func (s *MockBlockchain) Idle(ctx context.Context) error {
	panic("not implemented")
}
func (s *MockBlockchain) SendFSMEvent(ctx context.Context, event FSMEventType) error {
	panic("not implemented")
}
func (s *MockBlockchain) GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, blockHeaderHeight uint32) ([]*chainhash.Hash, error) {
	panic("not implemented")
}
func (s *MockBlockchain) LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error) {
	panic("not implemented")
}
func (s *MockBlockchain) HeightToHashRange(startHeight uint32, endHash *chainhash.Hash, maxResults int) ([]chainhash.Hash, error) {
	panic("not implemented")
}
func (s *MockBlockchain) IntervalBlockHashes(endHash *chainhash.Hash, interval int) ([]chainhash.Hash, error) {
	panic("not implemented")
}
func (s *MockBlockchain) GetBestHeightAndTime(ctx context.Context) (uint32, uint32, error) {
	panic("implement me")
}
func (s *MockBlockchain) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	panic("implement me")
}

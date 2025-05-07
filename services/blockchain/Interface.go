// Package blockchain provides functionality for managing the Bitcoin blockchain.
package blockchain

import (
	"context"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
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

	// GetLastNInvalidBlocks retrieves the most recent N blocks that have been marked as invalid.
	GetLastNInvalidBlocks(ctx context.Context, n int64) ([]*model.BlockInfo, error)

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

	// SetBlockProcessedAt sets or clears the processed_at timestamp for a block.
	SetBlockProcessedAt(ctx context.Context, blockHash *chainhash.Hash, clear ...bool) error

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

const notImplemented = "not implemented"

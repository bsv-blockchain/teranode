package blockchain

import (
	"context"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/stores/blob/file"
	"github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/usql"
	"github.com/libsv/go-bt/v2/chainhash"
)

type Store interface {
	Health(ctx context.Context, checkLiveness bool) (int, string, error)
	GetDB() *usql.DB
	GetDBEngine() util.SQLEngine
	GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, error)
	GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint32, error)
	GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error)
	GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error)

	/*
	   GetBlockInChainByHeightHash returns a block by height for a chain determined by the start hash.
	   This is useful for getting the block at a given height in a chain that may have a different tip.
	*/
	GetBlockInChainByHeightHash(ctx context.Context, height uint32, startHash *chainhash.Hash) (*model.Block, bool, error)

	GetBlockStats(ctx context.Context) (*model.BlockStats, error)
	GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error)
	GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error)
	GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error)
	GetHashOfAncestorBlock(ctx context.Context, blockHash *chainhash.Hash, depth int) (*chainhash.Hash, error)
	GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error)
	GetBlockHeight(ctx context.Context, blockHash *chainhash.Hash) (uint32, error)
	StoreBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) (uint64, uint32, error)
	GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error)
	GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error)
	GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	GetForkedBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error
	RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error
	GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error)
	GetState(ctx context.Context, key string) ([]byte, error)
	SetState(ctx context.Context, key string, data []byte) error
	SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error
	GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error)
	SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error
	GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error)
	GetBlocksByTime(ctx context.Context, fromTime, toTime time.Time) ([][]byte, error)
	CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error)
	GetFSMState(ctx context.Context) (string, error)
	SetFSMState(ctx context.Context, state string) error

	// legacy endpoints
	LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error)
	ExportBlockDB(ctx context.Context, hash *chainhash.Hash) (*file.File, error)
}

package blockchain

import (
	"context"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/usql"
	"github.com/libsv/go-bt/v2/chainhash"
)

var (
	ErrNotFound = ubsverrors.New(ubsverrors.ErrorConstants_NOT_FOUND, "not found")

	// ErrBlockNotFound is returned when a block is not found
	ErrBlockNotFound = ubsverrors.New(ubsverrors.ErrorConstants_BLOCK_NOT_FOUND, "block not found")
)

type Store interface {
	GetDB() *usql.DB
	GetDBEngine() util.SQLEngine
	GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, error)
	GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint32, error)
	GetBlockStats(ctx context.Context) (*model.BlockStats, error)
	GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error)
	GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error)
	GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error)
	GetHashOfAncestorBlock(ctx context.Context, blockHash *chainhash.Hash, depth int) (*chainhash.Hash, error)
	GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error)
	GetBlockHeight(ctx context.Context, blockHash *chainhash.Hash) (uint32, error)
	StoreBlock(ctx context.Context, block *model.Block, peerID string) (uint64, error)
	GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error)
	GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error)
	GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []uint32, error)
	InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error
	GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error)
	GetState(ctx context.Context, key string) ([]byte, error)
	SetState(ctx context.Context, key string, data []byte) error
}

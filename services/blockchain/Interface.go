package blockchain

import (
	"context"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/libsv/go-bt/v2/chainhash"
)

type ClientI interface {
	Health(ctx context.Context) (*blockchain_api.HealthResponse, error)
	AddBlock(ctx context.Context, block *model.Block, peerID string) error
	SendNotification(ctx context.Context, notification *model.Notification) error
	GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error)
	GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error)
	GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error)
	GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error)
	GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error)
	GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []uint32, error)
	InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error
	GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error)
	Subscribe(ctx context.Context, source string) (chan *model.Notification, error)
	GetState(ctx context.Context, key string) ([]byte, error)
	SetState(ctx context.Context, key string, data []byte) error
}

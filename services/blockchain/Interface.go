package blockchain

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockchain/blockchain_api"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type ClientI interface {
	Health(ctx context.Context) (*blockchain_api.HealthResponse, error)
	AddBlock(ctx context.Context, block *model.Block) error
	GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error)
	GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, uint32, error)
	GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, error)
	SubscribeBestBlockHeader(ctx context.Context) (chan *model.BlockHeader, error)
}

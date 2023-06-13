package blockchain

import (
	"context"
	"errors"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

var (
	ErrNotFound = errors.New("not found")

	// ErrBlockNotFound is returned when a block is not found
	ErrBlockNotFound = errors.New("block not found")
)

type Store interface {
	GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, error)
	GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint64, error)
	GetBlockHeight(ctx context.Context, blockHash *chainhash.Hash) (uint64, error)
	StoreBlock(ctx context.Context, block *model.Block) error
	GetChainTip(ctx context.Context) (*model.BlockHeader, uint64, error) // blockHeader, blockHeight
	GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, error)
}

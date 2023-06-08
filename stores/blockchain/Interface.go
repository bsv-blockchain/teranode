package blockchain

import (
	"context"
	"errors"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/libsv/go-bc"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

var (
	ErrNotFound = errors.New("not found")

	// ErrBlockNotFound is returned when a block is not found
	ErrBlockNotFound = errors.New("block not found")
)

type Store interface {
	GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*bc.BlockHeader, error)
	GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error)
	StoreBlock(ctx context.Context, block *model.Block) error
	GetChainTip(ctx context.Context) (*bc.BlockHeader, uint64, error) // blockHeader, blockHeight
	GetDifficulty(ctx context.Context) (uint64, error)
}

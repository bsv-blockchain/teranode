package blockchain

import (
	"context"
	"database/sql"
	"errors"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

var (
	ErrNotFound = errors.New("not found")

	// ErrBlockNotFound is returned when a block is not found
	ErrBlockNotFound = errors.New("block not found")
)

type Store interface {
	GetDB() *sql.DB
	GetDBEngine() util.SQLEngine
	GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, error)
	GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint32, error)
	GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error)
	GetBlockHeight(ctx context.Context, blockHash *chainhash.Hash) (uint32, error)
	StoreBlock(ctx context.Context, block *model.Block) error
	GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, uint32, error) // blockHeader, blockHeight
	GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, error)
	GetState(ctx context.Context, key string) ([]byte, error)
	SetState(ctx context.Context, key string, data []byte) error
}

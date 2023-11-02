package txmeta

import (
	"context"
	"fmt"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

var (
	ErrNotFound      = fmt.Errorf("not found")
	ErrAlreadyExists = fmt.Errorf("already exists")
)

type Store interface {
	Get(ctx context.Context, hash *chainhash.Hash) (*Data, error)
	Create(ctx context.Context, tx *bt.Tx) (*Data, error)
	SetMined(ctx context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error
	Delete(ctx context.Context, hash *chainhash.Hash) error
}

package txmeta

import (
	"context"
	"fmt"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// Error functions
func ErrNotFound(key string) error {
	return fmt.Errorf("key '%s' not found", key)
}

func ErrAlreadyExists(key string) error {
	return fmt.Errorf("key '%s' already exists", key)
}

type Store interface {
	Get(ctx context.Context, hash *chainhash.Hash) (*Data, error)
	GetMeta(ctx context.Context, hash *chainhash.Hash) (*Data, error)
	Create(ctx context.Context, tx *bt.Tx) (*Data, error)
	SetMined(ctx context.Context, hash *chainhash.Hash, blockID uint32) error
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error
	Delete(ctx context.Context, hash *chainhash.Hash) error
}

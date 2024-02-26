package txmeta

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// Error functions
func NewErrTxmetaNotFound(key *chainhash.Hash) error {
	return ubsverrors.New(ubsverrors.ErrorConstants_NOT_FOUND, fmt.Sprintf("txmeta key %q", key.String()))
}

func NewErrTxmetaAlreadyExists(key *chainhash.Hash) error {
	return ubsverrors.New(ubsverrors.ErrorConstants_NOT_FOUND, fmt.Sprintf("txmeta key %q", key.String()))
}

type Store interface {
	Get(ctx context.Context, hash *chainhash.Hash) (*Data, error)
	GetMulti(ctx context.Context, hashes []*chainhash.Hash, fields ...string) (map[chainhash.Hash]*Data, error)
	GetMeta(ctx context.Context, hash *chainhash.Hash) (*Data, error)
	Create(ctx context.Context, tx *bt.Tx) (*Data, error)
	SetMined(ctx context.Context, hash *chainhash.Hash, blockID uint32) error
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error
	Delete(ctx context.Context, hash *chainhash.Hash) error
}

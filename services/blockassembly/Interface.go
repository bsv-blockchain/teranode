package blockassembly

import (
	"context"

	"github.com/libsv/go-bt/v2/chainhash"
)

type Store interface {
	Store(ctx context.Context, hash *chainhash.Hash) (bool, error)
}

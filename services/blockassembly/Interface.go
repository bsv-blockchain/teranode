package blockassembly

import (
	"context"

	"github.com/libsv/go-bt/v2/chainhash"
)

type Store interface {
	Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64, locktime uint32, utxoHashes []*chainhash.Hash, parentTxHashes []*chainhash.Hash) (bool, error)
	RemoveTx(ctx context.Context, hash *chainhash.Hash) error
}

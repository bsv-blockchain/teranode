package blockassembly

import (
	"context"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type Store interface {
	Store(ctx context.Context, txid *chainhash.Hash, fees uint64, utxoHashes []*chainhash.Hash) (bool, error)
}

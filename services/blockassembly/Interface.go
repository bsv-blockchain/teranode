package blockassembly

import (
	"context"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type Store interface {
	Store(ctx context.Context, txid *chainhash.Hash) (bool, error)
}

type SubTreeProcessor interface {
	AddTxID(txid *chainhash.Hash) error
}

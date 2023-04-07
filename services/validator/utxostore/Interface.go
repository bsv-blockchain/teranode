package utxostore

import (
	"context"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type UTXOResponse struct {
	Status int
}

type UTXOStore interface {
	Store(ctx context.Context, hash *chainhash.Hash) (*UTXOResponse, error)
	Spend(ctx context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*UTXOResponse, error)
	Reset(ctx context.Context, hash *chainhash.Hash) (*UTXOResponse, error)
}

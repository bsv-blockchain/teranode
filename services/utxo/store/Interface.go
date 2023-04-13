package store

import (
	"context"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type UTXOResponse struct {
	Status       int
	SpendingTxID *chainhash.Hash
}

type UTXOStore interface {
	Get(ctx context.Context, hash *chainhash.Hash) (*UTXOResponse, error)
	Store(ctx context.Context, hash *chainhash.Hash) (*UTXOResponse, error)
	Spend(ctx context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*UTXOResponse, error)
	Reset(ctx context.Context, hash *chainhash.Hash) (*UTXOResponse, error)
}

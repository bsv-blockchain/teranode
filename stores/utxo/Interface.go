package utxo

import (
	"context"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type UTXOResponse struct {
	Status       int
	SpendingTxID *chainhash.Hash
}

type BatchResponse struct {
	Status int
}

type Interface interface {
	Get(ctx context.Context, hash *chainhash.Hash) (*UTXOResponse, error)
	Store(ctx context.Context, hash *chainhash.Hash) (*UTXOResponse, error)
	BatchStore(ctx context.Context, hash []*chainhash.Hash) (*BatchResponse, error)
	Spend(ctx context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*UTXOResponse, error)
	Reset(ctx context.Context, hash *chainhash.Hash) (*UTXOResponse, error)
	DeleteSpends(deleteSpends bool)
}

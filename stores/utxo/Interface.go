package utxo

import (
	"context"

	"github.com/libsv/go-bt/v2/chainhash"
)

type UTXOResponse struct {
	Status       int             `json:"status"`
	SpendingTxID *chainhash.Hash `json:"spendingTxId,omitempty"`
	LockTime     uint32          `json:"lockTime,omitempty"`
}

type BatchResponse struct {
	Status int
}

type Interface interface {
	Health(ctx context.Context) (int, string, error)
	Get(ctx context.Context, hash *chainhash.Hash) (*UTXOResponse, error)
	Store(ctx context.Context, hash *chainhash.Hash, nLockTime uint32) (*UTXOResponse, error)
	BatchStore(ctx context.Context, hash []*chainhash.Hash) (*BatchResponse, error)
	Spend(ctx context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*UTXOResponse, error)
	Reset(ctx context.Context, hash *chainhash.Hash) (*UTXOResponse, error)
	Delete(ctx context.Context, hash *chainhash.Hash) (*UTXOResponse, error)
	DeleteSpends(deleteSpends bool)
	SetBlockHeight(height uint32) error
}

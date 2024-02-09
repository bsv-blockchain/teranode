package utxo

import (
	"context"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type Spend struct {
	TxID         *chainhash.Hash `json:"txId"`
	Vout         uint32          `json:"vout"`
	Hash         *chainhash.Hash `json:"hash"`
	SpendingTxID *chainhash.Hash `json:"spendingTxId,omitempty"`
}

type Response struct {
	Status       int             `json:"status"`
	SpendingTxID *chainhash.Hash `json:"spendingTxId,omitempty"`
	LockTime     uint32          `json:"lockTime,omitempty"`
}

type BatchResponse struct {
	Status int
}

type Interface interface {
	Health(ctx context.Context) (int, string, error)
	Get(ctx context.Context, spend *Spend) (*Response, error)
	Store(ctx context.Context, tx *bt.Tx, lockTime ...uint32) error
	StoreFromHashes(ctx context.Context, txID chainhash.Hash, utxoHashes []chainhash.Hash, lockTime uint32) error
	Spend(ctx context.Context, spend []*Spend) error
	UnSpend(ctx context.Context, spend []*Spend) error
	Delete(ctx context.Context, tx *bt.Tx) error
	DeleteSpends(deleteSpends bool)
	SetBlockHeight(height uint32) error
	GetBlockHeight() (uint32, error)
}

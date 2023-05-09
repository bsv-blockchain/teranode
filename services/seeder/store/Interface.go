package store

import (
	"context"

	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type SpendableTransaction struct {
	Txid              *chainhash.Hash
	NumberOfOutputs   int32
	SatoshisPerOutput int64
	PrivateKey        *bec.PrivateKey
}

type SeederStore interface {
	Push(context.Context, *SpendableTransaction) error
	Pop(context.Context) (*SpendableTransaction, error)
	PopWithFilter(context.Context, func(*SpendableTransaction) bool) (*SpendableTransaction, error)
	Iterator() Iterator
}

type Iterator interface {
	Next() (*SpendableTransaction, error)
}

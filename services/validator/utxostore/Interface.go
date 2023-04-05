package store

import (
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type UTXOResponse struct {
	status int
}

type UTXOStore interface {
	Store(hash *chainhash.Hash) (UTXOResponse, error)
	Spend(hash *chainhash.Hash, txID *chainhash.Hash) (UTXOResponse, error)
	Reset(hash *chainhash.Hash) (UTXOResponse, error)
}

package txmeta

import (
	"context"
	"fmt"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

var (
	ErrNotFound      = fmt.Errorf("not found")
	ErrAlreadyExists = fmt.Errorf("already exists")
)

// Data struct for the transaction metadata
// do not change order, has been optimized for size: https://golangprojectstructure.com/how-to-make-go-structs-more-efficient/
type Data struct {
	Tx             *bt.Tx            `json:"tx"`
	UtxoHashes     []*chainhash.Hash `json:"utxoHashes"`
	ParentTxHashes []*chainhash.Hash `json:"parentTxHashes"`
	BlockHashes    []*chainhash.Hash `json:"blockHashes"`
	Fee            uint64            `json:"fee"`
	SizeInBytes    uint64            `json:"sizeInBytes"`
	FirstSeen      uint32            `json:"firstSeen"`
	BlockHeight    uint32            `json:"blockHeight"`
}

type Store interface {
	Get(ctx context.Context, hash *chainhash.Hash) (*Data, error)
	Create(ctx context.Context, tx *bt.Tx) (*Data, error)
	SetMined(ctx context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error
	Delete(ctx context.Context, hash *chainhash.Hash) error
}

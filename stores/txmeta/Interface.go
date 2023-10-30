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

type TxStatus int

const (
	Unknown TxStatus = iota
	Validated
	UtxosCreated
	BlockAssembled
	Confirmed
)

func (s TxStatus) String() string {
	switch s {
	case Unknown:
		return "Unknown"
	case Validated:
		return "Validated"
	case UtxosCreated:
		return "UtxosCreated"
	case BlockAssembled:
		return "BlockAssembled"
	case Confirmed:
		return "Confirmed"
	default:
		return "Unknown"
	}
}

// Data struct for the transaction metadata
// do not change order, has been optimized for size: https://golangprojectstructure.com/how-to-make-go-structs-more-efficient/
type Data struct {
	Tx             *bt.Tx            `json:"tx"`
	UtxoHashes     []*chainhash.Hash `json:"utxoHashes"`
	ParentTxHashes []*chainhash.Hash `json:"parentTxHashes"`
	BlockHashes    []*chainhash.Hash `json:"blockHashes"`
	Fee            uint64            `json:"fee"`
	SizeInBytes    uint64            `json:"sizeInBytes"`
	Status         TxStatus          `json:"status"`
	FirstSeen      uint32            `json:"firstSeen"`
	BlockHeight    uint32            `json:"blockHeight"`
	LockTime       uint32            `json:"lockTime"`
}

type Store interface {
	Get(ctx context.Context, hash *chainhash.Hash) (*Data, error)
	Create(ctx context.Context, tx *bt.Tx, hash *chainhash.Hash, fee uint64, sizeInBytes uint64, parentTxHashes []*chainhash.Hash, utxoHashes []*chainhash.Hash, nLockTime uint32) error
	SetMined(ctx context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error
	Delete(ctx context.Context, hash *chainhash.Hash) error
}

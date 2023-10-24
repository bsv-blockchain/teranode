package txmeta

import (
	"context"
	"fmt"

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
	UtxoHashes     []*chainhash.Hash
	ParentTxHashes []*chainhash.Hash
	BlockHashes    []*chainhash.Hash
	Fee            uint64
	SizeInBytes    uint64
	Status         TxStatus
	FirstSeen      uint32
	BlockHeight    uint32
	LockTime       uint32
}

type Store interface {
	Get(ctx context.Context, hash *chainhash.Hash) (*Data, error)
	Create(ctx context.Context, hash *chainhash.Hash, fee uint64, sizeInBytes uint64, parentTxHashes []*chainhash.Hash, utxoHashes []*chainhash.Hash, nLockTime uint32) error
	SetMined(ctx context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error
	Delete(ctx context.Context, hash *chainhash.Hash) error
}

package txmeta

import (
	"context"
	"fmt"
	"time"

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

type Data struct {
	Status         TxStatus
	Fee            uint64
	UtxoHashes     []*chainhash.Hash
	ParentTxHashes []*chainhash.Hash
	FirstSeen      time.Time
	BlockHashes    []*chainhash.Hash
	BlockHeight    uint32 // delete after 100 blocks?
	LockTime       uint32 // The block number or timestamp at which this transaction is unlocked
}

type Store interface {
	Get(ctx context.Context, hash *chainhash.Hash) (*Data, error)
	Create(ctx context.Context, hash *chainhash.Hash, fee uint64, parentTxHashes []*chainhash.Hash, utxoHashes []*chainhash.Hash, nLockTime uint32) error
	SetMined(ctx context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error
	Delete(ctx context.Context, hash *chainhash.Hash) error
}

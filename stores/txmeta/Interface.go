package txmeta

import (
	"context"
	"fmt"
	"time"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
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

type Status struct {
	Status         TxStatus
	Fee            uint64
	UtxoHashes     []*chainhash.Hash
	ParentTxHashes []*chainhash.Hash
	FirstSeen      time.Time
	BlockHashes    []*chainhash.Hash
	BlockHeight    uint32 // delete after 100 blocks?
}

type Store interface {
	Get(ctx context.Context, hash *chainhash.Hash) (*Status, error)
	Create(ctx context.Context, hash *chainhash.Hash, fee uint64, parentTxHashes []*chainhash.Hash, utxoHashes []*chainhash.Hash) error
	SetMined(ctx context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error
	Delete(ctx context.Context, hash *chainhash.Hash) error
}

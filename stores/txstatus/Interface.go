package txstatus

import (
	"context"
	"fmt"
	"time"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type TxStatus int

const (
	Unknown     TxStatus = iota
	Unconfirmed          // 1
	Confirmed            // 2
)

var (
	ErrNotFound      = fmt.Errorf("not found")
	ErrAlreadyExists = fmt.Errorf("already exists")
)

type Status struct {
	Status      TxStatus
	Fee         uint64
	UtxoHashes  []*chainhash.Hash
	FirstSeen   time.Time
	BlockHashes []*chainhash.Hash
	BlockHeight uint32 // delete after 100 blocks?
}

type Store interface {
	Get(ctx context.Context, hash *chainhash.Hash) (*Status, error)
	Set(ctx context.Context, hash *chainhash.Hash, fee uint64, utxoHashes []*chainhash.Hash) error
	SetMined(ctx context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error
	Delete(ctx context.Context, hash *chainhash.Hash) error
}

func (s TxStatus) String() string {
	switch s {
	case Unconfirmed:
		return "Unconfirmed"
	case Confirmed:
		return "Confirmed"
	default:
		return "Unknown"
	}
}

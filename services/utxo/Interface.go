package utxo

import (
	"time"

	"github.com/libsv/go-bt/v2/chainhash"
)

type Entry struct {
	TimeSpent    time.Time
	SpendingTxid *chainhash.Hash
}

type UtxoStore interface {
	GetUTXO(xtxoHash *chainhash.Hash) (*Entry, error)
	AddNewUTXO(xtxoHash *chainhash.Hash) error
	SpendUTXO(xtxoHash *chainhash.Hash, spendingTxid *chainhash.Hash) error
	ResetUTXO(xtxoHash *chainhash.Hash) error
}

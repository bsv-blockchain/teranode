package utxostore

import (
	"time"
)

import github.com/Taal
type UtxoEntry struct {
	timeSpent    time.Time
	spendingTxid *chaincfg.Hash
}

type UtxoStore interface {
	GetUTXO(xtxoHash *chaincfg.Hash) (*UtxoEntry, error)
	AddNewUTXO(xtxoHash *chaincfg.Hash) error
	SpendUTXO(xtxoHash *chaincfg.Hash, spendingTxid *chaincfg.Hash) error
	ResetUTXO(xtxoHash *chaincfg.Hash) error
}

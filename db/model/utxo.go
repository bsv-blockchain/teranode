package model

import (
	"gorm.io/gorm"
)

type UTXO struct {
	gorm.Model
	Txid          string
	Vout          uint32
	LockingScript string
	Satoshis      uint64
	Address       string
	Spent         bool
	Reserved      bool // when identifying utxo candidates mark as reserved
	// until a transaction is either accepted or rejected
	// this field is set to true when utxo is selected as input
	// into a new transaction and controlled by the synchronization
	// go routine which monitors utxos, if the field Spent is still not set
	// after a certain amount of time, the Reserved field is set to false.
	// At this this utxo becomes spendable again
}

func (tx *UTXO) Equal(other *UTXO) bool {
	if tx == nil || other == nil {
		return false
	}
	return tx.Txid == other.Txid &&
		tx.Vout == other.Vout &&
		tx.LockingScript == other.LockingScript &&
		tx.Satoshis == other.Satoshis &&
		tx.Address == other.Address &&
		tx.Spent == other.Spent &&
		tx.Reserved == other.Reserved
}

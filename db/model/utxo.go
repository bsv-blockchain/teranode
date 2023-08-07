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
		tx.Spent == other.Spent
}

package model

import (
	"gorm.io/gorm"
)

type UTXO struct {
	gorm.Model
	Txid    string
	Vout    uint32
	Script  string
	Amount  uint64
	Address string
	Spent   bool
}

func (tx *UTXO) Equal(other *UTXO) bool {
	return tx.Txid == other.Txid &&
		tx.Vout == other.Vout &&
		tx.Script == other.Script &&
		tx.Amount == other.Amount &&
		tx.Address == other.Address &&
		tx.Spent == other.Spent
}

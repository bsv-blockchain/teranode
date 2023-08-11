package model

import (
	"gorm.io/gorm"
)

type UtxoStatus uint8

const (
	StatusLocked UtxoStatus = iota
	StatusSpendable
	StatusReserved
	StatusSpent
)

type UTXO struct {
	gorm.Model
	BlockHash     string
	Txid          string
	Vout          uint32
	LockingScript string
	Satoshis      uint64
	Address       string
	Status        UtxoStatus
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
		tx.Status == other.Status
}

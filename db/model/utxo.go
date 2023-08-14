package model

import (
	"fmt"

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

func (u *UTXO) String() string {
	return fmt.Sprintf("Txid:%s, Vout:%d, Satoshis:%d, Address:%s, Status:%d",
		u.Txid, u.Vout, u.Satoshis, u.Address, u.Status)
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

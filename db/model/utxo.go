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

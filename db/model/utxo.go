package model

import (
	"gorm.io/gorm"
)

type BlockStatus int

const (
	BlockStatus_SPENT BlockStatus = iota
	BlockStatus_SPENDABLE
	BlockStatus_LOCKED
)

type UTXO struct {
	gorm.Model
	Txid      string
	Vout      uint32
	Script    string
	Address   string
	Blockhash string
	Status    BlockStatus
}

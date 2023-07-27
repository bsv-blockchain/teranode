package model

import (
	"gorm.io/gorm"
)

type Block struct {
	gorm.Model
	Height        uint64
	Address       string
	BlockHash     string
	PrevBlockHash string
}

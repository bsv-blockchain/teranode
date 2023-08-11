package model

import (
	"gorm.io/gorm"
)

type Block struct {
	gorm.Model
	Height        uint64
	BlockHash     string `gorm:"unique"`
	PrevBlockHash string
}

func (b *Block) Equal(other *Block) bool {
	if b == nil || other == nil {
		return false
	}
	return b.Height == other.Height &&
		b.BlockHash == other.BlockHash &&
		b.PrevBlockHash == other.PrevBlockHash
}

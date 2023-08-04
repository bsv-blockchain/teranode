package model

import (
	"gorm.io/gorm"
)

type Block struct {
	gorm.Model
	Height        uint64
	BlockHash     string
	PrevBlockHash string
}

func (b *Block) Equal(other *Block) bool {
	return b.Height == other.Height &&
		b.BlockHash == other.BlockHash &&
		b.PrevBlockHash == other.PrevBlockHash
}

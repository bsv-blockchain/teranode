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

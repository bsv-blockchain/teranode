package test

import (
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"sync"

	"github.com/libsv/go-bt/v2/chainhash"
)

var (
	LoadMetaToMemoryOnce         sync.Once
	CachedTxMetaStore            utxo.Store
	FileDir                      string
	FileNameTemplate             string
	FileNameTemplateMerkleHashes string
	FileNameTemplateBlock        string
	TxMetafileNameTemplate       string
	GenerateNewTestData          bool
	SubtreeSize                  int
	TxCount                      uint64
)

type TestConfig struct {
	FileDir                      string
	FileNameTemplate             string
	FileNameTemplateMerkleHashes string
	FileNameTemplateBlock        string
	TxMetafileNameTemplate       string
	SubtreeSize                  int
	TxCount                      uint64
	GenerateNewTestData          bool
}

type FeeAndSize struct {
	hash        chainhash.Hash
	fee         uint64
	sizeInBytes uint64
}

type TestSubtreeStore struct {
	Files map[chainhash.Hash]int
}

type TestSubtrees struct {
	totalFees     uint64
	subtreeHashes []*chainhash.Hash
}

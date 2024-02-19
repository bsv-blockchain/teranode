package test

import (
	"sync"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/libsv/go-bt/v2/chainhash"
)

// const (
// 	subtreeSize = 1024 * 1024
// 	txIdCount   = 10 * 1024 * 1024
// )

// var (
// 	loadMetaToMemoryOnce sync.Once
// 	// cachedTxMetaStore is a global variable to cache the txMetaStore in memory, to avoid reading from disk more than once
// 	cachedTxMetaStore txmeta.Store
// 	// following variables are used to store the file names for the testdata
// 	fileDir                      string
// 	fileNameTemplate             string
// 	fileNameTemplateMerkleHashes string
// 	fileNameTemplateBlock        string
// 	txMetafileNameTemplate       string
// 	subtreeSize                  int // 1024 * 1024
// 	txIdCount                    int // 10 * 1024 * 1024
// )

var (
	LoadMetaToMemoryOnce         sync.Once
	CachedTxMetaStore            txmeta.Store
	FileDir                      string
	FileNameTemplate             string
	FileNameTemplateMerkleHashes string
	FileNameTemplateBlock        string
	TxMetafileNameTemplate       string
	SubtreeSize                  int
	TxIdCount                    int
)

type TestConfig struct {
	FileDir                      string
	FileNameTemplate             string
	FileNameTemplateMerkleHashes string
	FileNameTemplateBlock        string
	TxMetafileNameTemplate       string
	SubtreeSize                  int
	TxIdCount                    uint64
}

type FeeAndSize struct {
	hash        chainhash.Hash
	fee         uint64
	sizeInBytes uint64
}

type TestSubtreeStore struct {
	Files map[chainhash.Hash]int
}

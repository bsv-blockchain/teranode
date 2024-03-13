package test

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func Test_GenerateBlock(t *testing.T) {
	subtreeStore := NewLocalSubtreeStore()
	testConfig := &TestConfig{
		FileDir:                      "./test-generated_test_data/",
		FileNameTemplate:             "./test-generated_test_data/subtree-%d.bin",
		FileNameTemplateMerkleHashes: "./test-generated_test_data/subtree-merkle-hashes.bin",
		FileNameTemplateBlock:        "./test-generated_test_data/block.bin",
		TxMetafileNameTemplate:       "./test-generated_test_data/txMeta.bin",
		SubtreeSize:                  1024,
		TxCount:                      4 * 1024,
		GenerateNewTestData:          true,
	}
	block, err := GenerateTestBlock(subtreeStore, testConfig)
	require.NoError(t, err)
	require.NotEmpty(t, block)

	txMetaStore := memory.New(ulogger.TestLogger{}, true)
	CachedTxMetaStore = txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, txMetaStore, 1024)
	err = LoadTxMetaIntoMemory()
	require.NoError(t, err)

	// check if the first txid is in the txMetaStore
	reqTxId, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	data, err := CachedTxMetaStore.Get(context.Background(), reqTxId)
	require.NoError(t, err)
	require.Equal(t, &txmeta.Data{
		Fee:            1,
		SizeInBytes:    1,
		ParentTxHashes: []chainhash.Hash{},
	}, data)

	for idx, subtreeHash := range block.Subtrees {
		subtreeStore.Files[*subtreeHash] = idx
	}

	currentChain := make([]*model.BlockHeader, 11)
	currentChainIDs := make([]uint32, 11)
	for i := 0; i < 11; i++ {
		currentChain[i] = &model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			// set the last 11 block header timestamps to be less than the current timestamps
			Timestamp: 1231469665 - uint32(i),
		}
		currentChainIDs[i] = uint32(i)
	}
	currentChain[0].HashPrevBlock = &chainhash.Hash{}

	// check if the block is valid, we expect an error because of the duplicate transaction
	v, err := block.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, CachedTxMetaStore, nil, currentChain, currentChainIDs)
	require.NoError(t, err)
	require.True(t, v)
}

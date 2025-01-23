package test

import (
	"context"
	"sync"
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/stores/txmetacache"
	"github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestGenerateBlock(t *testing.T) {
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

	utxoStore := memory.New(ulogger.TestLogger{})
	CachedTxMetaStore, _ = txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, utxoStore, 1024)
	err = LoadTxMetaIntoMemory()
	require.NoError(t, err)

	// check if the first txid is in the txMetaStore
	reqTxID, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	data, err := CachedTxMetaStore.Get(context.Background(), reqTxID)
	require.NoError(t, err)
	require.Equal(t, &meta.Data{
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
			Timestamp: 1231469665 - uint32(i), // nolint:gosec
		}
		currentChainIDs[i] = uint32(i) // nolint:gosec
	}

	currentChain[0].HashPrevBlock = &chainhash.Hash{}

	// check if the block is valid, we expect an error because of the duplicate transaction
	oldBlockIDs := &sync.Map{}
	v, err := block.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, CachedTxMetaStore, oldBlockIDs, nil, currentChain, currentChainIDs, model.NewBloomStats())
	require.NoError(t, err)
	require.True(t, v)
}

//go:build test_all || test_model || test_bigblock || test_longlong

package model

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	ubsv_model "github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_bigblock ./test/...

func TestBlock_ValidBlockWithMultipleTransactions(t *testing.T) {
	ubsv_model.TestFileDir = "./test-generated_test_data/"
	ubsv_model.TestFileNameTemplate = ubsv_model.TestFileDir + "subtree-%d.bin"
	ubsv_model.TestFileNameTemplateMerkleHashes = ubsv_model.TestFileDir + "subtree-merkle-hashes.bin"
	ubsv_model.TestFileNameTemplateBlock = ubsv_model.TestFileDir + "block.bin"
	ubsv_model.TestTxMetafileNameTemplate = ubsv_model.TestFileDir + "txMeta.bin"
	subtreeStore := ubsv_model.NewLocalSubtreeStore()
	txCount := uint64(4 * 1024)
	ubsv_model.TestSubtreeSize = 1024

	block, err := ubsv_model.GenerateTestBlock(txCount, subtreeStore, true)
	require.NoError(t, err)

	txMetaStore := memory.New(ulogger.TestLogger{})
	ubsv_model.TestCachedTxMetaStore, _ = txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, txMetaStore, 1024)
	err = ubsv_model.LoadTxMetaIntoMemory()
	require.NoError(t, err)

	// check if the first txid is in the txMetaStore
	reqTxId, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	data, err := ubsv_model.TestCachedTxMetaStore.Get(context.Background(), reqTxId)
	require.NoError(t, err)
	require.Equal(t, &meta.Data{
		Fee:            1,
		SizeInBytes:    1,
		ParentTxHashes: []chainhash.Hash{},
	}, data)

	for idx, subtreeHash := range block.Subtrees {
		subtreeStore.Files[*subtreeHash] = idx
	}

	currentChain := make([]*ubsv_model.BlockHeader, 11)
	currentChainIDs := make([]uint32, 11)
	for i := 0; i < 11; i++ {
		currentChain[i] = &ubsv_model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			// set the last 11 block header timestamps to be less than the current timestamps
			Timestamp: 1231469665 - uint32(i),
		}
		currentChainIDs[i] = uint32(i)
	}
	currentChain[0].HashPrevBlock = &chainhash.Hash{}

	// check if the block is valid, we expect an error because of the duplicate transaction
	oldBlockIDs := &sync.Map{}
	v, err := block.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, ubsv_model.TestCachedTxMetaStore, oldBlockIDs, nil, currentChain, currentChainIDs, ubsv_model.NewBloomStats())
	require.NoError(t, err)
	require.True(t, v)

	_, hasTransactionsReferencingOldBlocks := util.ConvertSyncMapToUint32Slice(oldBlockIDs)
	require.False(t, hasTransactionsReferencingOldBlocks)
}

func TestBlock_WithDuplicateTransaction(t *testing.T) {
	leafCount := 8
	subtree, err := util.NewTreeByLeafCount(leafCount)
	require.NoError(t, err)
	ubsv_model.TestSubtreeSize = 8

	subtreeStore := ubsv_model.NewLocalSubtreeStore()
	txMetaStore := memory.New(ulogger.TestLogger{})
	ubsv_model.TestCachedTxMetaStore, _ = txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, txMetaStore, 1024)
	txMetaCache := ubsv_model.TestCachedTxMetaStore.(*txmetacache.TxMetaCache)

	// create a slice of random hashes, for the leaves
	hashes := make([]*chainhash.Hash, leafCount)
	for i := 0; i < leafCount-2; i++ {
		// create random 32 bytes
		bytes := make([]byte, 32)
		_, _ = rand.Read(bytes)
		hashes[i], _ = chainhash.NewHash(bytes)
	}

	// first transaction is the coinbase transaction
	_ = subtree.AddCoinbaseNode()

	// rest of transactions are random
	for i := 0; i < leafCount-2; i++ {
		_ = subtree.AddNode(*hashes[i], 111, 0)
		err = txMetaCache.SetCache(hashes[i], &meta.Data{Fee: 111, SizeInBytes: 1})
		require.NoError(t, err)
	}

	// last transaction is a duplicate of the previous transaction
	_ = subtree.AddNode(*hashes[leafCount-3], 111, 0)

	// serialize the subtree
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)

	// create a coinbase transaction
	coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
	coinbase, err := bt.NewTxFromString(coinbaseHex)
	require.NoError(t, err)

	// add a P2PKH output to the coinbase transaction with fees
	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", util.GetBlockSubsidyForHeight(1)+subtree.Fees)
	nBits, _ := ubsv_model.NewNBitFromString("2000ffff")

	// get subtree root hash
	subtreeHash := subtree.RootHash()

	// store subtree hash in the subtreeStore map
	subtreeStore.Files[*subtreeHash] = 0

	// create a new subtree for replaced coinbase transaction
	replacedCoinbaseSubtree, err := util.NewTreeByLeafCount(ubsv_model.TestSubtreeSize)
	require.NoError(t, err)

	// deserialize the replaced coinbase subtree
	err = replacedCoinbaseSubtree.Deserialize(subtreeBytes)
	require.NoError(t, err)

	// replace the root node with the coinbase transaction
	replacedCoinbaseSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))

	// calculate the root hash of the replaced coinbase subtree
	rootHash := replacedCoinbaseSubtree.RootHash()

	// create a block header with the replaced coinbase subtree root hash
	blockHeader := &ubsv_model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: rootHash,
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           *nBits,
		Nonce:          0,
	}

	// mine block header to the target difficulty
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}
		blockHeader.Nonce++

		if blockHeader.Nonce%1000000 == 0 {
			fmt.Printf("mining Nonce: %d, hash: %s\n", blockHeader.Nonce, blockHeader.Hash().String())
		}
	}

	// initialize the block with the coinbase tx, block header and the subtree
	b := &ubsv_model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: uint64(leafCount),
		SizeInBytes:      123,
		Subtrees: []*chainhash.Hash{
			subtreeHash,
		},
	}

	currentChain := make([]*ubsv_model.BlockHeader, 11)
	currentChainIDs := make([]uint32, 11)
	for i := 0; i < 11; i++ {
		currentChain[i] = &ubsv_model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			// set the last 11 block header timestamps to be less than the current timestamps
			Timestamp: 1231469665 - uint32(i),
		}
		currentChainIDs[i] = uint32(i)
	}
	currentChain[0].HashPrevBlock = &chainhash.Hash{}

	// check if the block is valid, we expect an error because of the duplicate transaction
	oldBlockIDs := &sync.Map{}
	v, err := b.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, ubsv_model.TestCachedTxMetaStore, oldBlockIDs, nil, currentChain, currentChainIDs, ubsv_model.NewBloomStats())
	require.Error(t, err)
	require.False(t, v)

	_, hasTransactionsReferencingOldBlocks := util.ConvertSyncMapToUint32Slice(oldBlockIDs)
	require.False(t, hasTransactionsReferencingOldBlocks)
}

// This test runs a large block.Valid() test with a large number of txids
// It uses the testdata generated by the generateTestSets() function
func TestBigBlock_Valid(t *testing.T) {
	subtreeStore, block, err := generateBigBlockTestData(t)
	require.NoError(t, err)

	// check if the first txid is in the txMetaStore
	reqTxId, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	data, err := ubsv_model.TestCachedTxMetaStore.Get(context.Background(), reqTxId)
	require.NoError(t, err)
	require.Equal(t, &meta.Data{
		Fee:            1,
		SizeInBytes:    1,
		ParentTxHashes: []chainhash.Hash{},
	}, data)

	currentChain := make([]*ubsv_model.BlockHeader, 11)
	currentChainIDs := make([]uint32, 11)
	for i := 0; i < 11; i++ {
		currentChain[i] = &ubsv_model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			// set the last 11 block header timestamps to be less than the current timestamps
			Timestamp: 1231469665 - uint32(i),
		}
		currentChainIDs[i] = uint32(i)
	}
	currentChain[0].HashPrevBlock = &chainhash.Hash{}

	runtime.SetCPUProfileRate(500)
	f, _ := os.Create("cpu.prof")
	defer f.Close()

	_ = pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	start := time.Now()
	oldBlockIDs := &sync.Map{}
	v, err := block.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, ubsv_model.TestCachedTxMetaStore, oldBlockIDs, nil, currentChain, currentChainIDs, ubsv_model.NewBloomStats())
	require.NoError(t, err)
	t.Logf("Time taken: %s\n", time.Since(start))

	f, _ = os.Create("mem.prof")
	defer f.Close()
	_ = pprof.WriteHeapProfile(f)
	require.True(t, v)
}

func Test_NewOptimizedBloomFilter(t *testing.T) {
	subtreeStore, block, err := generateBigBlockTestData(t)
	require.NoError(t, err)

	// load the subtrees before starting profiling
	_ = block.GetAndValidateSubtrees(context.Background(), ulogger.TestLogger{}, subtreeStore, nil)

	runtime.SetCPUProfileRate(500)

	f, _ := os.Create("cpu.prof")

	defer f.Close()

	_ = pprof.StartCPUProfile(f)

	defer pprof.StopCPUProfile()

	timeStart := time.Now()
	bloomFilter, err := block.NewOptimizedBloomFilter(context.Background(), ulogger.TestLogger{}, subtreeStore)
	require.NoError(t, err)
	t.Logf("Time taken: %s\n", time.Since(timeStart))

	f, _ = os.Create("mem.prof")

	defer f.Close()

	_ = pprof.WriteHeapProfile(f)

	require.NotNil(t, bloomFilter)

	assert.Equal(t, true, bloomFilter.Has(0))
	assert.Equal(t, false, bloomFilter.Has(1))

	// check all txids are in bloom filter
	for idx, subtree := range block.SubtreeSlices {
		for nodeIdx, node := range subtree.Nodes {
			if idx == 0 && nodeIdx == 0 {
				continue
			}

			n64 := binary.BigEndian.Uint64(node.Hash[:])
			assert.Equal(t, true, bloomFilter.Has(n64))
		}
	}

	// random negative check
	assert.Equal(t, false, bloomFilter.Has(1231422))
	assert.Equal(t, false, bloomFilter.Has(5453456356))
	assert.Equal(t, false, bloomFilter.Has(4556873583))
}

func Test_NewOptimizedBloomFilter_EmptyBlock(t *testing.T) {
	subtreeStore := ubsv_model.NewLocalSubtreeStore()

	timeStart := time.Now()
	t.Logf("Time taken: %s\n", time.Since(timeStart))

	emptyBlock := &ubsv_model.Block{} // Assuming Block is your block struct
	emptyBloomFilter, err := emptyBlock.NewOptimizedBloomFilter(context.Background(), ulogger.TestLogger{}, subtreeStore)
	require.NoError(t, err)
	require.NotNil(t, emptyBloomFilter)
	assert.Equal(t, false, emptyBloomFilter.Has(0))

	// Case 3: Edge cases
	assert.Equal(t, false, emptyBloomFilter.Has(0xFFFFFFFFFFFFFFFF))
	assert.Equal(t, false, emptyBloomFilter.Has(0))
	assert.Equal(t, false, emptyBloomFilter.Has(1))

	// Case 4: Performance check
	timeThreshold := 1 * time.Millisecond
	assert.LessOrEqual(t, time.Since(timeStart), timeThreshold, "Bloom filter creation took too long")

}

func Test_LoadTxMetaIntoMemory(t *testing.T) {
	txMetaStore := memory.New(ulogger.TestLogger{})
	ubsv_model.TestCachedTxMetaStore, _ = txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, txMetaStore)

	f, _ := os.Create("cpu.prof")
	defer f.Close()

	_ = pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	err := ubsv_model.LoadTxMetaIntoMemory()
	require.NoError(t, err)

	f, _ = os.Create("mem.prof")
	defer f.Close()
	_ = pprof.WriteHeapProfile(f)
}

func generateBigBlockTestData(t *testing.T) (*ubsv_model.TestLocalSubtreeStore, *ubsv_model.Block, error) {
	ubsv_model.TestFileDir = "./big-test-generated_test_data/"
	ubsv_model.TestFileNameTemplate = ubsv_model.TestFileDir + "subtree-%d.bin"
	ubsv_model.TestFileNameTemplateMerkleHashes = ubsv_model.TestFileDir + "subtree-merkle-hashes.bin"
	ubsv_model.TestFileNameTemplateBlock = ubsv_model.TestFileDir + "block.bin"
	ubsv_model.TestTxMetafileNameTemplate = ubsv_model.TestFileDir + "txMeta.bin"
	subtreeStore := ubsv_model.NewLocalSubtreeStore()
	txCount := uint64(10 * 1024 * 1024)
	ubsv_model.TestSubtreeSize = 1024 * 1024
	createNewTestData := false

	// delete all the data in the ./testdata folder to regenerate the testdata
	block, err := ubsv_model.GenerateTestBlock(txCount, subtreeStore, createNewTestData)
	require.NoError(t, err)

	txMetaStore := memory.New(ulogger.TestLogger{})

	ubsv_model.TestLoadMetaToMemoryOnce.Do(func() {
		ubsv_model.TestCachedTxMetaStore, _ = txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, txMetaStore, 1024)
		err = ubsv_model.LoadTxMetaIntoMemory()
		require.NoError(t, err)
	})

	for idx, subtreeHash := range block.Subtrees {
		subtreeStore.Files[*subtreeHash] = idx
	}

	return subtreeStore, block, err
}

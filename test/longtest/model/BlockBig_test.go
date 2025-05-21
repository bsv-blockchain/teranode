package model

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/model"
	teranode_model "github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/txmetacache"
	"github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_bigblock ./test/...

func TestBlock_ValidBlockWithMultipleTransactions(t *testing.T) {
	teranode_model.TestFileDir = "./test-generated_test_data/"
	teranode_model.TestFileNameTemplate = teranode_model.TestFileDir + "subtree-%d.bin"
	teranode_model.TestFileNameTemplateMerkleHashes = teranode_model.TestFileDir + "subtree-merkle-hashes.bin"
	teranode_model.TestFileNameTemplateBlock = teranode_model.TestFileDir + "block.bin"
	teranode_model.TestTxMetafileNameTemplate = teranode_model.TestFileDir + "txMeta.bin"
	subtreeStore := teranode_model.NewLocalSubtreeStore()
	txCount := uint64(4 * 1024)
	teranode_model.TestSubtreeSize = 1024

	block, err := teranode_model.GenerateTestBlock(txCount, subtreeStore, true)
	require.NoError(t, err)

	txMetaStore := memory.New(ulogger.TestLogger{})
	teranode_model.TestCachedTxMetaStore, err = txmetacache.NewTxMetaCache(context.Background(), settings.NewSettings(), ulogger.TestLogger{}, txMetaStore, txmetacache.Unallocated, 1024)
	require.NoError(t, err)
	err = teranode_model.LoadTxMetaIntoMemory()
	require.NoError(t, err)

	// check if the first txid is in the txMetaStore
	reqTxId, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	data, err := teranode_model.TestCachedTxMetaStore.Get(context.Background(), reqTxId)
	require.NoError(t, err)
	require.Equal(t, &meta.Data{
		Fee:            1,
		SizeInBytes:    1,
		ParentTxHashes: []chainhash.Hash{},
	}, data)

	for idx, subtreeHash := range block.Subtrees {
		subtreeStore.Files[*subtreeHash] = idx
	}

	currentChain := make([]*teranode_model.BlockHeader, 11)
	currentChainIDs := make([]uint32, 11)
	for i := 0; i < 11; i++ {
		currentChain[i] = &teranode_model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			// set the last 11 block header timestamps to be less than the current timestamps
			Timestamp: 1231469665 - uint32(i),
		}
		currentChainIDs[i] = uint32(i)
	}
	currentChain[0].HashPrevBlock = &chainhash.Hash{}

	// check if the block is valid, we expect an error because of the duplicate transaction
	oldBlockIDs := util.NewSyncedMap[chainhash.Hash, []uint32]()

	v, err := block.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, teranode_model.TestCachedTxMetaStore, oldBlockIDs, nil, currentChain, currentChainIDs, teranode_model.NewBloomStats())
	require.NoError(t, err)
	require.True(t, v)

	_, hasTransactionsReferencingOldBlocks := util.ConvertSyncedMapToUint32Slice(oldBlockIDs)
	require.False(t, hasTransactionsReferencingOldBlocks)
}

func TestBlock_WithDuplicateTransaction(t *testing.T) {
	leafCount := 8
	subtree, err := util.NewTreeByLeafCount(leafCount)
	require.NoError(t, err)
	teranode_model.TestSubtreeSize = 8

	subtreeStore := teranode_model.NewLocalSubtreeStore()
	txMetaStore := memory.New(ulogger.TestLogger{})
	teranode_model.TestCachedTxMetaStore, err = txmetacache.NewTxMetaCache(context.Background(), settings.NewSettings(), ulogger.TestLogger{}, txMetaStore, txmetacache.Unallocated, 1024)
	require.NoError(t, err)
	txMetaCache := teranode_model.TestCachedTxMetaStore.(*txmetacache.TxMetaCache)

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
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", util.GetBlockSubsidyForHeight(1, &chaincfg.MainNetParams)+subtree.Fees)
	nBits, _ := teranode_model.NewNBitFromString("2000ffff")

	// get subtree root hash
	subtreeHash := subtree.RootHash()

	// store subtree hash in the subtreeStore map
	subtreeStore.Files[*subtreeHash] = 0

	// create a new subtree for replaced coinbase transaction
	replacedCoinbaseSubtree, err := util.NewTreeByLeafCount(teranode_model.TestSubtreeSize)
	require.NoError(t, err)

	// deserialize the replaced coinbase subtree
	err = replacedCoinbaseSubtree.Deserialize(subtreeBytes)
	require.NoError(t, err)

	// replace the root node with the coinbase transaction
	replacedCoinbaseSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))

	// calculate the root hash of the replaced coinbase subtree
	rootHash := replacedCoinbaseSubtree.RootHash()

	// create a block header with the replaced coinbase subtree root hash
	blockHeader := &teranode_model.BlockHeader{
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

	b, err := teranode_model.NewBlock(
		blockHeader,
		coinbase,
		[]*chainhash.Hash{
			subtreeHash,
		},
		uint64(leafCount),
		123,
		0,
		0,
		nil,
	)
	require.NoError(t, err)

	currentChain := make([]*teranode_model.BlockHeader, 11)
	currentChainIDs := make([]uint32, 11)
	for i := 0; i < 11; i++ {
		currentChain[i] = &teranode_model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			// set the last 11 block header timestamps to be less than the current timestamps
			Timestamp: 1231469665 - uint32(i),
		}
		currentChainIDs[i] = uint32(i)
	}
	currentChain[0].HashPrevBlock = &chainhash.Hash{}

	// check if the block is valid, we expect an error because of the duplicate transaction
	oldBlockIDs := util.NewSyncedMap[chainhash.Hash, []uint32]()

	v, err := b.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, teranode_model.TestCachedTxMetaStore, oldBlockIDs, nil, currentChain, currentChainIDs, teranode_model.NewBloomStats())
	require.Error(t, err)
	require.False(t, v)

	_, hasTransactionsReferencingOldBlocks := util.ConvertSyncedMapToUint32Slice(oldBlockIDs)
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

	data, err := teranode_model.TestCachedTxMetaStore.Get(context.Background(), reqTxId)
	require.NoError(t, err)
	require.Equal(t, &meta.Data{
		Fee:            1,
		SizeInBytes:    1,
		ParentTxHashes: []chainhash.Hash{},
	}, data)

	currentChain := make([]*teranode_model.BlockHeader, 11)
	currentChainIDs := make([]uint32, 11)
	for i := 0; i < 11; i++ {
		currentChain[i] = &teranode_model.BlockHeader{
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
	oldBlockIDs := util.NewSyncedMap[chainhash.Hash, []uint32]()

	v, err := block.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, teranode_model.TestCachedTxMetaStore, oldBlockIDs, nil, currentChain, currentChainIDs, teranode_model.NewBloomStats())
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

	bbf := &model.BlockBloomFilter{
		CreationTime: time.Now(),
		BlockHash:    block.Hash(),
	}

	timeStart := time.Now()
	bloomFilter, err := block.NewOptimizedBloomFilter(context.Background(), ulogger.TestLogger{}, subtreeStore)
	require.NoError(t, err)
	t.Logf("Time taken: %s\n", time.Since(timeStart))

	bbf.Filter = bloomFilter

	f, _ = os.Create("mem.prof")

	defer f.Close()

	_ = pprof.WriteHeapProfile(f)

	require.NotNil(t, bbf.Filter)

	assert.Equal(t, true, bbf.Filter.Has(0))
	assert.Equal(t, false, bbf.Filter.Has(1))

	// check all txids are in bloom filter
	for idx, subtree := range block.SubtreeSlices {
		for nodeIdx, node := range subtree.Nodes {
			if idx == 0 && nodeIdx == 0 {
				continue
			}

			n64 := binary.BigEndian.Uint64(node.Hash[:])
			assert.Equal(t, true, bbf.Filter.Has(n64))
		}
	}

	// random negative check
	assert.Equal(t, false, bbf.Filter.Has(1231422))
	assert.Equal(t, false, bbf.Filter.Has(5453456356))
	assert.Equal(t, false, bbf.Filter.Has(4556873583))

	// store the bloom filter to the subtreestore

	// Serialize the bloom filter
	bloomFilterBytes, err := bbf.Serialize()
	require.NoError(t, err)
	require.NotNil(t, bloomFilterBytes)

	//record the bloom filter in the subtreestore
	err = subtreeStore.Set(context.Background(), block.Hash()[:], bloomFilterBytes, options.WithFileExtension("bloomfilter"))
	require.NoError(t, err)

	// get the bloom filter from the subtreestore
	retrievedBloomFilterBytes, err := subtreeStore.Get(context.Background(), block.Hash()[:], options.WithFileExtension("bloomfilter"))
	require.NoError(t, err)

	fmt.Println("retrievedBloomFilterBytes Length: ", len(retrievedBloomFilterBytes))

	createdBbf := &model.BlockBloomFilter{
		CreationTime: time.Now(),
		BlockHash:    block.Hash(),
	}

	err = createdBbf.Deserialize(bloomFilterBytes)
	require.NoError(t, err)

	assert.Equal(t, true, createdBbf.Filter.Has(0))
	assert.Equal(t, false, createdBbf.Filter.Has(1))

	// check all txids are in bloom filter
	for idx, subtree := range block.SubtreeSlices {
		for nodeIdx, node := range subtree.Nodes {
			if idx == 0 && nodeIdx == 0 {
				continue
			}

			n64 := binary.BigEndian.Uint64(node.Hash[:])
			assert.Equal(t, true, createdBbf.Filter.Has(n64))
		}
	}

	// random negative check
	assert.Equal(t, false, createdBbf.Filter.Has(1231422))
	assert.Equal(t, false, createdBbf.Filter.Has(5453456356))
	assert.Equal(t, false, createdBbf.Filter.Has(4556873583))
}

func Test_NewOptimizedBloomFilter_EmptyBlock(t *testing.T) {
	subtreeStore := teranode_model.NewLocalSubtreeStore()

	timeStart := time.Now()
	t.Logf("Time taken: %s\n", time.Since(timeStart))

	emptyBlock := &teranode_model.Block{} // Assuming Block is your block struct
	emptyBloomFilter, err := emptyBlock.NewOptimizedBloomFilter(context.Background(), ulogger.TestLogger{}, subtreeStore)
	require.NoError(t, err)
	require.NotNil(t, emptyBloomFilter)
	assert.Equal(t, false, emptyBloomFilter.Has(0))

	// Case 3: Edge cases
	assert.Equal(t, false, emptyBloomFilter.Has(0xFFFFFFFFFFFFFFFF))
	assert.Equal(t, false, emptyBloomFilter.Has(0))
	assert.Equal(t, false, emptyBloomFilter.Has(1))

}

func Test_LoadTxMetaIntoMemory(t *testing.T) {
	var err error
	txMetaStore := memory.New(ulogger.TestLogger{})
	teranode_model.TestCachedTxMetaStore, err = txmetacache.NewTxMetaCache(context.Background(), settings.NewSettings(), ulogger.TestLogger{}, txMetaStore, txmetacache.Unallocated)
	require.NoError(t, err)

	f, _ := os.Create("cpu.prof")
	defer f.Close()

	_ = pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	err = teranode_model.LoadTxMetaIntoMemory()
	require.NoError(t, err)

	f, _ = os.Create("mem.prof")
	defer f.Close()
	_ = pprof.WriteHeapProfile(f)
}

func generateBigBlockTestData(t *testing.T) (*teranode_model.TestLocalSubtreeStore, *teranode_model.Block, error) {
	teranode_model.TestFileDir = "./big-test-generated_test_data/"
	teranode_model.TestFileNameTemplate = teranode_model.TestFileDir + "subtree-%d.bin"
	teranode_model.TestFileNameTemplateMerkleHashes = teranode_model.TestFileDir + "subtree-merkle-hashes.bin"
	teranode_model.TestFileNameTemplateBlock = teranode_model.TestFileDir + "block.bin"
	teranode_model.TestTxMetafileNameTemplate = teranode_model.TestFileDir + "txMeta.bin"
	subtreeStore := teranode_model.NewLocalSubtreeStore()
	txCount := uint64(10 * 1024 * 1024)
	teranode_model.TestSubtreeSize = 1024 * 1024
	createNewTestData := false

	// delete all the data in the ./testdata folder to regenerate the testdata
	block, err := teranode_model.GenerateTestBlock(txCount, subtreeStore, createNewTestData)
	require.NoError(t, err)

	txMetaStore := memory.New(ulogger.TestLogger{})

	teranode_model.TestLoadMetaToMemoryOnce.Do(func() {
		teranode_model.TestCachedTxMetaStore, err = txmetacache.NewTxMetaCache(context.Background(), settings.NewSettings(), ulogger.TestLogger{}, txMetaStore, txmetacache.Unallocated, 1024)
		require.NoError(t, err)
		err = teranode_model.LoadTxMetaIntoMemory()
		require.NoError(t, err)
	})

	for idx, subtreeHash := range block.Subtrees {
		subtreeStore.Files[*subtreeHash] = idx
	}

	return subtreeStore, block, err
}

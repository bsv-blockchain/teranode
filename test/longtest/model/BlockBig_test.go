package model

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	"github.com/bsv-blockchain/go-chaincfg"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/model"
	teranode_model "github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_bigblock ./test/...

func TestBlock_ValidBlockWithMultipleTransactions(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	txCount := uint64(4 * 1024) // configurable
	subtreeSize := 1024         // configurable

	block, subtreeStore, txMetaStore, err := createTestBlockWithMultipleTxs(t, txCount, subtreeSize)
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
	oldBlockIDs := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

	v, err := block.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, txMetaStore, oldBlockIDs, nil, currentChain, currentChainIDs, teranode_model.NewBloomStats(), tSettings)
	require.NoError(t, err)
	require.True(t, v)

	_, hasTransactionsReferencingOldBlocks := txmap.ConvertSyncedMapToUint32Slice(oldBlockIDs)
	require.False(t, hasTransactionsReferencingOldBlocks)
}

func calculateMerkleRoot(hashes []*chainhash.Hash) (*chainhash.Hash, error) {
	var calculatedMerkleRootHash *chainhash.Hash
	if len(hashes) == 1 {
		calculatedMerkleRootHash = hashes[0]
	} else if len(hashes) > 0 {
		st, err := subtreepkg.NewIncompleteTreeByLeafCount(len(hashes))
		if err != nil {
			return nil, err
		}

		for _, hash := range hashes {
			err := st.AddNode(*hash, 1, 0)
			if err != nil {
				return nil, err
			}
		}

		calculatedMerkleRoot := st.RootHash()
		calculatedMerkleRootHash, err = chainhash.NewHash(calculatedMerkleRoot[:])

		if err != nil {
			return nil, err
		}
	}

	return calculatedMerkleRootHash, nil
}

// createTestBlockWithMultipleTxs creates a block with a coinbase and (txCount-1) regular transactions, builds the subtree, stores tx meta, calculates the merkle root, mines the header, and returns the block, subtree store, and tx meta store.
func createTestBlockWithMultipleTxs(t *testing.T, txCount uint64, subtreeSize int) (*teranode_model.Block, *blobmemory.Memory, utxo.Store, error) {
	ctx := context.Background()

	var err error

	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	blockHeight := uint32(1)
	blockID := uint32(1)

	// Coinbase tx
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From(
		"0000000000000000000000000000000000000000000000000000000000000000",
		0xffffffff,
		"",
		0,
	)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	subtreeStore := blobmemory.New()

	// Store coinbase meta with block ID, since it has to be mined in the first block
	_, err = utxoStore.Create(ctx, coinbaseTx, blockHeight, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID:     blockID,
		BlockHeight: blockHeight,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)

	txs := make([]*bt.Tx, txCount)
	txs[0] = coinbaseTx
	prevTx := coinbaseTx

	for i := uint64(1); i < txCount; i++ {
		tx := bt.NewTx()
		_ = tx.FromUTXOs(&bt.UTXO{
			TxIDHash:      prevTx.TxIDChainHash(),
			Vout:          0,
			LockingScript: prevTx.Outputs[0].LockingScript,
			Satoshis:      prevTx.Outputs[0].Satoshis,
		})
		_ = tx.AddP2PKHOutputFromAddress(address.AddressString, prevTx.Outputs[0].Satoshis-1)
		_ = tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
		txs[i] = tx

		// Store meta for this tx
		_, err = utxoStore.Create(ctx, tx, blockHeight)
		require.NoError(t, err)

		prevTx = tx
	}

	// Subtree batching with IsComplete check
	subtreeHashes := make([]*chainhash.Hash, 0)
	subtreeCount := 0

	var (
		subtree           *subtreepkg.Subtree
		subtreeMeta       *subtreepkg.Meta
		subtreeBytes      []byte
		firstSubtreeBytes []byte
		subtreeMetaBytes  []byte
	)

	subtree, _ = subtreepkg.NewTreeByLeafCount(subtreeSize)
	require.NoError(t, subtree.AddCoinbaseNode())
	subtreeMeta = subtreepkg.NewSubtreeMeta(subtree)

	for i := 1; i < int(txCount); i++ { //nolint:gosec
		tx := txs[i]
		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), uint64(tx.Size()), 0)) //nolint:gosec
		require.NoError(t, subtreeMeta.SetTxInpointsFromTx(tx))

		if subtree.IsComplete() {
			subtreeBytes, err = subtree.Serialize()
			if subtreeCount == 0 {
				firstSubtreeBytes = subtreeBytes
			}

			require.NoError(t, err)
			require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes, options.WithDeleteAt(1000), options.WithAllowOverwrite(true)))

			subtreeMetaBytes, err = subtreeMeta.Serialize()
			require.NoError(t, err)
			require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

			subtreeHash := subtree.RootHash()
			subtreeHashes = append(subtreeHashes, subtreeHash)

			subtreeCount++
			// Start new subtree/meta
			subtree, _ = subtreepkg.NewTreeByLeafCount(subtreeSize)
			subtreeMeta = subtreepkg.NewSubtreeMeta(subtree)
		}
	}
	// After all txs, if the last subtree is not empty, store it
	if subtree.Length() > 0 {
		subtreeBytes, err = subtree.Serialize()
		require.NoError(t, err)
		require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

		subtreeMetaBytes, err = subtreeMeta.Serialize()
		require.NoError(t, err)
		require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

		subtreeHash := subtree.RootHash()
		subtreeHashes = append(subtreeHashes, subtreeHash)
	}

	// Calculate merkle root from all subtree hashes
	replacedCoinbaseSubtree, _ := subtreepkg.NewTreeByLeafCount(subtreeSize)
	require.NoError(t, replacedCoinbaseSubtree.Deserialize(firstSubtreeBytes))
	replacedCoinbaseSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec

	merkleRootSubtreeHashes := make([]*chainhash.Hash, len(subtreeHashes))

	for idx, hash := range subtreeHashes {
		if idx == 0 {
			merkleRootSubtreeHashes[idx] = replacedCoinbaseSubtree.RootHash()
		} else {
			merkleRootSubtreeHashes[idx] = hash
		}
	}

	calculatedMerkleRootHash, err := calculateMerkleRoot(merkleRootSubtreeHashes)
	require.NoError(t, err)

	nBits, _ := teranode_model.NewNBitFromString("2000ffff")
	blockHeader := &teranode_model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	// Mine header
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	block, err := teranode_model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		txCount,
		uint64(coinbaseTx.Size())+uint64(txs[1].Size())*(txCount-1), //nolint:gosec
		100, 0,
	)
	require.NoError(t, err)

	return block, subtreeStore, utxoStore, nil
}

func TestBlock_WithDuplicateTransaction(t *testing.T) {
	leafCount := 8
	subtree, err := subtreepkg.NewTreeByLeafCount(leafCount)
	require.NoError(t, err)
	teranode_model.TestSubtreeSize = 8

	subtreeStore := teranode_model.NewLocalSubtreeStore()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	teranode_model.TestCachedTxMetaStore, err = txmetacache.NewTxMetaCache(context.Background(), settings.NewSettings(), ulogger.TestLogger{}, utxoStore, txmetacache.Unallocated, 1024)
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
	replacedCoinbaseSubtree, err := subtreepkg.NewTreeByLeafCount(teranode_model.TestSubtreeSize)
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
	oldBlockIDs := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

	v, err := b.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, teranode_model.TestCachedTxMetaStore, oldBlockIDs, nil, currentChain, currentChainIDs, teranode_model.NewBloomStats(), tSettings)
	require.Error(t, err)
	require.False(t, v)

	_, hasTransactionsReferencingOldBlocks := txmap.ConvertSyncedMapToUint32Slice(oldBlockIDs)
	require.False(t, hasTransactionsReferencingOldBlocks)
}

// This test runs a large block.Valid() test with a large number of txids
// It uses the testdata generated by the generateTestSets() function
func TestBigBlock_Valid(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	// t.Skip("The test data is no longer compatible with the current code")
	subtreeStore, block, err := generateBigBlockTestData(t)
	require.NoError(t, err)

	// check if the first txid is in the txMetaStore
	reqTxId, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	data, err := teranode_model.TestCachedTxMetaStore.GetMeta(context.Background(), reqTxId)
	require.NoError(t, err)
	require.Equal(t, &meta.Data{
		Fee:         1,
		SizeInBytes: 1,
		TxInpoints:  subtreepkg.TxInpoints{ParentTxHashes: nil, Idxs: nil},
		BlockIDs:    []uint32{},
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

	// Only start profiling if not already profiling
	f, err := os.Create("cpu.prof")
	if err == nil {
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			t.Logf("CPU profiling already active or failed to start: %v", err)
		} else {
			defer pprof.StopCPUProfile()
		}
	}

	start := time.Now()
	oldBlockIDs := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

	v, err := block.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, teranode_model.TestCachedTxMetaStore, oldBlockIDs, nil, currentChain, currentChainIDs, teranode_model.NewBloomStats(), tSettings)
	require.NoError(t, err)
	t.Logf("Time taken: %s\n", time.Since(start))

	f, _ = os.Create("mem.prof")
	defer f.Close()
	_ = pprof.WriteHeapProfile(f)
	require.True(t, v)
}

func Test_NewOptimizedBloomFilter(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	subtreeStore, block, err := generateBigBlockTestData(t)
	require.NoError(t, err)

	// load the subtrees before starting profiling
	_ = block.GetAndValidateSubtrees(context.Background(), ulogger.TestLogger{}, subtreeStore, tSettings.Block.GetAndValidateSubtreesConcurrency)

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
	bloomFilter, err := block.NewOptimizedBloomFilter(context.Background(), ulogger.TestLogger{}, subtreeStore, tSettings.Block.GetAndValidateSubtreesConcurrency)
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

	// record the bloom filter in the subtreestore
	err = subtreeStore.Set(context.Background(), block.Hash()[:], fileformat.FileTypeBloomFilter, bloomFilterBytes)
	require.NoError(t, err)

	// get the bloom filter from the subtreestore
	retrievedBloomFilterBytes, err := subtreeStore.Get(context.Background(), block.Hash()[:], fileformat.FileTypeBloomFilter)
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
	tSettings := test.CreateBaseTestSettings(t)
	subtreeStore := teranode_model.NewLocalSubtreeStore()

	timeStart := time.Now()
	t.Logf("Time taken: %s\n", time.Since(timeStart))

	emptyBlock := &teranode_model.Block{} // Assuming Block is your block struct
	emptyBloomFilter, err := emptyBlock.NewOptimizedBloomFilter(context.Background(), ulogger.TestLogger{}, subtreeStore, tSettings.Block.GetAndValidateSubtreesConcurrency)
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

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	teranode_model.TestFileDir = t.TempDir() + "/"
	teranode_model.TestTxMetafileNameTemplate = teranode_model.TestFileDir + "txMeta.bin"

	teranode_model.TestCachedTxMetaStore, err = txmetacache.NewTxMetaCache(context.Background(), settings.NewSettings(), ulogger.TestLogger{}, utxoStore, txmetacache.Unallocated)
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
	teranode_model.TestFileDir = t.TempDir() + "/"
	teranode_model.TestFileNameTemplate = teranode_model.TestFileDir + "subtree-%d.bin"
	teranode_model.TestFileNameTemplateMerkleHashes = teranode_model.TestFileDir + "subtree-merkle-hashes.bin"
	teranode_model.TestFileNameTemplateBlock = teranode_model.TestFileDir + "block.bin"
	teranode_model.TestTxMetafileNameTemplate = teranode_model.TestFileDir + "txMeta.bin"
	subtreeStore := teranode_model.NewLocalSubtreeStore()
	txCount := uint64(10 * 1024 * 1024)
	teranode_model.TestSubtreeSize = 1024 * 1024
	createNewTestData := true

	// delete all the data in the ./testdata folder to regenerate the testdata
	block, err := teranode_model.GenerateTestBlock(txCount, subtreeStore, createNewTestData)
	require.NoError(t, err)

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	teranode_model.TestLoadMetaToMemoryOnce.Do(func() {
		teranode_model.TestCachedTxMetaStore, err = txmetacache.NewTxMetaCache(context.Background(), settings.NewSettings(), ulogger.TestLogger{}, utxoStore, txmetacache.Unallocated, 1024)
		require.NoError(t, err)
		err = teranode_model.LoadTxMetaIntoMemory()
		require.NoError(t, err)
	})

	for idx, subtreeHash := range block.Subtrees {
		subtreeStore.Files[*subtreeHash] = idx
	}

	return subtreeStore, block, err
}

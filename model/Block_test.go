package model

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/null"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlock_Bytes(t *testing.T) {

	hash1, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	hash2, _ := chainhash.NewHashFromStr("000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd")
	coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000")

	t.Run("test block bytes - min size", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block1Header)
		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		require.NoError(t, err)

		block := &Block{
			Header:           blockHeader,
			CoinbaseTx:       &bt.Tx{},
			TransactionCount: 1,
			SizeInBytes:      123,
			Subtrees:         []*chainhash.Hash{},
		}

		blockBytes, err := block.Bytes()
		require.NoError(t, err)

		assert.Equal(t, 93, len(blockBytes))
	})

	t.Run("test block bytes", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block1Header)
		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		require.NoError(t, err)

		block := &Block{
			Header:           blockHeader,
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			SizeInBytes:      123,
			Subtrees:         []*chainhash.Hash{},
		}

		blockBytes, err := block.Bytes()
		require.NoError(t, err)

		blockFromBytes, err := NewBlockFromBytes(blockBytes)
		require.NoError(t, err)

		assert.Equal(t, block1Header, hex.EncodeToString(blockFromBytes.Header.Bytes()))
		assert.Equal(t, block.CoinbaseTx.String(), blockFromBytes.CoinbaseTx.String())
		assert.Equal(t, block.TransactionCount, blockFromBytes.TransactionCount)
		assert.Equal(t, block.Subtrees, blockFromBytes.Subtrees)

		assert.Equal(t, "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048", block.Hash().String())
		assert.Equal(t, block.Hash().String(), blockFromBytes.Hash().String())
		assert.Equal(t, uint64(1), block.TransactionCount)
		assert.Equal(t, uint64(123), block.SizeInBytes)

		assert.NoError(t, block.CheckMerkleRoot(context.Background()))
	})

	t.Run("test block bytes - subtrees", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block1Header)
		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		require.NoError(t, err)

		block := &Block{
			Header:           blockHeader,
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			SizeInBytes:      uint64(len(coinbaseTx.Bytes())) + 80 + util.VarintSize(1),
			Subtrees: []*chainhash.Hash{
				hash1,
				hash2,
			},
		}

		blockBytes, err := block.Bytes()
		require.NoError(t, err)

		blockFromBytes, err := NewBlockFromBytes(blockBytes)
		require.NoError(t, err)

		assert.Len(t, blockFromBytes.Subtrees, 2)
		assert.Equal(t, block.Subtrees[0].String(), blockFromBytes.Subtrees[0].String())
		assert.Equal(t, block.Subtrees[1].String(), blockFromBytes.Subtrees[1].String())
		assert.Equal(t, uint64(1), block.TransactionCount)
		assert.Equal(t, uint64(215), block.SizeInBytes)
	})
}

func TestMedianTimestamp(t *testing.T) {
	timestamps := make([]time.Time, 11)
	for i := range timestamps {
		timestamps[i] = time.Unix(int64(i), 0)
	}

	t.Run("test for correct median time", func(t *testing.T) {
		expected := timestamps[5]
		median, err := medianTimestamp(timestamps)

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if !median.Equal(expected) {
			t.Errorf("Expected median %v, got %v", expected, *median)
		}
	})

	t.Run("test for correct median time unsorted", func(t *testing.T) {
		expected := timestamps[6]
		// add a new high timestamp out of sequence
		timestamps[5] = time.Unix(int64(20), 0)
		median, err := medianTimestamp(timestamps)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if !median.Equal(expected) {
			t.Errorf("Expected median %v, got %v", expected, *median)
		}
	})

	t.Run("test for correct median time unsorted 2", func(t *testing.T) {
		expected := timestamps[4]
		// add a new low timestamp out of sequence
		timestamps[5] = time.Unix(int64(1), 0)
		median, err := medianTimestamp(timestamps)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if !median.Equal(expected) {
			t.Errorf("Expected median %v, got %v", expected, *median)
		}
	})

	t.Run("test for less than 11 timestamps", func(t *testing.T) {
		expected := timestamps[5]
		median, err := medianTimestamp(timestamps[:10])
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if !median.Equal(expected) {
			t.Errorf("Expected median %v, got %v", expected, *median)
		}
	})
}

func TestBlock_ValidWithOneTransaction(t *testing.T) {
	blockHeaderBytes, _ := hex.DecodeString(block1Header)
	blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)

	coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
	coinbase, err := bt.NewTxFromString(coinbaseHex)
	require.NoError(t, err)

	b := &Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		SizeInBytes:      123,
		Subtrees:         []*chainhash.Hash{},
	}

	subtreeStore, _ := null.New(ulogger.TestLogger{})
	txMetaStore := memory.New(ulogger.TestLogger{}, true)

	currentChain := make([]*BlockHeader, 11)
	currentChainIDs := make([]uint32, 11)
	for i := 0; i < 11; i++ {
		currentChain[i] = &BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			// set the last 11 block header timestamps to be less than the current timestamps
			Timestamp: 1231469665 - uint32(i),
		}
		currentChainIDs[i] = uint32(i)
	}
	currentChain[0].HashPrevBlock = &chainhash.Hash{}
	v, err := b.Valid(context.Background(), subtreeStore, txMetaStore, nil, currentChain, currentChainIDs)
	require.NoError(t, err)
	require.True(t, v)
}

func TestBlock_ValidBlockWithMultipleTransactions(t *testing.T) {
	fileDir = "./test-generated_test_data/"
	fileNameTemplate = fileDir + "subtree-%d.bin"
	fileNameTemplateMerkleHashes = fileDir + "subtree-merkle-hashes.bin"
	fileNameTemplateBlock = fileDir + "block.bin"
	txMetafileNameTemplate = fileDir + "txMeta.bin"
	subtreeStore := newLocalSubtreeStore()
	txIdCount := uint64(8)
	subtreeSize = 8

	block, err := generateTestSets(txIdCount, subtreeStore, true)
	require.NoError(t, err)

	txMetaStore := memory.New(ulogger.TestLogger{}, true)
	cachedTxMetaStore = txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, txMetaStore, 1024)
	err = loadTxMetaIntoMemory()
	require.NoError(t, err)

	// check if the first txid is in the txMetaStore
	reqTxId, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	data, err := cachedTxMetaStore.Get(context.Background(), reqTxId)
	require.NoError(t, err)
	require.Equal(t, &txmeta.Data{
		Fee:            1,
		SizeInBytes:    1,
		ParentTxHashes: []chainhash.Hash{},
	}, data)

	for idx, subtreeHash := range block.Subtrees {
		subtreeStore.files[*subtreeHash] = idx
	}

	currentChain := make([]*BlockHeader, 11)
	currentChainIDs := make([]uint32, 11)
	for i := 0; i < 11; i++ {
		currentChain[i] = &BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			// set the last 11 block header timestamps to be less than the current timestamps
			Timestamp: 1231469665 - uint32(i),
		}
		currentChainIDs[i] = uint32(i)
	}
	currentChain[0].HashPrevBlock = &chainhash.Hash{}

	// check if the block is valid, we expect an error because of the duplicate transaction
	v, err := block.Valid(context.Background(), subtreeStore, cachedTxMetaStore, nil, currentChain, currentChainIDs)
	require.NoError(t, err)
	require.True(t, v)
}

func TestBlock_WithDuplicateTransaction(t *testing.T) {
	leafCount := 8
	subtree, err := util.NewTreeByLeafCount(leafCount)
	require.NoError(t, err)

	fileDir = "./test-generated_test_data/"
	fileNameTemplate = fileDir + "subtree-%d.bin"
	subtreeStore := newLocalSubtreeStore()
	txMetaStore := memory.New(ulogger.TestLogger{}, true)
	cachedTxMetaStore = txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, txMetaStore, 1024)
	txMetaCache := cachedTxMetaStore.(*txmetacache.TxMetaCache)

	if _, err := os.Stat(fileDir); os.IsNotExist(err) {
		err = os.Mkdir(fileDir, 0755)
		require.NoError(t, err)
	}

	// create a slice of random hashes, for the leaves
	hashes := make([]*chainhash.Hash, leafCount)
	for i := 0; i < leafCount-2; i++ {
		// create random 32 bytes
		bytes := make([]byte, 32)
		_, _ = rand.Read(bytes)
		hashes[i], _ = chainhash.NewHash(bytes)
	}

	// first transaction is the coinbase transaction
	_ = subtree.AddNode(CoinbasePlaceholder, 0, 0)

	// rest of transactions are random
	for i := 0; i < leafCount-2; i++ {
		_ = subtree.AddNode(*hashes[i], 111, 0)
		err = txMetaCache.SetCache(hashes[i], &txmeta.Data{Fee: 111, SizeInBytes: 1})
		require.NoError(t, err)
	}

	// last transaction is a duplicate of the previous transaction
	_ = subtree.AddNode(*hashes[leafCount-3], 111, 0)

	// check if cachedTxMetaStore has the correct data
	data, err := cachedTxMetaStore.Get(context.Background(), hashes[0])
	require.NoError(t, err)
	require.Equal(t, &txmeta.Data{
		Fee:            111,
		SizeInBytes:    1,
		ParentTxHashes: []chainhash.Hash{},
	}, data)

	// create a subtree file
	subtreeFile, err := os.Create(fmt.Sprintf(fileNameTemplate, 0))
	require.NoError(t, err)

	// serialize the subtree
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)

	// write the subtree to the file
	_, err = subtreeFile.Write(subtreeBytes)
	require.NoError(t, err)

	// create a coinbase transaction
	coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
	coinbase, err := bt.NewTxFromString(coinbaseHex)
	require.NoError(t, err)

	// add a P2PKH output to the coinbase transaction with fees
	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", util.GetBlockSubsidyForHeight(1)+subtree.Fees)
	nBits := NewNBitFromString("2000ffff")

	// get subtree root hash
	subtreeHash := subtree.RootHash()

	// store subtree hash in the subtreeStore map
	subtreeStore.files[*subtreeHash] = 0

	// create a new subtree for replaced coinbase transaction
	replacedCoinbaseSubtree, err := util.NewTreeByLeafCount(subtreeSize)
	require.NoError(t, err)

	// close the subtree file
	err = subtreeFile.Close()
	require.NoError(t, err)

	// deserialize the replaced coinbase subtree
	err = replacedCoinbaseSubtree.Deserialize(subtreeBytes)
	require.NoError(t, err)

	// replace the root node with the coinbase transaction
	replacedCoinbaseSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))

	// calculate the root hash of the replaced coinbase subtree
	rootHash := replacedCoinbaseSubtree.RootHash()

	// create a block header with the replaced coinbase subtree root hash
	blockHeader := &BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: rootHash,
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           nBits,
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
	b := &Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: uint64(leafCount),
		SizeInBytes:      123,
		Subtrees: []*chainhash.Hash{
			subtreeHash,
		},
	}

	currentChain := make([]*BlockHeader, 11)
	currentChainIDs := make([]uint32, 11)
	for i := 0; i < 11; i++ {
		currentChain[i] = &BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			// set the last 11 block header timestamps to be less than the current timestamps
			Timestamp: 1231469665 - uint32(i),
		}
		currentChainIDs[i] = uint32(i)
	}
	currentChain[0].HashPrevBlock = &chainhash.Hash{}

	// check if the block is valid, we expect an error because of the duplicate transaction
	_, _ = b.Valid(context.Background(), subtreeStore, cachedTxMetaStore, nil, currentChain, currentChainIDs)
	// TODO reactivate this test when we have a way to check for duplicate transactions
	// require.Error(t, err)
	// require.False(t, v)
}

func TestGetAndValidateSubtrees(t *testing.T) {
	blockHeaderBytes, _ := hex.DecodeString(block1Header)
	blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)

	coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
	coinbase, err := bt.NewTxFromString(coinbaseHex)
	require.NoError(t, err)

	subtreeHash, _ := chainhash.NewHashFromStr("9daba5e5c8ecdb80e811ef93558e960a6ffed0c481182bd47ac381547361ff25")

	b := &Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		SizeInBytes:      123,
		Subtrees: []*chainhash.Hash{
			subtreeHash,
		},
	}

	mockBlobStore, _ := New(ulogger.TestLogger{})
	err = b.GetAndValidateSubtrees(context.Background(), mockBlobStore)
	require.NoError(t, err)
}

func TestCheckDuplicateTransactions(t *testing.T) {
	leafCount := 4
	subtree, err := util.NewTreeByLeafCount(leafCount)
	require.NoError(t, err)

	// create a slice of random hashes
	hashes := make([]*chainhash.Hash, leafCount)
	for i := 0; i < leafCount; i++ {
		// create random 32 bytes
		bytes := make([]byte, 32)
		_, _ = rand.Read(bytes)
		hashes[i], _ = chainhash.NewHash(bytes)
	}

	for i := 0; i < leafCount-1; i++ {
		_ = subtree.AddNode(*hashes[i], 111, 0)
	}
	// add the same hash twice
	_ = subtree.AddNode(*hashes[0], 111, 0)

	blockHeaderBytes, _ := hex.DecodeString(block1Header)
	blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)

	coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
	coinbase, err := bt.NewTxFromString(coinbaseHex)
	require.NoError(t, err)

	b := &Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		SizeInBytes:      123,
		Subtrees: []*chainhash.Hash{
			subtree.RootHash(),
		},
		SubtreeSlices: []*util.Subtree{subtree},
	}
	err = b.checkDuplicateTransactions(context.Background())
	_ = err // To stop lint warning
	// TODO reactivate this test when we have a way to check for duplicate transactions
	// require.Error(t, err)
}

type BlobStoreStub struct {
	logger ulogger.Logger
}

func New(logger ulogger.Logger) (*BlobStoreStub, error) {
	logger = logger.New("null")

	return &BlobStoreStub{
		logger: logger,
	}, nil
}

func (n *BlobStoreStub) Health(_ context.Context) (int, string, error) {
	return 0, "BlobStoreStub Store", nil
}

func (n *BlobStoreStub) Close(_ context.Context) error {
	return nil
}

func (n *BlobStoreStub) SetFromReader(_ context.Context, _ []byte, _ io.ReadCloser, _ ...options.Options) error {
	return nil
}

func (n *BlobStoreStub) Set(_ context.Context, _ []byte, _ []byte, _ ...options.Options) error {
	return nil
}

func (n *BlobStoreStub) SetTTL(_ context.Context, _ []byte, _ time.Duration) error {
	return nil
}

func (n *BlobStoreStub) GetIoReader(_ context.Context, _ []byte) (io.ReadCloser, error) {
	path := filepath.Join("testdata", "testSubtreeHex.bin")

	// read the file
	subtreeReader, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %s", err)
	}

	return subtreeReader, nil
}

func (n *BlobStoreStub) Get(_ context.Context, hash []byte) ([]byte, error) {
	path := filepath.Join("testdata", "testSubtreeHex.bin")

	// read the file
	subtreeBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %s", err)
	}

	return subtreeBytes, nil
}

func (n *BlobStoreStub) Exists(_ context.Context, _ []byte) (bool, error) {
	return false, nil
}

func (n *BlobStoreStub) Del(_ context.Context, _ []byte) error {
	return nil
}

package model

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/null"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/stores/utxo/memory"
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
			Height:           800000,
		}

		blockBytes, err := block.Bytes()
		require.NoError(t, err)

		assert.Equal(t, 98, len(blockBytes))
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

	t.Run("test block reader - subtrees", func(t *testing.T) {
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

		buf := bytes.NewReader(blockBytes)
		blockFromBytes, err := NewBlockFromReader(buf)
		require.NoError(t, err)

		assert.Len(t, blockFromBytes.Subtrees, 2)
		assert.Equal(t, block.Subtrees[0].String(), blockFromBytes.Subtrees[0].String())
		assert.Equal(t, block.Subtrees[1].String(), blockFromBytes.Subtrees[1].String())
		assert.Equal(t, uint64(1), block.TransactionCount)
		assert.Equal(t, uint64(215), block.SizeInBytes)
	})

	t.Run("test multiple blocks reader - subtrees", func(t *testing.T) {
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

		blockBytes = append(blockBytes, blockBytes...)
		blockBytes = append(blockBytes, blockBytes...)

		buf := bytes.NewReader(blockBytes)

		// read 4 blocks
		for i := 0; i < 4; i++ {
			blockFromBytes, err := NewBlockFromReader(buf)
			require.NoError(t, err)

			assert.Len(t, blockFromBytes.Subtrees, 2)
			assert.Equal(t, block.Subtrees[0].String(), blockFromBytes.Subtrees[0].String())
			assert.Equal(t, block.Subtrees[1].String(), blockFromBytes.Subtrees[1].String())
			assert.Equal(t, uint64(1), block.TransactionCount)
			assert.Equal(t, uint64(215), block.SizeInBytes)
		}

		// no more blocks to read
		_, err = NewBlockFromReader(buf)
		require.Error(t, err)
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
	txMetaStore := memory.New(ulogger.TestLogger{})

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
	v, err := b.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, txMetaStore, nil, currentChain, currentChainIDs, NewBloomStats())
	require.NoError(t, err)
	require.True(t, v)
}

func TestBlock_ValidBlockWithMultipleTransactions(t *testing.T) {
	util.SkipVeryLongTests(t)
	fileDir = "./test-generated_test_data/"
	fileNameTemplate = fileDir + "subtree-%d.bin"
	fileNameTemplateMerkleHashes = fileDir + "subtree-merkle-hashes.bin"
	fileNameTemplateBlock = fileDir + "block.bin"
	txMetafileNameTemplate = fileDir + "txMeta.bin"
	subtreeStore := newLocalSubtreeStore()
	txCount := uint64(4 * 1024)
	subtreeSize = 1024

	block, err := generateTestBlock(txCount, subtreeStore, true)
	require.NoError(t, err)

	txMetaStore := memory.New(ulogger.TestLogger{})
	cachedTxMetaStore = txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, txMetaStore, 1024)
	err = loadTxMetaIntoMemory()
	require.NoError(t, err)

	// check if the first txid is in the txMetaStore
	reqTxId, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	data, err := cachedTxMetaStore.Get(context.Background(), reqTxId)
	require.NoError(t, err)
	require.Equal(t, &meta.Data{
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
	v, err := block.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, cachedTxMetaStore, nil, currentChain, currentChainIDs, NewBloomStats())
	require.NoError(t, err)
	require.True(t, v)
}

func TestBlock_WithDuplicateTransaction(t *testing.T) {
	//  skip due to size requirements of the cache, use cache size / 1024 and number of buckets / 1024 for testing
	util.SkipVeryLongTests(t)
	leafCount := 8
	subtree, err := util.NewTreeByLeafCount(leafCount)
	require.NoError(t, err)
	subtreeSize = 8

	subtreeStore := newLocalSubtreeStore()
	txMetaStore := memory.New(ulogger.TestLogger{})
	cachedTxMetaStore = txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, txMetaStore, 1024)
	txMetaCache := cachedTxMetaStore.(*txmetacache.TxMetaCache)

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
	nBits := NewNBitFromString("2000ffff")

	// get subtree root hash
	subtreeHash := subtree.RootHash()

	// store subtree hash in the subtreeStore map
	subtreeStore.files[*subtreeHash] = 0

	// create a new subtree for replaced coinbase transaction
	replacedCoinbaseSubtree, err := util.NewTreeByLeafCount(subtreeSize)
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
	_, _ = b.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, cachedTxMetaStore, nil, currentChain, currentChainIDs, NewBloomStats())
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
	err = b.GetAndValidateSubtrees(context.Background(), ulogger.TestLogger{}, mockBlobStore)
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

func TestCheckParentExistsOnChain(t *testing.T) {
	txMetaStore := memory.New(ulogger.TestLogger{})

	blockID1 := uint32(1)
	blockID100 := uint32(100)
	blockID101 := uint32(101)

	txParent := newTx(1)
	tx := newTx(2)

	_, err := txMetaStore.Create(context.Background(), txParent, blockID100)
	require.NoError(t, err)
	_, err = txMetaStore.Create(context.Background(), tx, blockID101)
	require.NoError(t, err)

	currentBlockHeaderIDsMap := make(map[uint32]struct{})
	currentBlockHeaderIDsMap[blockID100] = struct{}{}

	block := &Block{}
	logger := ulogger.TestLogger{}

	t.Run("test parent is in a previous block", func(t *testing.T) {
		parentTxStruct := missingParentTx{
			parentTxHash: *txParent.TxIDChainHash(),
			txHash:       *tx.TxIDChainHash(),
		}

		err = block.checkParentExistsOnChain(context.Background(), logger, txMetaStore, parentTxStruct, currentBlockHeaderIDsMap)
		require.NoError(t, err)
	})

	t.Run("test parent is not in a previous block", func(t *testing.T) {
		// swap parent/tx hashes to simulate a missing parent
		parentTxStruct := missingParentTx{
			parentTxHash: *tx.TxIDChainHash(),
			txHash:       *txParent.TxIDChainHash(),
		}

		err = block.checkParentExistsOnChain(context.Background(), logger, txMetaStore, parentTxStruct, currentBlockHeaderIDsMap)
		require.Error(t, err)
		e := err.(*errors.Error)
		require.Equal(t, errors.ERR_BLOCK_INVALID, e.Code, "expected error code ERR_BLOCK_INVALID")
	})

	t.Run("test parent has no block ID", func(t *testing.T) {
		txParentWithNoBlockID := newTx(3)
		_, err = txMetaStore.Create(context.Background(), txParentWithNoBlockID)
		parentTxStruct := missingParentTx{
			parentTxHash: *txParentWithNoBlockID.TxIDChainHash(),
			txHash:       *tx.TxIDChainHash(),
		}

		err = block.checkParentExistsOnChain(context.Background(), logger, txMetaStore, parentTxStruct, currentBlockHeaderIDsMap)
		require.Error(t, err)
		e := err.(*errors.Error)
		require.Equal(t, errors.ERR_BLOCK_INVALID, e.Code, "expected error code ERR_BLOCK_INVALID")
	})

	t.Run("test parent is not in store so assume is in a previous block", func(t *testing.T) {
		txMissingParent := newTx(999) // don't put this in the store
		parentTxStruct := missingParentTx{
			parentTxHash: *txMissingParent.TxIDChainHash(),
			txHash:       *tx.TxIDChainHash(),
		}

		err = block.checkParentExistsOnChain(context.Background(), logger, txMetaStore, parentTxStruct, currentBlockHeaderIDsMap)
		require.NoError(t, err)
	})

	t.Run("test parent is in store and block ID is < min BlockID of last 100 blocks", func(t *testing.T) {
		txMissingParent := newTx(4)
		_, err = txMetaStore.Create(context.Background(), txMissingParent, blockID1)
		parentTxStruct := missingParentTx{
			parentTxHash: *txMissingParent.TxIDChainHash(),
			txHash:       *tx.TxIDChainHash(),
		}

		err = block.checkParentExistsOnChain(context.Background(), logger, txMetaStore, parentTxStruct, currentBlockHeaderIDsMap)
		require.NoError(t, err)
	})

}

package model

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/go-chaincfg"
	"github.com/bitcoin-sv/teranode/pkg/go-subtree"
	txmap "github.com/bitcoin-sv/teranode/pkg/go-tx-map"
	"github.com/bitcoin-sv/teranode/pkg/go-wire"
	"github.com/bitcoin-sv/teranode/services/legacy/bsvutil"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob/null"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/sql"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlock_Bytes(t *testing.T) {
	hash1, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	hash2, _ := chainhash.NewHashFromStr("000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd")
	coinbaseTx, _ := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")

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

		blockFromBytes, err := NewBlockFromBytes(blockBytes, nil)
		require.NoError(t, err)

		assert.Equal(t, block1Header, hex.EncodeToString(blockFromBytes.Header.Bytes()))
		assert.Equal(t, block.CoinbaseTx.String(), blockFromBytes.CoinbaseTx.String())
		assert.Equal(t, block.TransactionCount, blockFromBytes.TransactionCount)
		assert.Equal(t, block.Subtrees, blockFromBytes.Subtrees)

		assert.Equal(t, "4c74e0128fef1a01469380c05b215afaf4cfe51183461f4a7996a84295b6925a", block.Hash().String())
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

		blockFromBytes, err := NewBlockFromBytes(blockBytes, nil)
		require.NoError(t, err)

		assert.Len(t, blockFromBytes.Subtrees, 2)
		assert.Equal(t, block.Subtrees[0].String(), blockFromBytes.Subtrees[0].String())
		assert.Equal(t, block.Subtrees[1].String(), blockFromBytes.Subtrees[1].String())
		assert.Equal(t, uint64(1), block.TransactionCount)
		assert.Equal(t, uint64(179), block.SizeInBytes)
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
		blockFromBytes, err := NewBlockFromReader(buf, nil)
		require.NoError(t, err)

		assert.Len(t, blockFromBytes.Subtrees, 2)
		assert.Equal(t, block.Subtrees[0].String(), blockFromBytes.Subtrees[0].String())
		assert.Equal(t, block.Subtrees[1].String(), blockFromBytes.Subtrees[1].String())
		assert.Equal(t, uint64(1), block.TransactionCount)
		assert.Equal(t, uint64(179), block.SizeInBytes)
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
			blockFromBytes, err := NewBlockFromReader(buf, nil)
			require.NoError(t, err)

			assert.Len(t, blockFromBytes.Subtrees, 2)
			assert.Equal(t, block.Subtrees[0].String(), blockFromBytes.Subtrees[0].String())
			assert.Equal(t, block.Subtrees[1].String(), blockFromBytes.Subtrees[1].String())
			assert.Equal(t, uint64(1), block.TransactionCount)
			assert.Equal(t, uint64(179), block.SizeInBytes)
		}

		// no more blocks to read
		_, err = NewBlockFromReader(buf, nil)
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

		median, err := CalculateMedianTimestamp(timestamps)
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

		median, err := CalculateMedianTimestamp(timestamps)
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

		median, err := CalculateMedianTimestamp(timestamps)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		if !median.Equal(expected) {
			t.Errorf("Expected median %v, got %v", expected, *median)
		}
	})

	t.Run("test for less than 11 timestamps", func(t *testing.T) {
		expected := timestamps[5]

		median, err := CalculateMedianTimestamp(timestamps[:10])
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

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	b, err := NewBlock(
		blockHeader,
		coinbase,
		[]*chainhash.Hash{},
		1,
		123, 0, 0, nil)
	require.NoError(t, err)

	subtreeStore, _ := null.New(ulogger.TestLogger{})

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	currentChain := make([]*BlockHeader, 11)
	currentChainIDs := make([]uint32, 11)

	for i := 0; i < 11; i++ {
		currentChain[i] = &BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			// set the last 11 block header timestamps to be less than the current timestamps
			Timestamp: 1231469665 - uint32(i), // nolint:gosec
		}
		currentChainIDs[i] = uint32(i) // nolint:gosec
	}

	currentChain[0].HashPrevBlock = &chainhash.Hash{}
	oldBlockIDs := txmap.NewSyncedMap[chainhash.Hash, []uint32]()
	v, err := b.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, utxoStore, oldBlockIDs, nil, currentChain, currentChainIDs, NewBloomStats())
	require.NoError(t, err)
	require.True(t, v)

	_, hasTransactionsReferencingOldBlocks := txmap.ConvertSyncedMapToUint32Slice(oldBlockIDs)
	require.False(t, hasTransactionsReferencingOldBlocks)
}

func TestGetAndValidateSubtrees(t *testing.T) {
	blockHeaderBytes, _ := hex.DecodeString(block1Header)
	blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	subtreeHash, _ := chainhash.NewHashFromStr("9daba5e5c8ecdb80e811ef93558e960a6ffed0c481182bd47ac381547361ff25")

	b, err := NewBlock(blockHeader,
		coinbase,
		[]*chainhash.Hash{
			subtreeHash,
		},
		1,
		123, 0, 0, nil)
	require.NoError(t, err)

	mockBlobStore, _ := New(ulogger.TestLogger{})
	err = b.GetAndValidateSubtrees(context.Background(), ulogger.TestLogger{}, mockBlobStore, nil)
	require.NoError(t, err)
}

func TestCheckDuplicateTransactions(t *testing.T) {
	leafCount := 4
	subtree, err := subtree.NewTreeByLeafCount(leafCount)
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

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	b, err := NewBlock(
		blockHeader,
		coinbase,
		[]*chainhash.Hash{
			subtree.RootHash(),
		},
		1,
		123, 0, 0, nil)
	require.NoError(t, err)

	err = b.checkDuplicateTransactions(context.Background())
	_ = err // To stop lint warning
}

// TODO reactivate this test when we have a way to check for duplicate transactions
// require.Error(t, err)

func TestCheckParentExistsOnChain(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	blockID1 := uint32(1)
	blockID100 := uint32(100)
	blockID101 := uint32(101)

	txParent := newTx(1)
	tx := newTx(2)

	_, err = utxoStore.Create(context.Background(), txParent, blockID100, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 100, BlockHeight: 100}))
	require.NoError(t, err)
	_, err = utxoStore.Create(context.Background(), tx, blockID101, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 101, BlockHeight: 101}))
	require.NoError(t, err)

	currentBlockHeaderIDsMap := make(map[uint32]struct{})
	currentBlockHeaderIDsMap[blockID100] = struct{}{}

	block := &Block{}

	t.Run("test parent is in a previous block", func(t *testing.T) {
		parentTxStruct := missingParentTx{
			parentTxHash: *txParent.TxIDChainHash(),
			txHash:       *tx.TxIDChainHash(),
		}

		oldBlockIDs, err := block.checkParentExistsOnChain(context.Background(), logger, utxoStore, parentTxStruct, currentBlockHeaderIDsMap)
		require.NoError(t, err)
		require.True(t, len(oldBlockIDs) == 0)
	})

	t.Run("test parent is not in a previous block", func(t *testing.T) {
		// swap parent/tx hashes to simulate a missing parent
		parentTxStruct := missingParentTx{
			parentTxHash: *tx.TxIDChainHash(),
			txHash:       *txParent.TxIDChainHash(),
		}

		oldBlockIDs, err := block.checkParentExistsOnChain(context.Background(), logger, utxoStore, parentTxStruct, currentBlockHeaderIDsMap)
		require.Error(t, err)
		require.True(t, len(oldBlockIDs) == 0)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid))
	})

	t.Run("test parent has no block ID", func(t *testing.T) {
		txParentWithNoBlockID := newTx(3)
		_, err = utxoStore.Create(context.Background(), txParentWithNoBlockID, 0)
		parentTxStruct := missingParentTx{
			parentTxHash: *txParentWithNoBlockID.TxIDChainHash(),
			txHash:       *tx.TxIDChainHash(),
		}

		oldBlockIDs, err := block.checkParentExistsOnChain(context.Background(), logger, utxoStore, parentTxStruct, currentBlockHeaderIDsMap)
		require.Error(t, err)
		require.True(t, len(oldBlockIDs) == 0)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid))
	})

	t.Run("test parent is not in store so assume is in a previous block", func(t *testing.T) {
		txMissingParent := newTx(999) // don't put this in the store
		parentTxStruct := missingParentTx{
			parentTxHash: *txMissingParent.TxIDChainHash(),
			txHash:       *tx.TxIDChainHash(),
		}

		oldBlockIDs, err := block.checkParentExistsOnChain(context.Background(), logger, utxoStore, parentTxStruct, currentBlockHeaderIDsMap)
		require.True(t, len(oldBlockIDs) == 0)
		require.NoError(t, err)
	})

	t.Run("test parent is in store and block ID is < min BlockID of last 100 blocks", func(t *testing.T) {
		txMissingParent := newTx(4)
		_, err = utxoStore.Create(context.Background(), txMissingParent, blockID1, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1}))
		parentTxStruct := missingParentTx{
			parentTxHash: *txMissingParent.TxIDChainHash(),
			txHash:       *tx.TxIDChainHash(),
		}

		oldBlockIDs, err := block.checkParentExistsOnChain(context.Background(), logger, utxoStore, parentTxStruct, currentBlockHeaderIDsMap)
		require.True(t, len(oldBlockIDs) > 0)
		require.NoError(t, err)
	})
}

var blockBytesForBenchmark, _ = hex.DecodeString("010000006fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d6190000000000982051fd1e4ba744bbbe680e1fee14677ba1a3c3540bf7b1cdb606e857233e0e61bc6649ffff001d01e3629901d7026fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d6190000000000bddd99ccfda39da1b108ce1a5d70038d0a967bacb68b6b63065f626a0000000001000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac0000000000")

func Benchmark_NewBlockFromBytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = NewBlockFromBytes(blockBytesForBenchmark, nil)
	}
}

func TestT(t *testing.T) {
	tx := &bt.Tx{}

	b := tx.Bytes()

	tx2, err := bt.NewTxFromBytes(b)
	require.NoError(t, err)

	assert.Equal(t, tx, tx2)
	// t.Logf("%x", tx.Bytes())
	// t.Logf("%x", tx2.Bytes())

	assert.True(t, tx.TxIDChainHash().Equal(*emptyTX.TxIDChainHash()))
	assert.True(t, tx2.TxIDChainHash().Equal(*emptyTX.TxIDChainHash()))
}

// tests for msgBlock
func TestNewBlockFromMsgBlock(t *testing.T) {
	t.Run("test NewBlockFromMsgBlock", func(t *testing.T) {
		// Create a mock wire.MsgBlock
		prevBlockHash, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
		merkleRootHash, _ := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")

		msgBlock := &wire.MsgBlock{
			Header: wire.BlockHeader{
				Version:    1,
				PrevBlock:  *prevBlockHash,
				MerkleRoot: *merkleRootHash,
				Timestamp:  time.Unix(1231006505, 0),
				Bits:       0x1d00ffff,
				Nonce:      2083236893,
			},
			Transactions: []*wire.MsgTx{
				{
					Version: 1,
					TxIn: []*wire.TxIn{
						{
							PreviousOutPoint: wire.OutPoint{
								Hash:  chainhash.Hash{},
								Index: 0xffffffff,
							},
							SignatureScript: []byte{0x04, 0xff, 0xff, 0x00, 0x1d, 0x01, 0x04},
							Sequence:        0xffffffff,
						},
					},
					TxOut: []*wire.TxOut{
						{
							Value:    5000000000,
							PkScript: []byte{0x41, 0x04, 0x67, 0x8a, 0xfd, 0xb0},
						},
					},
					LockTime: 0,
				},
			},
		}

		// Call the function
		block, err := NewBlockFromMsgBlock(msgBlock, nil)

		// Assert no error
		assert.NoError(t, err)

		expectedBits, err := NewNBitFromString("1d00ffff")
		assert.NoError(t, err)

		// Assert block properties
		assert.Equal(t, uint32(1), block.Header.Version)
		assert.Equal(t, prevBlockHash, block.Header.HashPrevBlock)
		assert.Equal(t, merkleRootHash, block.Header.HashMerkleRoot)
		assert.Equal(t, uint32(1231006505), block.Header.Timestamp)
		assert.Equal(t, *expectedBits, block.Header.Bits)
		assert.Equal(t, uint32(2083236893), block.Header.Nonce)

		assert.Equal(t, uint64(1), block.TransactionCount)
		assert.NotNil(t, block.CoinbaseTx)
		assert.Equal(t, uint64(msgBlock.SerializeSize()), block.SizeInBytes) // nolint: gosec
		assert.Empty(t, block.Subtrees)
	})

	t.Run("test NewBlockFromMsgBlock incorrect merkle root", func(t *testing.T) {
		msgBlock, err := os.ReadFile("./testdata/000000000e511cb16e3a0dda35c9cf813f6f020d3e42394623b12ba2a8f73b8a.msgBlock")
		require.NoError(t, err)

		reader := bytes.NewReader(msgBlock)

		block, err := bsvutil.NewBlockFromReader(reader)
		require.NoError(t, err)

		assert.NotNil(t, block)

		coinbaseTxStr := block.MsgBlock().Transactions[0].TxHash().String()
		assert.NotNil(t, coinbaseTxStr)
	})
}

func TestNewBlockFromMsgBlockAndModelBlock(t *testing.T) {
	blockHeaderBytes, err := hex.DecodeString(block1Header)
	require.NoError(t, err)

	// Create a wire.BlockHeader from block1Header string
	var wireBlockHeader wire.BlockHeader
	err = wireBlockHeader.Deserialize(bytes.NewReader(blockHeaderBytes))
	require.NoError(t, err)

	// create a model.blockheader
	modelBlockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)

	// Assert block properties
	assert.Equal(t, modelBlockHeader.Version, uint32(wireBlockHeader.Version)) // nolint: gosec
	assert.Equal(t, modelBlockHeader.Bits.String(), fmt.Sprintf("%x", wireBlockHeader.Bits))
	assert.Equal(t, modelBlockHeader.Nonce, wireBlockHeader.Nonce)
	assert.Equal(t, *modelBlockHeader.HashMerkleRoot, wireBlockHeader.MerkleRoot)
	assert.Equal(t, modelBlockHeader.Timestamp, uint32(wireBlockHeader.Timestamp.Unix())) // nolint: gosec
}

func TestGenesisBytesFromModelBlock(t *testing.T) {
	expectedPrevBlockHash := "0000000000000000000000000000000000000000000000000000000000000000"

	wireGenesisBlock := chaincfg.MainNetParams.GenesisBlock

	genesisBlock, err := NewBlockFromMsgBlock(wireGenesisBlock, nil)
	if err != nil {
		t.Fatalf("Failed to create new block from bytes: %v", err)
	}

	if genesisBlock.Header.HashPrevBlock.String() != expectedPrevBlockHash {
		t.Fatalf("Genesis hash mismatch:\nexpected: %s\ngot:      %s", expectedPrevBlockHash, genesisBlock.Header.HashPrevBlock.String())
	}

	bitsBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bitsBytes, wireGenesisBlock.Header.Bits)

	nbits, err := NewNBitFromSlice(bitsBytes)
	if err != nil {
		t.Fatalf("failed to create NBit from Bits: %v", err)
	}

	if genesisBlock.Header.Bits != *nbits {
		t.Fatalf("Genesis hash mismatch:\nexpected: %s\ngot:      %s", expectedPrevBlockHash, genesisBlock.Header.HashPrevBlock.String())
	}
}

func TestNewBlockSettings(t *testing.T) {
	blockHeaderBytes, _ := hex.DecodeString(block1Header)
	blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	tSettings := &settings.Settings{
		ChainCfgParams: &chaincfg.RegressionNetParams,
	}

	b, err := NewBlock(
		blockHeader,
		coinbase,
		[]*chainhash.Hash{},
		0, 0, 0, 0,
		tSettings)
	require.NoError(t, err)

	assert.Equal(t, b.settings, tSettings)
}

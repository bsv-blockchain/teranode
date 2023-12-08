package model

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/null"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	coinbaseTx, _ = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000")
	hash1, _      = chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	hash2, _      = chainhash.NewHashFromStr("000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd")
)

func TestBlock_Bytes(t *testing.T) {
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
	now := time.Now()
	for i := range timestamps {
		timestamps[i] = now.Add(time.Duration(i) * time.Hour)
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
		expected := timestamps[4]
		// add a new timestamp out of sequence
		now = time.Now()
		timestamps[5] = now
		median, err := medianTimestamp(timestamps)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if !median.Equal(expected) {
			t.Errorf("Expected median %v, got %v", expected, *median)
		}
	})
	t.Run("test for error when not enough timestamps", func(t *testing.T) {
		_, err := medianTimestamp(timestamps[:10])
		if err == nil {
			t.Errorf("Expected error for insufficient timestamps, got none")
		}
	})
}

func TestBlock_Valid(t *testing.T) {

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
	for i := 0; i < 11; i++ {
		currentChain[i] = &BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			// set the last 11 block header timestamps to be less than the current timestamps
			Timestamp: 1231469665 - uint32(i),
		}
	}
	currentChain[0].HashPrevBlock = &chainhash.Hash{}
	v, err := b.Valid(context.Background(), subtreeStore, txMetaStore, currentChain)
	require.NoError(t, err)
	require.True(t, v)

}

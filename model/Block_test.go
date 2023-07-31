package model

import (
	"encoding/hex"
	"testing"

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
			Subtrees:         []*chainhash.Hash{},
		}

		blockBytes, err := block.Bytes()
		require.NoError(t, err)

		assert.Equal(t, 92, len(blockBytes))
	})

	t.Run("test block bytes", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block1Header)
		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		require.NoError(t, err)

		block := &Block{
			Header:           blockHeader,
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
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

		assert.NoError(t, block.CheckMerkleRoot())
	})

	t.Run("test block bytes - subtrees", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block1Header)
		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		require.NoError(t, err)

		block := &Block{
			Header:           blockHeader,
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
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
	})
}

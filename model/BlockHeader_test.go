package model

import (
	"bytes"
	"encoding/hex"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
)

var (
	block1       = "010000006fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d6190000000000982051fd1e4ba744bbbe680e1fee14677ba1a3c3540bf7b1cdb606e857233e0e61bc6649ffff001d01e362990101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
	block1Header = block1[:160]
)

func TestNewBlockHeaderFromBytes(t *testing.T) {
	t.Run("block 1 from bytes", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block1Header)
		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, uint32(1), blockHeader.Version)
		assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", blockHeader.HashPrevBlock.String())
		assert.Equal(t, "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098", blockHeader.HashMerkleRoot.String())
		assert.Equal(t, uint32(1231469665), blockHeader.Timestamp)
		assert.Equal(t, "1d00ffff", blockHeader.Bits.String())
		assert.Equal(t, uint32(2573394689), blockHeader.Nonce)
	})

	t.Run("block 0 from string", func(t *testing.T) {
		blockHeader, err := NewBlockHeaderFromString(block1Header)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, uint32(1), blockHeader.Version)
		assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", blockHeader.HashPrevBlock.String())
		assert.Equal(t, "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098", blockHeader.HashMerkleRoot.String())
		assert.Equal(t, uint32(1231469665), blockHeader.Timestamp)
		assert.Equal(t, "1d00ffff", blockHeader.Bits.String())
		assert.Equal(t, uint32(2573394689), blockHeader.Nonce)
	})

	t.Run("block 1 bytes", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block1Header)
		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, blockHeaderBytes, blockHeader.Bytes())
		assert.Equal(t, "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048", blockHeader.Hash().String())
	})

	t.Run("block hash", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block1Header)
		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048", blockHeader.Hash().String())
	})

	t.Run("block hash - block 1", func(t *testing.T) {
		hashPrevBlock, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		hashMerkleRoot, _ := chainhash.NewHashFromStr("0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098")
		nBits := NewNBitFromString("1d00ffff")
		blockHeader := &BlockHeader{
			Version:        1,
			HashPrevBlock:  hashPrevBlock,
			HashMerkleRoot: hashMerkleRoot,
			Timestamp:      1231469665,
			Bits:           nBits,
			Nonce:          2573394689,
		}

		assert.Equal(t, "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048", blockHeader.Hash().String())
	})
}

func Test_ToWireBlockHeader(t *testing.T) {
	t.Run("block 1", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block1Header)
		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		require.NoError(t, err)

		// reader to bytes
		reader := bytes.NewReader(blockHeaderBytes)
		w := wire.BlockHeader{}
		err = w.Deserialize(reader)
		require.NoError(t, err)

		wireBlockHeader := blockHeader.ToWireBlockHeader()
		assert.Equal(t, blockHeader.Hash().String(), wireBlockHeader.BlockHash().String())
	})
}

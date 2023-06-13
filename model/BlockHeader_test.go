package model

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	block0Header = "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c"
)

func TestNewBlockHeaderFromBytes(t *testing.T) {
	t.Run("block 0 from bytes", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block0Header)
		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, uint32(1), blockHeader.Version)
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", blockHeader.HashPrevBlock.String())
		assert.Equal(t, "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b", blockHeader.HashMerkleRoot.String())
		assert.Equal(t, uint32(1231006505), blockHeader.Timestamp)
		assert.Equal(t, "1d00ffff", hex.EncodeToString(blockHeader.Bits))
		assert.Equal(t, uint32(2083236893), blockHeader.Nonce)
	})

	t.Run("block 0 from string", func(t *testing.T) {
		blockHeader, err := NewBlockHeaderFromString(block0Header)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, uint32(1), blockHeader.Version)
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", blockHeader.HashPrevBlock.String())
		assert.Equal(t, "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b", blockHeader.HashMerkleRoot.String())
		assert.Equal(t, uint32(1231006505), blockHeader.Timestamp)
		assert.Equal(t, "1d00ffff", hex.EncodeToString(blockHeader.Bits))
		assert.Equal(t, uint32(2083236893), blockHeader.Nonce)
	})

	t.Run("block bytes", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block0Header)
		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, blockHeaderBytes, blockHeader.Bytes())
	})

	t.Run("block hash", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block0Header)
		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", blockHeader.Hash().String())
	})
}

package model

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/stretchr/testify/require"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
)

var (
	block1       = "0000002006226e46111a0b59caaf126043eb5bbf28c34f3a5e332a1fc7b2b73cf188910f1633819a69afbd7ce1f1a01c3b786fcbb023274f3b15172b24feadd4c80e6c6a8b491267ffff7f20040000000102000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000"
	block1Header = block1[:160]
)

func TestNewBlockHeaderFromBytes(t *testing.T) {
	t.Run("block 1 from bytes", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block1Header)
		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, uint32(0x20000000), blockHeader.Version)
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", blockHeader.HashPrevBlock.String())
		assert.Equal(t, "6a6c0ec8d4adfe242b17153b4f2723b0cb6f783b1ca0f1e17cbdaf699a813316", blockHeader.HashMerkleRoot.String())
		assert.Equal(t, uint32(1729251723), blockHeader.Timestamp)
		assert.Equal(t, "207fffff", blockHeader.Bits.String())
		assert.Equal(t, uint32(4), blockHeader.Nonce)
	})

	t.Run("block 0 from string", func(t *testing.T) {
		blockHeader, err := NewBlockHeaderFromString(block1Header)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, uint32(0x20000000), blockHeader.Version)
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", blockHeader.HashPrevBlock.String())
		assert.Equal(t, "6a6c0ec8d4adfe242b17153b4f2723b0cb6f783b1ca0f1e17cbdaf699a813316", blockHeader.HashMerkleRoot.String())
		assert.Equal(t, uint32(1729251723), blockHeader.Timestamp)
		assert.Equal(t, "207fffff", blockHeader.Bits.String())
		assert.Equal(t, uint32(4), blockHeader.Nonce)
	})

	t.Run("block 1 bytes", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block1Header)
		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, blockHeaderBytes, blockHeader.Bytes())
		assert.Equal(t, "4c74e0128fef1a01469380c05b215afaf4cfe51183461f4a7996a84295b6925a", blockHeader.Hash().String())
	})

	t.Run("block hash", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block1Header)
		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, "4c74e0128fef1a01469380c05b215afaf4cfe51183461f4a7996a84295b6925a", blockHeader.Hash().String())
	})

	t.Run("block hash - block 1", func(t *testing.T) {
		hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
		hashMerkleRoot, _ := chainhash.NewHashFromStr("6a6c0ec8d4adfe242b17153b4f2723b0cb6f783b1ca0f1e17cbdaf699a813316")
		nBits, _ := NewNBitFromString("207fffff")
		blockHeader := &BlockHeader{
			Version:        0x20000000,
			HashPrevBlock:  hashPrevBlock,
			HashMerkleRoot: hashMerkleRoot,
			Timestamp:      1729251723,
			Bits:           *nBits,
			Nonce:          4,
		}

		assert.Equal(t, "4c74e0128fef1a01469380c05b215afaf4cfe51183461f4a7996a84295b6925a", blockHeader.Hash().String())
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

// Package utxopersister provides functionality for managing UTXO (Unspent Transaction Output) persistence.
package utxopersister

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadWrite(t *testing.T) {
	h := chainhash.HashH([]byte("hash"))

	p := chainhash.HashH([]byte("prev"))

	m := chainhash.HashH([]byte("merkle"))

	b, err := model.NewNBitFromString("00112233")
	require.NoError(t, err)

	bi := &BlockIndex{
		Hash:    &h,
		Height:  42,
		TxCount: 1001,
		BlockHeader: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &p,
			HashMerkleRoot: &m,
			Timestamp:      123456789,
			Bits:           *b,
			Nonce:          123456789,
		},
	}

	t.Run("V2 format (with coinbase field)", func(t *testing.T) {
		buf := bytes.NewBuffer(nil)

		err = bi.Serialise(buf)
		require.NoError(t, err)

		// Test V2 format - coinbase field is present but empty (length=0)
		bi2, err := NewUTXOHeaderFromReader(buf, false)
		require.NoError(t, err)

		assert.Equal(t, bi.Hash, bi2.Hash)
		assert.Equal(t, bi.Height, bi2.Height)
		assert.Equal(t, bi.TxCount, bi2.TxCount)
		assert.Equal(t, bi.BlockHeader.Version, bi2.BlockHeader.Version)
		assert.Equal(t, bi.BlockHeader.HashPrevBlock, bi2.BlockHeader.HashPrevBlock)
		assert.Equal(t, bi.BlockHeader.HashMerkleRoot, bi2.BlockHeader.HashMerkleRoot)
		assert.Equal(t, bi.BlockHeader.Timestamp, bi2.BlockHeader.Timestamp)
		assert.Equal(t, bi.BlockHeader.Bits, bi2.BlockHeader.Bits)
		assert.Equal(t, bi.BlockHeader.Nonce, bi2.BlockHeader.Nonce)
		assert.Nil(t, bi2.CoinbaseTx)
	})

	t.Run("V1 format (without coinbase field)", func(t *testing.T) {
		buf := bytes.NewBuffer(nil)

		// Write only the V1 fields (no coinbase)
		require.NoError(t, buf.WriteByte(0)) // Write hash
		_, err := buf.Write(bi.Hash[:])
		require.NoError(t, err)

		var heightBytes [4]byte
		binary.LittleEndian.PutUint32(heightBytes[:], bi.Height)
		_, err = buf.Write(heightBytes[:])
		require.NoError(t, err)

		var txCountBytes [8]byte
		binary.LittleEndian.PutUint64(txCountBytes[:], bi.TxCount)
		_, err = buf.Write(txCountBytes[:])
		require.NoError(t, err)

		err = bi.BlockHeader.ToWireBlockHeader().Serialize(buf)
		require.NoError(t, err)

		// Read back skipping the first byte we added
		_, _ = buf.ReadByte()

		// Test V1 format - no coinbase field in file
		bi2, err := NewUTXOHeaderFromReader(buf, true)
		require.NoError(t, err)

		assert.Equal(t, bi.Hash, bi2.Hash)
		assert.Equal(t, bi.Height, bi2.Height)
		assert.Equal(t, bi.TxCount, bi2.TxCount)
		assert.Equal(t, bi.BlockHeader.Version, bi2.BlockHeader.Version)
		assert.Equal(t, bi.BlockHeader.HashPrevBlock, bi2.BlockHeader.HashPrevBlock)
		assert.Equal(t, bi.BlockHeader.HashMerkleRoot, bi2.BlockHeader.HashMerkleRoot)
		assert.Equal(t, bi.BlockHeader.Timestamp, bi2.BlockHeader.Timestamp)
		assert.Equal(t, bi.BlockHeader.Bits, bi2.BlockHeader.Bits)
		assert.Equal(t, bi.BlockHeader.Nonce, bi2.BlockHeader.Nonce)
		assert.Nil(t, bi2.CoinbaseTx)
	})
}

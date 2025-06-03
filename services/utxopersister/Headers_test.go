// Package utxopersister provides functionality for managing UTXO (Unspent Transaction Output) persistence.
package utxopersister

import (
	"bytes"
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/libsv/go-bt/v2/chainhash"
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

	buf := bytes.NewBuffer(nil)

	err = bi.Serialise(buf)
	require.NoError(t, err)

	bi2, err := NewUTXOHeaderFromReader(buf)
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
}

// Package utxopersister provides functionality for managing UTXO (Unspent Transaction Output) persistence.
package utxopersister

import (
	"bytes"
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/libsv/go-bt/v2/chainhash"
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

	require.Equal(t, bi.Hash, bi2.Hash)
	require.Equal(t, bi.Height, bi2.Height)
	require.Equal(t, bi.TxCount, bi2.TxCount)
	require.Equal(t, bi.BlockHeader.Version, bi2.BlockHeader.Version)
	require.Equal(t, bi.BlockHeader.HashPrevBlock, bi2.BlockHeader.HashPrevBlock)
	require.Equal(t, bi.BlockHeader.HashMerkleRoot, bi2.BlockHeader.HashMerkleRoot)
	require.Equal(t, bi.BlockHeader.Timestamp, bi2.BlockHeader.Timestamp)
	require.Equal(t, bi.BlockHeader.Bits, bi2.BlockHeader.Bits)
	require.Equal(t, bi.BlockHeader.Nonce, bi2.BlockHeader.Nonce)
}

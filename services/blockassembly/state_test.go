package blockassembly

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeState(t *testing.T) {
	prev, err := chainhash.NewHashFromStr("00000000000000000002a7c4c1e48d76c5a37902165a9fe9a6a4476fc0c9e2a9")
	require.NoError(t, err)

	merkle, err := chainhash.NewHashFromStr("0d8f8a3d3b6a7a1b0c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7")
	require.NoError(t, err)

	bits, err := model.NewNBitFromString("1d00ffff")
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prev,
		HashMerkleRoot: merkle,
		Timestamp:      1700000000,
		Bits:           *bits,
		Nonce:          123456789,
	}

	const height = uint32(300)

	encoded := EncodeState(header, height)
	require.Len(t, encoded, 4+80, "state must be 4-byte height + 80-byte header")

	gotHeader, gotHeight, err := DecodeState(encoded)
	require.NoError(t, err)
	require.Equal(t, height, gotHeight)
	require.Equal(t, header.Hash().String(), gotHeader.Hash().String())
	require.Equal(t, header.Bytes(), gotHeader.Bytes())
}

func TestDecodeState_ShortData(t *testing.T) {
	_, _, err := DecodeState([]byte{0x01, 0x02})
	require.Error(t, err)

	_, _, err = DecodeState(nil)
	require.Error(t, err)
}

package mining

import (
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildBlockHeader(t *testing.T) {
	candidate := &model.MiningCandidate{
		Version:      2,
		PreviousHash: subtree.CoinbasePlaceholderHash.CloneBytes(),
		MerkleProof:  [][]byte{subtree.CoinbasePlaceholderHash.CloneBytes()},
		Time:         123456789,
		NBits:        []byte{0x1D, 0x00, 0x00, 0x00},
	}

	t.Run("block header", func(t *testing.T) {
		coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
		require.NoError(t, err)

		solution := &model.MiningSolution{
			Version:  &candidate.Version,
			Time:     &candidate.Time,
			Nonce:    12345678,
			Coinbase: coinbaseTx.Bytes(),
		}

		header, err := BuildBlockHeader(candidate, solution)
		require.NoError(t, err)

		assert.Len(t, header, model.BlockHeaderSize)

		bh, err := model.NewBlockHeaderFromBytes(header)
		require.NoError(t, err)

		assert.Equal(t, bh.Version, candidate.Version)
		assert.Equal(t, bh.HashPrevBlock.CloneBytes(), candidate.PreviousHash)
		assert.Equal(t, bh.Timestamp, candidate.Time)
	})

	t.Run("invalid coinbase tx", func(t *testing.T) {
		solution := &model.MiningSolution{
			Version:  &candidate.Version,
			Time:     &candidate.Time,
			Nonce:    12345678,
			Coinbase: []byte("invalid coinbase tx"),
		}

		_, err := BuildBlockHeader(candidate, solution)
		assert.Error(t, err)
	})
}

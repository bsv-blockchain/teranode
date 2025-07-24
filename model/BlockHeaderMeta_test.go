package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewBlockHeaderMetaFromBytes(t *testing.T) {
	t.Run("empty bytes", func(t *testing.T) {
		_, err := NewBlockHeaderMetaFromBytes([]byte{})
		require.Error(t, err)
	})

	t.Run("too small", func(t *testing.T) {
		_, err := NewBlockHeaderMetaFromBytes([]byte{0, 1, 2, 3})
		require.Error(t, err)
	})

	t.Run("valid bytes", func(t *testing.T) {
		bhm := &BlockHeaderMeta{
			ID:          1,
			Height:      2,
			TxCount:     3,
			SizeInBytes: 4,
			Miner:       "test_miner",
			PeerID:      "test_peer",
			BlockTime:   5,
			Timestamp:   6,
			ChainWork:   []byte{7, 8, 9, 10},
		}

		b := bhm.Bytes()

		meta, err := NewBlockHeaderMetaFromBytes(b)
		require.NoError(t, err)

		require.Equal(t, bhm.ID, meta.ID)
		require.Equal(t, bhm.Height, meta.Height)
		require.Equal(t, bhm.TxCount, meta.TxCount)
		require.Equal(t, bhm.SizeInBytes, meta.SizeInBytes)
		require.Equal(t, bhm.Miner, meta.Miner)
		require.Equal(t, bhm.PeerID, meta.PeerID)
		require.Equal(t, bhm.BlockTime, meta.BlockTime)
		require.Equal(t, bhm.Timestamp, meta.Timestamp)
		require.Equal(t, bhm.ChainWork, meta.ChainWork)

		require.NoError(t, err)
		require.Equal(t, b, meta.Bytes())
	})

	t.Run("no chainwork", func(t *testing.T) {
		bhm := &BlockHeaderMeta{
			ID:          1,
			Height:      2,
			TxCount:     3,
			SizeInBytes: 4,
			Miner:       "test_miner",
			PeerID:      "test_peer",
			BlockTime:   5,
			Timestamp:   6,
			ChainWork:   nil, // No chainwork
		}

		b := bhm.Bytes()

		meta, err := NewBlockHeaderMetaFromBytes(b)
		require.NoError(t, err)

		require.Equal(t, bhm.ID, meta.ID)
		require.Equal(t, bhm.Height, meta.Height)
		require.Equal(t, bhm.TxCount, meta.TxCount)
		require.Equal(t, bhm.SizeInBytes, meta.SizeInBytes)
		require.Equal(t, bhm.Miner, meta.Miner)
		require.Equal(t, bhm.PeerID, meta.PeerID)
		require.Equal(t, bhm.BlockTime, meta.BlockTime)
		require.Equal(t, bhm.Timestamp, meta.Timestamp)
		require.Nil(t, meta.ChainWork)

		require.NoError(t, err)
		require.Equal(t, b, meta.Bytes())
	})
}

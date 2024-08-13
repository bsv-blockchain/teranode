package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSqlGetChainTip(t *testing.T) {
	t.Run("block 0 - genesis block", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeUrl)
		require.NoError(t, err)

		tip, meta, err := s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta.Height)
		assert.Equal(t, uint32(1), tip.Version)

		assertGenesis(t, tip)
	})

	t.Run("multiple blocks", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeUrl)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		tip, meta, err := s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)

		// block 2 should be the tip
		assert.Equal(t, uint32(2), meta.Height)
		assert.Equal(t, "000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd", tip.Hash().String())
		assert.Equal(t, uint32(1), tip.Version)
		assert.Equal(t, *block1.Header.Hash(), *tip.HashPrevBlock)
		assert.Equal(t, "9b0fc92260312ce44e74ef369f5c66bbb85848f2eddd5a7a1cde251e54ccfdd5", tip.HashMerkleRoot.String())
		assert.Equal(t, uint32(1231469744), tip.Timestamp)
		assert.Equal(t, []byte{0xff, 0xff, 0x0, 0x1d}, tip.Bits.CloneBytes())
		assert.Equal(t, uint32(1639830024), tip.Nonce)
	})

	t.Run("multiple tips", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeUrl)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		tip, meta, err := s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)

		// block 2 (000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd) should be the tip
		assert.Equal(t, uint32(2), meta.Height)
		assert.Equal(t, "000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd", tip.Hash().String())

		// add a block that should not become the new tip
		_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "")
		require.NoError(t, err)

		tip, meta, err = s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)

		// block 2 (000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd) should still be the tip
		assert.Equal(t, uint32(2), meta.Height)
		assert.Equal(t, "000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd", tip.Hash().String())
		assert.Equal(t, uint32(1), tip.Version)
		assert.Equal(t, *block1.Header.Hash(), *tip.HashPrevBlock)
		assert.Equal(t, "9b0fc92260312ce44e74ef369f5c66bbb85848f2eddd5a7a1cde251e54ccfdd5", tip.HashMerkleRoot.String())
		assert.Equal(t, uint32(1231469744), tip.Timestamp)
		assert.Equal(t, []byte{0xff, 0xff, 0x0, 0x1d}, tip.Bits.CloneBytes())
		assert.Equal(t, uint32(1639830024), tip.Nonce)
	})
}

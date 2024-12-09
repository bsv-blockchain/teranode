package sql

import (
	"context"
	"encoding/hex"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSqlGetChainTip(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	t.Run("block 0 - genesis block", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		tip, meta, err := s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta.Height)
		assert.Equal(t, uint32(1), tip.Version)

		assertRegtestGenesis(t, tip)
	})

	t.Run("multiple blocks", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)

		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		tip, meta, err := s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)

		// block 2 should be the tip
		assert.Equal(t, uint32(2), meta.Height)
		assert.Equal(t, "484e58c7bf0208d787314710535ef7be8ca31748bc9fef5e1ee2de67ebda757a", tip.Hash().String())
		assert.Equal(t, uint32(1), tip.Version)
		assert.Equal(t, *block1.Header.Hash(), *tip.HashPrevBlock)
		assert.Equal(t, "b881339d3b500bcaceb5d2f1225f45edd77e846805ddffe27788fc06f218f177", tip.HashMerkleRoot.String())
		assert.Equal(t, uint32(0x671268cf), tip.Timestamp)
		assert.Equal(t, []byte{0xff, 0xff, 0x7f, 0x20}, tip.Bits.CloneBytes())
		assert.Equal(t, uint32(0x1), tip.Nonce)
		assert.Equal(t, "0600000000000000000000000000000000000000000000000000000000000000", hex.EncodeToString(meta.ChainWork))
	})

	t.Run("multiple tips", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)

		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		tip, meta, err := s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)

		// block 2 (000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd) should be the tip
		assert.Equal(t, uint32(2), meta.Height)
		assert.Equal(t, "484e58c7bf0208d787314710535ef7be8ca31748bc9fef5e1ee2de67ebda757a", tip.Hash().String())

		// add a block that should not become the new tip
		_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "")
		require.NoError(t, err)

		tip, meta, err = s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)

		// block 2 (000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd) should still be the tip
		assert.Equal(t, uint32(2), meta.Height)
		assert.Equal(t, "484e58c7bf0208d787314710535ef7be8ca31748bc9fef5e1ee2de67ebda757a", tip.Hash().String())
		assert.Equal(t, uint32(1), tip.Version)
		assert.Equal(t, *block1.Header.Hash(), *tip.HashPrevBlock)
		assert.Equal(t, "b881339d3b500bcaceb5d2f1225f45edd77e846805ddffe27788fc06f218f177", tip.HashMerkleRoot.String())
		assert.Equal(t, uint32(0x671268cf), tip.Timestamp)
		assert.Equal(t, []byte{0xff, 0xff, 0x7f, 0x20}, tip.Bits.CloneBytes())
		assert.Equal(t, uint32(0x1), tip.Nonce)
	})
}

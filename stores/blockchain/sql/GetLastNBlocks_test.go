package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLGetLastNBlocks(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	t.Run("get last blocks with empty chain", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		blocks, err := s.GetLastNBlocks(context.Background(), 10, false, 0)
		require.NoError(t, err)
		assert.Len(t, blocks, 1) // Genesis block
	})

	t.Run("get last blocks with chain", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store blocks 1, 2, and 3
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		// Get last 2 blocks
		blocks, err := s.GetLastNBlocks(context.Background(), 2, false, 0)
		require.NoError(t, err)
		assert.Len(t, blocks, 2)
		assert.Equal(t, block3.Header.Bytes(), blocks[0].BlockHeader)
		assert.Equal(t, block2.Header.Bytes(), blocks[1].BlockHeader)
	})

	t.Run("get last blocks with fromHeight", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store blocks 1, 2, and 3
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		// Get blocks from height 1
		blocks, err := s.GetLastNBlocks(context.Background(), 10, false, 2)
		require.NoError(t, err)
		assert.Len(t, blocks, 3)
		assert.Equal(t, uint32(2), blocks[0].Height)
		assert.Equal(t, uint32(1), blocks[1].Height)
		assert.Equal(t, uint32(0), blocks[2].Height)
	})
}

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

func TestSQLGetBlocks(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	t.Run("get blocks from genesis", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings.ChainCfgParams)
		require.NoError(t, err)

		// Store blocks 1, 2, and 3
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		// Get 3 blocks starting from block1
		blocks, err := s.GetBlocks(context.Background(), block3.Hash(), 3)
		require.NoError(t, err)
		assert.Len(t, blocks, 3)
		assert.Equal(t, block3.Hash().String(), blocks[0].Hash().String())
		assert.Equal(t, block2.Hash().String(), blocks[1].Hash().String())
		assert.Equal(t, block1.Hash().String(), blocks[2].Hash().String())
	})

	t.Run("get blocks with invalid hash", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings.ChainCfgParams)
		require.NoError(t, err)

		// Try to get blocks from a non-existent hash
		blocks, err := s.GetBlocks(context.Background(), block1.Hash(), 3)
		require.NoError(t, err)
		assert.NotNil(t, blocks)
		assert.Len(t, blocks, 0)
	})

	t.Run("get blocks with zero count", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings.ChainCfgParams)
		require.NoError(t, err)

		// Store block1
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		// Try to get 0 blocks
		blocks, err := s.GetBlocks(context.Background(), block1.Hash(), 0)
		require.NoError(t, err)
		assert.Empty(t, blocks)
	})
}

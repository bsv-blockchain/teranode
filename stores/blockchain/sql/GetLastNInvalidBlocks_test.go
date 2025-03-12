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

func TestGetLastNInvalidBlocks(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	// Note: block1, block2, block3 are defined in sql_test.go as package variables

	t.Run("get invalid blocks with empty chain", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Verify we have no invalid blocks initially
		invalidBlocks, err := s.GetLastNInvalidBlocks(context.Background(), 10)
		require.NoError(t, err)
		assert.Empty(t, invalidBlocks)
	})

	t.Run("get invalid blocks after invalidation", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store blocks
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		// Verify we have no invalid blocks initially
		invalidBlocks, err := s.GetLastNInvalidBlocks(context.Background(), 10)
		require.NoError(t, err)
		assert.Empty(t, invalidBlocks)

		// Invalidate a block
		err = s.InvalidateBlock(context.Background(), block2.Hash())
		require.NoError(t, err)

		// Verify we can retrieve the invalid block
		invalidBlocks, err = s.GetLastNInvalidBlocks(context.Background(), 10)
		require.NoError(t, err)
		assert.Len(t, invalidBlocks, 2) // block2 and block3 (child) should be invalid

		// Test limit parameter
		invalidBlocks, err = s.GetLastNInvalidBlocks(context.Background(), 1)
		require.NoError(t, err)
		assert.Len(t, invalidBlocks, 1)

		// Verify the returned block info has the expected properties
		blockInfo := invalidBlocks[0]
		assert.NotNil(t, blockInfo.BlockHeader)
		assert.NotNil(t, blockInfo.SeenAt)
	})

	t.Run("revalidate blocks", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store blocks
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		// Invalidate a block
		err = s.InvalidateBlock(context.Background(), block2.Hash())
		require.NoError(t, err)

		// Verify we have invalid blocks
		invalidBlocks, err := s.GetLastNInvalidBlocks(context.Background(), 10)
		require.NoError(t, err)
		assert.Len(t, invalidBlocks, 1)

		// Revalidate the block
		err = s.RevalidateBlock(context.Background(), block2.Hash())
		require.NoError(t, err)

		// Verify we no longer have invalid blocks
		invalidBlocks, err = s.GetLastNInvalidBlocks(context.Background(), 10)
		require.NoError(t, err)
		assert.Empty(t, invalidBlocks)
	})
}

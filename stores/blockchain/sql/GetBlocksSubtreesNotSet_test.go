package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBlocksSubtreesNotSet(t *testing.T) {
	t.Run("returns empty when no blocks with subtrees_set=false", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		subStore, err := New(logger, dbURL, settings.NewSettings())
		require.NoError(t, err)
		defer subStore.Close()

		// Genesis block has subtrees_set=true by default, so should return empty
		blocks, err := subStore.GetBlocksSubtreesNotSet(context.Background())
		require.NoError(t, err)
		assert.Empty(t, blocks)
	})

	t.Run("returns blocks with subtrees_set=false", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		subStore, err := New(logger, dbURL, settings.NewSettings())
		require.NoError(t, err)
		defer subStore.Close()

		// Store block with subtrees_set=false (default)
		_, _, err = subStore.StoreBlock(context.Background(), block1, "test", options.WithSubtreesSet(false))
		require.NoError(t, err)

		blocks, err := subStore.GetBlocksSubtreesNotSet(context.Background())
		require.NoError(t, err)
		assert.Len(t, blocks, 1)
		assert.Equal(t, block1.Hash().String(), blocks[0].Hash().String())
	})

	t.Run("excludes blocks with subtrees_set=true", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		subStore, err := New(logger, dbURL, settings.NewSettings())
		require.NoError(t, err)
		defer subStore.Close()

		// Store block with subtrees_set=true
		_, _, err = subStore.StoreBlock(context.Background(), block1, "test", options.WithSubtreesSet(true))
		require.NoError(t, err)

		// Store block with subtrees_set=false
		_, _, err = subStore.StoreBlock(context.Background(), block2, "test", options.WithSubtreesSet(false))
		require.NoError(t, err)

		blocks, err := subStore.GetBlocksSubtreesNotSet(context.Background())
		require.NoError(t, err)
		assert.Len(t, blocks, 1)
		assert.Equal(t, block2.Hash().String(), blocks[0].Hash().String())
	})

	t.Run("returns blocks ordered by height ascending", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		subStore, err := New(logger, dbURL, settings.NewSettings())
		require.NoError(t, err)
		defer subStore.Close()

		// Store blocks with subtrees_set=false
		_, _, err = subStore.StoreBlock(context.Background(), block1, "test", options.WithSubtreesSet(false))
		require.NoError(t, err)
		_, _, err = subStore.StoreBlock(context.Background(), block2, "test", options.WithSubtreesSet(false))
		require.NoError(t, err)

		blocks, err := subStore.GetBlocksSubtreesNotSet(context.Background())
		require.NoError(t, err)
		assert.Len(t, blocks, 2)

		// Verify blocks are ordered by height ascending
		assert.LessOrEqual(t, blocks[0].Height, blocks[1].Height)
	})

	t.Run("returns complete block objects", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		subStore, err := New(logger, dbURL, settings.NewSettings())
		require.NoError(t, err)
		defer subStore.Close()

		// Store block with subtrees_set=false
		_, _, err = subStore.StoreBlock(context.Background(), block1, "test", options.WithSubtreesSet(false))
		require.NoError(t, err)

		blocks, err := subStore.GetBlocksSubtreesNotSet(context.Background())
		require.NoError(t, err)
		assert.Len(t, blocks, 1)

		block := blocks[0]
		assert.NotNil(t, block.Header)
		assert.Equal(t, block1.Header.Version, block.Header.Version)
		assert.Equal(t, block1.Header.Timestamp, block.Header.Timestamp)
		assert.Equal(t, block1.Header.Nonce, block.Header.Nonce)
		assert.Equal(t, block1.Header.HashPrevBlock.String(), block.Header.HashPrevBlock.String())
		assert.Equal(t, block1.Header.HashMerkleRoot.String(), block.Header.HashMerkleRoot.String())
		assert.Equal(t, block1.Header.Bits.String(), block.Header.Bits.String())
		assert.GreaterOrEqual(t, block.Height, uint32(0))
		assert.Equal(t, block1.TransactionCount, block.TransactionCount)
		assert.NotNil(t, block.CoinbaseTx)
		assert.NotNil(t, block.Subtrees)
		assert.Len(t, block.Subtrees, len(block1.Subtrees))
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		subStore, err := New(logger, dbURL, settings.NewSettings())
		require.NoError(t, err)
		defer subStore.Close()

		// Create cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = subStore.GetBlocksSubtreesNotSet(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})
}

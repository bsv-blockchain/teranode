package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLHasBlockBelowHeightContainingSubtree(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	newStore := func(t *testing.T) *SQL {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)
		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)
		require.NoError(t, s.insertGenesisTransaction(ulogger.TestLogger{}, tSettings.ChainCfgParams))
		return s
	}

	t.Run("nil hash errors", func(t *testing.T) {
		s := newStore(t)
		_, err := s.HasBlockBelowHeightContainingSubtree(context.Background(), nil, 1000)
		require.Error(t, err)
	})

	t.Run("no blocks stored", func(t *testing.T) {
		s := newStore(t)
		found, err := s.HasBlockBelowHeightContainingSubtree(context.Background(), subtree, 1000)
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("subtree referenced below height", func(t *testing.T) {
		s := newStore(t)
		_, _, err := s.StoreBlock(context.Background(), block1, "", options.WithMinedSet(true))
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "", options.WithMinedSet(true))
		require.NoError(t, err)

		// block1 (height 1) references subtree; search with maxHeight=1 -> found.
		found, err := s.HasBlockBelowHeightContainingSubtree(context.Background(), subtree, 1)
		require.NoError(t, err)
		assert.True(t, found)
	})

	t.Run("subtree referenced only above height", func(t *testing.T) {
		s := newStore(t)
		_, _, err := s.StoreBlock(context.Background(), block1, "", options.WithMinedSet(true))
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "", options.WithMinedSet(true))
		require.NoError(t, err)

		// Both blocks reference subtree; search with maxHeight=0 (only genesis) -> not found.
		found, err := s.HasBlockBelowHeightContainingSubtree(context.Background(), subtree, 0)
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("unknown subtree hash", func(t *testing.T) {
		s := newStore(t)
		_, _, err := s.StoreBlock(context.Background(), block1, "", options.WithMinedSet(true))
		require.NoError(t, err)

		randomHash, err := chainhash.NewHashFromStr("deadbeef00000000000000000000000000000000000000000000000000000000")
		require.NoError(t, err)

		found, err := s.HasBlockBelowHeightContainingSubtree(context.Background(), randomHash, 1000)
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("invalid block still counts", func(t *testing.T) {
		s := newStore(t)
		_, _, err := s.StoreBlock(context.Background(), block1, "", options.WithMinedSet(true))
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "", options.WithMinedSet(true))
		require.NoError(t, err)

		// Invalidate block1.
		_, err = s.InvalidateBlock(context.Background(), block1.Hash())
		require.NoError(t, err)

		// Even invalidated, block1 still has the subtree row and must be reported.
		found, err := s.HasBlockBelowHeightContainingSubtree(context.Background(), subtree, 1)
		require.NoError(t, err)
		assert.True(t, found, "invalid blocks should still count as referencing the subtree")
	})
}

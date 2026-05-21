package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLDeleteBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("missing block is a no-op", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		require.NoError(t, s.DeleteBlock(context.Background(), block2.Hash()))
	})

	t.Run("nil hash errors", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		err = s.DeleteBlock(context.Background(), nil)
		require.Error(t, err)
	})

	t.Run("delete tip block", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		require.NoError(t, s.insertGenesisTransaction(ulogger.TestLogger{}, tSettings.ChainCfgParams))

		_, _, err = s.StoreBlock(context.Background(), block1, "", options.WithMinedSet(true))
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "", options.WithMinedSet(true))
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "", options.WithMinedSet(true))
		require.NoError(t, err)

		// Delete tip (block3) — has no descendants so FK is safe.
		require.NoError(t, s.DeleteBlock(context.Background(), block3.Hash()))

		// block3 row should be gone.
		var count int
		err = s.db.QueryRowContext(context.Background(),
			"SELECT COUNT(*) FROM blocks WHERE hash = $1",
			block3.Hash().CloneBytes()).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)

		// block1 and block2 remain.
		err = s.db.QueryRowContext(context.Background(),
			"SELECT COUNT(*) FROM blocks WHERE hash = $1",
			block1.Hash().CloneBytes()).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)

		err = s.db.QueryRowContext(context.Background(),
			"SELECT COUNT(*) FROM blocks WHERE hash = $1",
			block2.Hash().CloneBytes()).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("delete chain in reverse height order", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		require.NoError(t, s.insertGenesisTransaction(ulogger.TestLogger{}, tSettings.ChainCfgParams))

		_, _, err = s.StoreBlock(context.Background(), block1, "", options.WithMinedSet(true))
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "", options.WithMinedSet(true))
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "", options.WithMinedSet(true))
		require.NoError(t, err)

		// Walk tip-down: block3, block2, block1.
		require.NoError(t, s.DeleteBlock(context.Background(), block3.Hash()))
		require.NoError(t, s.DeleteBlock(context.Background(), block2.Hash()))
		require.NoError(t, s.DeleteBlock(context.Background(), block1.Hash()))

		var count int
		err = s.db.QueryRowContext(context.Background(),
			"SELECT COUNT(*) FROM blocks WHERE height > 0").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("deleting a block with surviving child errors", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		require.NoError(t, s.insertGenesisTransaction(ulogger.TestLogger{}, tSettings.ChainCfgParams))

		_, _, err = s.StoreBlock(context.Background(), block1, "", options.WithMinedSet(true))
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "", options.WithMinedSet(true))
		require.NoError(t, err)

		// Attempt to delete block1 while block2 still references it.
		err = s.DeleteBlock(context.Background(), block1.Hash())
		require.Error(t, err, "FK constraint should prevent parent deletion when a child still references it")
	})
}

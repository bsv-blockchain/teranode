package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQL_RevalidateBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("empty - error", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		err = s.RevalidateBlock(context.Background(), block2.Hash())
		require.Error(t, err)
	})

	t.Run("Block revalidated", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		err = s.insertGenesisTransaction(ulogger.TestLogger{})
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "", options.WithMinedSet(true))
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "", options.WithMinedSet(true))
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block3, "", options.WithMinedSet(true))
		require.NoError(t, err)

		_, err = s.InvalidateBlock(context.Background(), block3.Hash())
		require.NoError(t, err)

		var (
			id        int
			height    uint32
			invalid   bool
			mined_set bool
		)

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid, mined_set FROM blocks WHERE hash = $1",
			block3.Hash().CloneBytes()).Scan(&id, &height, &invalid, &mined_set)
		require.NoError(t, err)

		assert.Equal(t, 3, id)
		assert.Equal(t, uint32(3), height)
		assert.True(t, invalid)
		assert.False(t, mined_set)

		// reset the mined_set to true, the revalidation should set it back to false
		err = s.SetBlockMinedSet(t.Context(), block3.Hash())
		require.NoError(t, err)

		err = s.RevalidateBlock(context.Background(), block3.Hash())
		require.NoError(t, err)

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid, mined_set FROM blocks WHERE hash = $1",
			block3.Hash().CloneBytes()).Scan(&id, &height, &invalid, &mined_set)
		require.NoError(t, err)

		assert.Equal(t, 3, id)
		assert.Equal(t, uint32(3), height)
		assert.False(t, invalid)
		assert.False(t, mined_set)
	})
}

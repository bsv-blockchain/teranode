package sql

import (
	"context"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
)

func TestSQL_RevalidateBlock(t *testing.T) {
	t.Run("empty - error", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeUrl)
		require.NoError(t, err)

		err = s.RevalidateBlock(context.Background(), block2.Hash())
		require.Error(t, err)
	})

	t.Run("Block revalidated", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeUrl)
		require.NoError(t, err)

		err = s.insertGenesisTransaction(ulogger.TestLogger{})
		require.NoError(t, err)

		_, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		_, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		err = s.InvalidateBlock(context.Background(), block3.Hash())
		require.NoError(t, err)

		var id int
		var height uint32
		var invalid bool

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid FROM blocks WHERE hash = $1",
			block3.Hash().CloneBytes()).Scan(&id, &height, &invalid)
		require.NoError(t, err)

		assert.Equal(t, 3, id)
		assert.Equal(t, uint32(3), height)
		assert.True(t, invalid)

		err = s.RevalidateBlock(context.Background(), block3.Hash())
		require.NoError(t, err)

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid FROM blocks WHERE hash = $1",
			block3.Hash().CloneBytes()).Scan(&id, &height, &invalid)
		require.NoError(t, err)

		assert.Equal(t, 3, id)
		assert.Equal(t, uint32(3), height)
		assert.False(t, invalid)
	})
}

package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQL_InvalidateBlock(t *testing.T) {
	t.Run("empty - error", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeUrl)
		require.NoError(t, err)

		err = s.InvalidateBlock(context.Background(), block2.Hash())
		require.Error(t, err)
	})

	t.Run("Block invalidated", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeUrl)
		require.NoError(t, err)

		err = s.insertGenesisTransaction(ulogger.TestLogger{})
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		err = s.InvalidateBlock(context.Background(), block3.Hash())
		require.NoError(t, err)

		var id int
		var height uint32
		var invalid bool

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid FROM blocks WHERE hash = $1",
			block1.Hash().CloneBytes()).Scan(&id, &height, &invalid)
		require.NoError(t, err)

		assert.Equal(t, 1, id)
		assert.Equal(t, uint32(1), height)
		assert.False(t, invalid)

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid FROM blocks WHERE hash = $1",
			block2.Hash().CloneBytes()).Scan(&id, &height, &invalid)
		require.NoError(t, err)

		assert.Equal(t, 2, id)
		assert.Equal(t, uint32(2), height)
		assert.False(t, invalid)

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid FROM blocks WHERE hash = $1",
			block3.Hash().CloneBytes()).Scan(&id, &height, &invalid)
		require.NoError(t, err)

		assert.Equal(t, 3, id)
		assert.Equal(t, uint32(3), height)
		assert.True(t, invalid)
	})

	t.Run("Blocks invalidated", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeUrl)
		require.NoError(t, err)

		err = s.insertGenesisTransaction(ulogger.TestLogger{})
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		//err = dumpDBData(t, err, s)

		err = s.InvalidateBlock(context.Background(), block2.Hash())
		require.NoError(t, err)

		var id int
		var height uint32
		var invalid bool

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid FROM blocks WHERE hash = $1",
			block1.Hash().CloneBytes()).Scan(&id, &height, &invalid)
		require.NoError(t, err)

		assert.Equal(t, 1, id)
		assert.Equal(t, uint32(1), height)
		assert.False(t, invalid)

		b2, block2Height, err := s.GetBlock(context.Background(), block2.Hash())
		require.NoError(t, err)
		assert.Equal(t, uint32(2), block2Height)
		assert.Equal(t, block2.Hash().String(), b2.Hash().String())

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid FROM blocks WHERE hash = $1",
			block2.Hash().CloneBytes()).Scan(&id, &height, &invalid)
		require.NoError(t, err)

		assert.Equal(t, 2, id)
		assert.Equal(t, uint32(2), height)
		assert.True(t, invalid)

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid FROM blocks WHERE hash = $1",
			block3.Hash().CloneBytes()).Scan(&id, &height, &invalid)
		require.NoError(t, err)

		assert.Equal(t, 3, id)
		assert.Equal(t, uint32(3), height)
		assert.True(t, invalid)
	})
}

//func dumpDBData(t *testing.T, err error, s *SQL) error {
//	// get and print all the data in the db
//	rows, err := s.db.QueryContext(context.Background(), "SELECT id, hash, height, invalid FROM blocks")
//	require.NoError(t, err)
//	defer rows.Close()
//
//	for rows.Next() {
//		var id int
//		var hash []byte
//		var height uint32
//		var invalid bool
//		err = rows.Scan(&id, &hash, &height, &invalid)
//		require.NoError(t, err)
//		t.Logf("id: %d, hash: %x, height: %d, invalid: %t", id, hash, height, invalid)
//	}
//	return err
//}

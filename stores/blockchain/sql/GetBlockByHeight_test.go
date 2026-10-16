package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLGetBlockByHeight(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("block 0 - genesis block", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		block, err := s.GetBlockByHeight(context.Background(), 0)
		require.NoError(t, err)

		assertRegtestGenesis(t, block.Header)

		// block
		assert.Equal(t, uint64(1), block.TransactionCount)
		assert.Len(t, block.Subtrees, 0)
	})

	t.Run("blocks ", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		id, _, err := s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		block1.ID = uint32(id) //nolint:gosec

		id, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		block2.ID = uint32(id) //nolint:gosec

		block, err := s.GetBlockByHeight(context.Background(), 1)
		require.NoError(t, err)

		assert.Equal(t, block1.String(), block.String())

		block, err = s.GetBlockByHeight(context.Background(), 2)
		require.NoError(t, err)

		assert.Equal(t, block2.String(), block.String())
	})
}

// TestGetBlockByHeight_ARowsErrorIsAStoreFault: a database error while reading
// the result ends rows.Next() exactly as an empty result does. Read as a miss, it
// reached getblockbyheight as -8 "Block height out of range" for a height the
// node may hold.
func TestGetBlockByHeight_ARowsErrorIsAStoreFault(t *testing.T) {
	t.Run("a database error while reading is a store fault", func(t *testing.T) {
		s, mock, err := createMockSQL()
		require.NoError(t, err)
		defer s.db.Close()

		mock.ExpectQuery("SELECT").WillReturnRows(
			sqlmock.NewRows([]string{"id"}).AddRow(1).RowError(0, errors.NewError("driver: bad connection")))

		_, err = s.GetBlockByHeight(context.Background(), 99)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.NotErrorIs(t, err, errors.ErrBlockNotFound)
	})

	t.Run("an empty result is still a miss", func(t *testing.T) {
		s, mock, err := createMockSQL()
		require.NoError(t, err)
		defer s.db.Close()

		mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id"}))

		_, err = s.GetBlockByHeight(context.Background(), 99)
		require.ErrorIs(t, err, errors.ErrBlockNotFound)
	})
}

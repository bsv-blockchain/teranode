package sql

import (
	"context"
	"database/sql"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBlockByID(t *testing.T) {
	t.Run("valid block ID", func(t *testing.T) {
		// Setup test logger and database
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s := settings.NewSettings()
		store, err := New(logger, dbURL, s)
		require.NoError(t, err)

		defer store.Close()

		// Insert a block into the database
		newBlockID, _, err := store.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		block1.ID = uint32(newBlockID) //nolint:gosec

		// Retrieve the block by ID
		retrievedBlock, err := store.GetBlockByID(context.Background(), newBlockID)
		require.NoError(t, err)

		assert.Equal(t, block1.String(), retrievedBlock.String())

		// Insert a block into the database
		newBlockID, _, err = store.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		block2.ID = uint32(newBlockID) //nolint:gosec

		// Retrieve the block by ID
		retrievedBlock, err = store.GetBlockByID(context.Background(), newBlockID)
		require.NoError(t, err)

		assert.Equal(t, block2.String(), retrievedBlock.String())
	})

	t.Run("block ID not found", func(t *testing.T) {
		// Setup test logger and database
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s := settings.NewSettings()
		store, err := New(logger, dbURL, s)
		require.NoError(t, err)

		defer store.Close()

		// Attempt to retrieve a block with a non-existent ID
		blockID := uint64(999)
		_, err = store.GetBlockByID(context.Background(), blockID)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, sql.ErrNoRows))
	})
}

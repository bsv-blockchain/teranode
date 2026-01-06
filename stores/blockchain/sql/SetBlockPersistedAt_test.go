package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetBlockPersistedAt(t *testing.T) {
	t.Run("Set persisted_at timestamp", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		store, err := New(logger, dbURL, settings.NewSettings())
		require.NoError(t, err)
		defer store.Close()

		// Store block1 using the proper method
		_, _, err = store.StoreBlock(context.Background(), block1, "test")
		require.NoError(t, err)

		// Set persisted_at
		err = store.SetBlockPersistedAt(context.Background(), block1.Hash())
		require.NoError(t, err)

		// Verify the persisted_at field was updated
		var persistedAt interface{}
		q := `SELECT persisted_at FROM blocks WHERE hash = $1`
		err = store.db.QueryRowContext(context.Background(), q, block1.Hash().CloneBytes()).Scan(&persistedAt)
		require.NoError(t, err)
		assert.NotNil(t, persistedAt)
	})

	t.Run("Block not found", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		store, err := New(logger, dbURL, settings.NewSettings())
		require.NoError(t, err)
		defer store.Close()

		nonExistentHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")

		err = store.SetBlockPersistedAt(context.Background(), nonExistentHash)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrStorageError))
	})

	t.Run("Idempotent - can set multiple times", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		store, err := New(logger, dbURL, settings.NewSettings())
		require.NoError(t, err)
		defer store.Close()

		// Store block1
		_, _, err = store.StoreBlock(context.Background(), block1, "test")
		require.NoError(t, err)

		// Set persisted_at twice - should not error
		err = store.SetBlockPersistedAt(context.Background(), block1.Hash())
		require.NoError(t, err)

		err = store.SetBlockPersistedAt(context.Background(), block1.Hash())
		require.NoError(t, err)
	})
}

package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/utxo/tests"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func newDB(t *testing.T) (context.Context, *Store) {
	sqliteURL, err := url.Parse("sqlitememory:///utxo")
	require.NoError(t, err)

	// ubsv db client
	db, err := New(sqliteURL)
	require.NoError(t, err)

	return context.Background(), db
}

func TestSQL(t *testing.T) {
	t.Run("memory store", func(t *testing.T) {
		ctx, db := newDB(t)
		err := db.delete(ctx, tests.Hash)
		require.NoError(t, err)

		tests.Store(t, db)
	})

	t.Run("memory spend", func(t *testing.T) {
		ctx, db := newDB(t)
		err := db.delete(ctx, tests.Hash)
		require.NoError(t, err)

		tests.Spend(t, db)
	})

	t.Run("memory reset", func(t *testing.T) {
		ctx, db := newDB(t)
		err := db.delete(ctx, tests.Hash)
		require.NoError(t, err)

		tests.Restore(t, db)
	})

	t.Run("memory lock time", func(t *testing.T) {
		ctx, db := newDB(t)
		err := db.delete(ctx, tests.Hash)
		require.NoError(t, err)

		tests.LockTime(t, db)
	})
}

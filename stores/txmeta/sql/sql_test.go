package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/txmeta/tests"
	"github.com/stretchr/testify/require"
)

var storeUrl, _ = url.Parse("sqlitememory:///")

func TestMemory(t *testing.T) {
	t.Run("sql set", func(t *testing.T) {
		db, err := New(storeUrl)
		require.NoError(t, err)

		err = db.Delete(context.Background(), tests.Hash)
		require.NoError(t, err)

		tests.Store(t, db)
	})
}

func TestMemorySanity(t *testing.T) {
	db, err := New(storeUrl)
	require.NoError(t, err)

	tests.Sanity(t, db)
}

func BenchmarkMemory(b *testing.B) {
	db, err := New(storeUrl)
	require.NoError(b, err)

	tests.Benchmark(b, db)
}

package memory

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

func TestMemory(t *testing.T) {

	t.Run("memory store", func(t *testing.T) {
		db := New(false)
		err := db.delete(tests.Hash)
		require.NoError(t, err)

		tests.Store(t, db)
	})

	t.Run("memory store from hashes", func(t *testing.T) {
		db := New(false)
		err := db.delete(tests.Hash)
		require.NoError(t, err)

		tests.StoreFromHashes(t, db)
	})

	t.Run("memory spend", func(t *testing.T) {
		db := New(false)
		err := db.delete(tests.Hash)
		require.NoError(t, err)

		tests.Spend(t, db)
	})

	t.Run("memory reset", func(t *testing.T) {
		db := New(false)
		err := db.delete(tests.Hash)
		require.NoError(t, err)

		tests.Restore(t, db)
	})

	t.Run("memory lock time", func(t *testing.T) {
		db := New(false)
		err := db.delete(tests.Hash)
		require.NoError(t, err)

		tests.LockTime(t, db)
	})
}

func TestMemorySanity(t *testing.T) {
	db := New(false)
	tests.Sanity(t, db)
}

func BenchmarkMemory(b *testing.B) {
	db := New(true)
	tests.Benchmark(b, db)
}

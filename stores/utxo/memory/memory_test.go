package memory

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/utxo/tests"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/require"
)

func TestMemory(t *testing.T) {
	t.Run("memory store", func(t *testing.T) {
		db := New(ulogger.TestLogger{})
		err := db.delete(tests.Hash)
		require.NoError(t, err)

		tests.Store(t, db)
	})

	t.Run("memory spend", func(t *testing.T) {
		db := New(ulogger.TestLogger{})
		err := db.delete(tests.Hash)
		require.NoError(t, err)

		tests.Spend(t, db)
	})

	t.Run("memory reset", func(t *testing.T) {
		db := New(ulogger.TestLogger{})
		err := db.delete(tests.Hash)
		require.NoError(t, err)

		tests.Restore(t, db)
	})

	t.Run("memory freeze", func(t *testing.T) {
		db := New(ulogger.TestLogger{})
		err := db.delete(tests.Hash)
		require.NoError(t, err)

		tests.Freeze(t, db)
	})

	t.Run("memory reassign", func(t *testing.T) {
		db := New(ulogger.TestLogger{})
		err := db.delete(tests.Hash)
		require.NoError(t, err)

		tests.ReAssign(t, db)
	})
}

func BenchmarkMemory(b *testing.B) {
	db := New(ulogger.TestLogger{})
	tests.Benchmark(b, db)
}

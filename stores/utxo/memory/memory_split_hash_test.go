package memory

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

func TestSplitByHash(t *testing.T) {

	t.Run("memory store", func(t *testing.T) {
		db := NewSplitByHash(false)
		err := db.delete(tests.Hash)
		require.NoError(t, err)

		tests.Store(t, db)
	})

	t.Run("memory spend", func(t *testing.T) {
		db := NewSplitByHash(false)
		err := db.delete(tests.Hash)
		require.NoError(t, err)

		tests.Spend(t, db)
	})

	t.Run("memory reset", func(t *testing.T) {
		db := NewSplitByHash(false)
		err := db.delete(tests.Hash)
		require.NoError(t, err)

		tests.Restore(t, db)
	})

	t.Run("memory lock time", func(t *testing.T) {
		db := NewSplitByHash(false)
		err := db.delete(tests.Hash)
		require.NoError(t, err)

		tests.LockTime(t, db)
	})
}

//func TestSplitByHashSanity(t *testing.T) {
//	db := NewSplitByHash(false)
//	tests.Sanity(t, db)
//}

func BenchmarkSplitByHash(b *testing.B) {
	db := NewSplitByHash(true)
	tests.Benchmark(b, db)
}

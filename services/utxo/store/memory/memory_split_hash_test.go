package memory

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitByHash(t *testing.T) {

	t.Run("memory store", func(t *testing.T) {
		db := NewSplitByHash(false)
		err := db.delete(hash)
		require.NoError(t, err)

		testStore(t, db)
	})

	t.Run("memory spend", func(t *testing.T) {
		db := NewSplitByHash(false)
		err := db.delete(hash)
		require.NoError(t, err)

		testSpend(t, db)
	})

	t.Run("memory reset", func(t *testing.T) {
		db := NewSplitByHash(false)
		err := db.delete(hash)
		require.NoError(t, err)

		testRestore(t, db)
	})
}

func TestSplitByHashSanity(t *testing.T) {
	db := NewSplitByHash(false)
	testSanity(t, db)
}

func BenchmarkSplitByHash(b *testing.B) {
	db := NewSplitByHash(true)
	benchmark(b, db)
}

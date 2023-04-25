package memory

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemory(t *testing.T) {

	t.Run("memory store", func(t *testing.T) {
		db := New()
		err := db.delete(hash)
		require.NoError(t, err)

		testStore(t, db)
	})

	t.Run("memory spend", func(t *testing.T) {
		db := New()
		err := db.delete(hash)
		require.NoError(t, err)

		testSpend(t, db)
	})

	t.Run("memory reset", func(t *testing.T) {
		db := New()
		err := db.delete(hash)
		require.NoError(t, err)

		testRestore(t, db)
	})
}

func TestMemorySanity(t *testing.T) {
	db := New()
	testSanity(t, db)
}

func BenchmarkMemory(b *testing.B) {
	db := New()
	benchmark(b, db)
}

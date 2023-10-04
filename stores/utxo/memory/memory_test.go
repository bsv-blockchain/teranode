package memory

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemory(t *testing.T) {

	t.Run("memory store", func(t *testing.T) {
		db := New(false)
		err := db.delete(testHash1)
		require.NoError(t, err)

		testStore(t, db)
	})

	t.Run("memory spend", func(t *testing.T) {
		db := New(false)
		err := db.delete(testHash1)
		require.NoError(t, err)

		testSpend(t, db)
	})

	t.Run("memory reset", func(t *testing.T) {
		db := New(false)
		err := db.delete(testHash1)
		require.NoError(t, err)

		testRestore(t, db)
	})

	t.Run("memory lock time", func(t *testing.T) {
		db := New(false)
		err := db.delete(testHash1)
		require.NoError(t, err)

		testLockTime(t, db)
	})
}

func TestMemorySanity(t *testing.T) {
	db := New(false)
	testSanity(t, db)
}

func BenchmarkMemory(b *testing.B) {
	db := New(true)
	benchmark(b, db)
}

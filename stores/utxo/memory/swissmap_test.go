package memory

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSwissMap(t *testing.T) {
	t.Run("memory store", func(t *testing.T) {
		db := NewSwissMap(false)

		err := db.delete(hash)
		require.NoError(t, err)
		testStore(t, db)
	})

	t.Run("memory spend", func(t *testing.T) {
		db := NewSwissMap(false)

		err := db.delete(hash)
		require.NoError(t, err)
		testSpend(t, db)
	})

	t.Run("memory reset", func(t *testing.T) {
		db := NewSwissMap(false)

		err := db.delete(hash)
		require.NoError(t, err)
		testRestore(t, db)
	})

	t.Run("memory lock time", func(t *testing.T) {
		db := NewSwissMap(false)
		err := db.delete(hash)
		require.NoError(t, err)

		testLockTime(t, db)
	})
}

func TestSwissMapSanity(t *testing.T) {
	db := NewSwissMap(false)
	db.DeleteSpentUtxos = false

	testSanity(t, db)
}

func BenchmarkSwissMap(b *testing.B) {
	db := NewSwissMap(true)
	benchmark(b, db)
}

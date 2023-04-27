package memory

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestXsyncMap(t *testing.T) {
	t.Run("memory store", func(t *testing.T) {
		db := NewXSyncMap(false)
		err := db.delete(hash)
		require.NoError(t, err)

		testStore(t, db)
	})

	t.Run("memory spend", func(t *testing.T) {
		db := NewXSyncMap(false)
		err := db.delete(hash)
		require.NoError(t, err)

		testSpend(t, db)
	})

	t.Run("memory reset", func(t *testing.T) {
		db := NewXSyncMap(false)
		err := db.delete(hash)
		require.NoError(t, err)

		testRestore(t, db)
	})
}

func TestXsyncMapSanity(t *testing.T) {
	db := NewXSyncMap(false)
	db.DeleteSpentUtxos = false

	testSanity(t, db)
}

func BenchmarkXsyncMap(b *testing.B) {
	db := NewXSyncMap(true)
	benchmark(b, db)
}

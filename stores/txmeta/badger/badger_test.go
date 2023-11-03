package badger

import (
	"context"
	"os"
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/txmeta/tests"
	"github.com/stretchr/testify/require"
)

func TestMemory(t *testing.T) {
	t.Run("memory set", func(t *testing.T) {
		defer func() {
			_ = os.RemoveAll("./test")
		}()

		_ = os.RemoveAll("./test")

		db, err := New("./test")
		require.NoError(t, err)

		err = db.Delete(context.Background(), tests.Tx1.TxIDChainHash())
		require.NoError(t, err)

		tests.Store(t, db)
	})
}

func TestMemorySanity(t *testing.T) {
	defer func() {
		_ = os.RemoveAll("./test")
	}()

	_ = os.RemoveAll("./test")

	db, err := New("./test")
	require.NoError(t, err)
	tests.Sanity(t, db)
}

func BenchmarkMemory(b *testing.B) {
	db, err := New("./test")
	require.NoError(b, err)
	tests.Benchmark(b, db)
}

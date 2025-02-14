package memory

import (
	"testing"

	"github.com/bitcoin-sv/teranode/stores/utxo/tests"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libsv/go-bt/v2"
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

	t.Run("memory conflicting tx", func(t *testing.T) {
		db := New(ulogger.TestLogger{})
		err := db.delete(tests.Hash)
		require.NoError(t, err)

		// add the parents of the tx, so that the conflicting tx can be added to the parents as conflictingChildren
		for _, parent := range tests.Tx.Inputs {
			db.txs[*parent.PreviousTxIDChainHash()] = &memoryData{
				tx: &bt.Tx{},
			}
		}

		tests.Conflicting(t, db)
	})
}

func TestMemorySanity(t *testing.T) {
	db := New(ulogger.TestLogger{})
	tests.Sanity(t, db)
}

func BenchmarkMemory(b *testing.B) {
	db := New(ulogger.TestLogger{})
	tests.Benchmark(b, db)
}
